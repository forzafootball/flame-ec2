defmodule FlameEC2.EC2Api do
  @moduledoc false

  import FlameEC2.Utils

  alias FlameEC2.BackendState
  alias FlameEC2.Config
  alias FlameEC2.EC2Api.XML
  alias FlameEC2.Templates

  require Logger

  def run_instances!(%BackendState{} = state) do
    if state.config.spot do
      create_fleet_instant!(state)
    else
      run_instances_direct!(state)
    end
  end

  # --- CreateFleet (spot instances with price-capacity-optimized allocation) ---

  defp create_fleet_instant!(%BackendState{} = state) do
    uri = ec2_uri(state.config)
    credentials = fetch_credentials!()

    # Build launch template data with all instance config EXCEPT instance type and subnet
    lt_data = build_launch_template_data(state)
    lt_name = "flame-#{state.config.app}-#{System.system_time(:millisecond)}"

    # 1. Create temporary launch template
    create_lt_params =
      %{
        "Action" => "CreateLaunchTemplate",
        "Version" => "2016-11-15",
        "LaunchTemplateName" => lt_name,
        "LaunchTemplateData" => lt_data
      }
      |> flatten_json_object()
      |> Map.filter(fn {_k, v} -> not is_nil(v) end)

    lt_resp = ec2_request!(uri, create_lt_params, credentials)
    lt_id = get_in(lt_resp, ["CreateLaunchTemplateResponse", "launchTemplate", "launchTemplateId"])

    if is_nil(lt_id) do
      raise "Failed to create launch template: #{inspect(lt_resp)}"
    end

    Logger.info("Created launch template #{lt_name} (#{lt_id})")

    try do
      # 2. Build overrides: all instance_type × subnet combinations
      overrides = fleet_overrides(state.config)

      fleet_params =
        %{
          "Action" => "CreateFleet",
          "Version" => "2016-11-15",
          "Type" => "instant",
          "SpotOptions" => %{
            "AllocationStrategy" => "price-capacity-optimized"
          },
          "LaunchTemplateConfigs" => [
            %{
              "LaunchTemplateSpecification" => %{
                "LaunchTemplateId" => lt_id,
                "Version" => "$Default"
              },
              "Overrides" => overrides
            }
          ],
          "TargetCapacitySpecification" => %{
            "TotalTargetCapacity" => 1,
            "DefaultTargetCapacityType" => "spot"
          },
          "TagSpecification" => instance_tag_specs(state)
        }
        |> flatten_json_object()
        |> Map.filter(fn {_k, v} -> not is_nil(v) end)

      fleet_resp = ec2_request!(uri, fleet_params, credentials)

      # Parse instance from fleet response
      parse_fleet_response(fleet_resp)
    after
      # 3. Always clean up launch template
      delete_lt_params = %{
        "Action" => "DeleteLaunchTemplate",
        "Version" => "2016-11-15",
        "LaunchTemplateId" => lt_id
      }

      case ec2_request(uri, delete_lt_params, credentials) do
        {:ok, _} -> Logger.debug("Deleted launch template #{lt_id}")
        {:error, reason} -> Logger.warning("Failed to delete launch template #{lt_id}: #{inspect(reason)}")
      end
    end
  end

  defp fleet_overrides(%Config{} = config) do
    instance_types = [config.instance_type | config.fallback_instance_types || []]
    subnet_ids = [config.subnet_id | config.fallback_subnet_ids || []]

    for instance_type <- instance_types,
        subnet_id <- subnet_ids do
      %{"InstanceType" => instance_type, "SubnetId" => subnet_id}
    end
  end

  defp instance_tag_specs(%BackendState{} = state) do
    [
      %{
        "ResourceType" => "instance",
        "Tag" => [
          %{"Key" => "Name", "Value" => "#{state.config.app}-flame-worker"},
          %{"Key" => "FLAME_PARENT_IP", "Value" => state.config.local_ip},
          %{"Key" => "FLAME_PARENT_APP", "Value" => state.config.app}
        ]
      }
    ]
  end

  defp build_launch_template_data(%BackendState{} = state) do
    config = state.config
    systemd_service = Templates.systemd_service(app: config.app)
    env = Templates.env(vars: state.runner_env)

    start_script =
      Templates.start_script(
        app: config.app,
        systemd_service: systemd_service,
        env: env,
        aws_region: config.aws_region,
        s3_bundle_url: config.s3_bundle_url,
        s3_bundle_compressed?: config.s3_bundle_compressed?,
        setup_commands: config.setup_commands
      )

    data = %{
      "InstanceInitiatedShutdownBehavior" => "terminate",
      "UserData" => Base.encode64(start_script),
      "IamInstanceProfile" => %{"Arn" => config.iam_instance_profile},
      "NetworkInterface" => [
        %{
          "AssociatePublicIpAddress" => true,
          "DeleteOnTermination" => true,
          "DeviceIndex" => 0,
          "SecurityGroupId" => config.security_group_ids
        }
      ]
    }

    data
    |> maybe_put_image_id(config)
    |> maybe_put_key_name(config)
    |> maybe_put_root_volume_lt(config)
  end

  defp maybe_put_image_id(data, %Config{image_id: id}) when is_binary(id) and id != "" do
    Map.put(data, "ImageId", id)
  end
  defp maybe_put_image_id(data, _config), do: data

  defp maybe_put_key_name(data, %Config{key_name: name}) when is_binary(name) and name != "" do
    Map.put(data, "KeyName", name)
  end
  defp maybe_put_key_name(data, _config), do: data

  defp maybe_put_root_volume_lt(data, %Config{root_volume_size: size}) when is_integer(size) do
    Map.put(data, "BlockDeviceMapping", [
      %{
        "DeviceName" => "/dev/sda1",
        "Ebs" => %{
          "VolumeSize" => size,
          "VolumeType" => "gp3",
          "DeleteOnTermination" => true
        }
      }
    ])
  end
  defp maybe_put_root_volume_lt(data, _config), do: data

  defp parse_fleet_response(resp) do
    instance_id =
      case get_in(resp, ["CreateFleetResponse", "fleetInstanceSet", "item"]) do
        %{"instanceIds" => %{"item" => %{"instanceId" => id}}} -> id
        %{"instanceIds" => %{"item" => [%{"instanceId" => id} | _]}} -> id
        nil ->
          errors = get_in(resp, ["CreateFleetResponse", "errorSet", "item"])
          raise "CreateFleet failed to launch any instances: #{inspect(errors || resp)}"
        other ->
          raise "Unexpected CreateFleet response: #{inspect(other)}"
      end

    # DescribeInstances to get privateIpAddress (not returned by CreateFleet)
    wait_for_instance_ip(instance_id, _retries = 10)
  end

  defp wait_for_instance_ip(instance_id, 0) do
    raise "Timed out waiting for private IP of instance #{instance_id}"
  end

  defp wait_for_instance_ip(instance_id, retries) do
    credentials = fetch_credentials!()
    region = Map.get(credentials, :region, "eu-west-1")
    uri = "https://ec2.#{region}.amazonaws.com/"

    params = %{
      "Action" => "DescribeInstances",
      "Version" => "2016-11-15",
      "InstanceId.1" => instance_id
    }

    case ec2_request(uri, params, credentials) do
      {:ok, resp} ->
        instances = parse_describe_instances(resp)

        case Enum.find(instances, &(&1.instance_id == instance_id)) do
          %{private_ip: ip} when is_binary(ip) and ip != "" ->
            %{"instanceId" => instance_id, "privateIpAddress" => ip}

          _ ->
            Process.sleep(2_000)
            wait_for_instance_ip(instance_id, retries - 1)
        end

      {:error, _} ->
        Process.sleep(2_000)
        wait_for_instance_ip(instance_id, retries - 1)
    end
  end

  # --- RunInstances (on-demand, no fleet needed) ---

  defp run_instances_direct!(%BackendState{} = state) do
    combinations = instance_type_subnet_combinations(state.config)
    run_instances_with_fallback!(state, combinations)
  end

  defp instance_type_subnet_combinations(%Config{} = config) do
    instance_types = [config.instance_type | config.fallback_instance_types || []]
    subnet_ids = [config.subnet_id | config.fallback_subnet_ids || []]

    for instance_type <- instance_types,
        subnet_id <- subnet_ids,
        do: {instance_type, subnet_id}
  end

  defp run_instances_with_fallback!(_state, []) do
    raise "No instance capacity available for any configured instance type and subnet combination"
  end

  defp run_instances_with_fallback!(state, [{instance_type, subnet_id} | remaining]) do
    params =
      state
      |> build_run_instances_params()
      |> Map.put("InstanceType", instance_type)
      |> Map.put("NetworkInterface.1.SubnetId", subnet_id)

    uri = ec2_uri(state.config)
    credentials = fetch_credentials!()

    case ec2_request(uri, params, credentials) do
      {:ok, resp} ->
        resp
        |> Map.fetch!("RunInstancesResponse")
        |> Map.fetch!("instancesSet")
        |> Map.fetch!("item")

      {:error, {:http, status, body}} ->
        if insufficient_capacity?(body) and remaining != [] do
          {next_type, next_subnet} = hd(remaining)
          Logger.warning("No capacity for #{instance_type} in #{subnet_id}, trying #{next_type} in #{next_subnet}")
          run_instances_with_fallback!(state, remaining)
        else
          raise "Failed to create instance: status #{status}, body: #{inspect(body)}"
        end

      {:error, reason} ->
        raise "Failed to create instance: #{inspect(reason)}"
    end
  end

  defp build_run_instances_params(%BackendState{} = state) do
    state.config
    |> params_from_config(state.runner_env)
    |> Map.merge(%{
      "TagSpecification" => instance_tag_specs(state)
    })
    |> Map.put("Action", "RunInstances")
    |> flatten_json_object()
    |> Map.filter(fn {_k, v} -> not is_nil(v) end)
  end

  defp params_from_config(%Config{} = config, env) do
    systemd_service = Templates.systemd_service(app: config.app)
    env = Templates.env(vars: env)

    start_script =
      Templates.start_script(
        app: config.app,
        systemd_service: systemd_service,
        env: env,
        aws_region: config.aws_region,
        s3_bundle_url: config.s3_bundle_url,
        s3_bundle_compressed?: config.s3_bundle_compressed?,
        setup_commands: config.setup_commands
      )

    base_params = %{
      "Version" => "2016-11-15",
      "MaxCount" => 1,
      "MinCount" => 1,
      "KeyName" => config.key_name,
      "NetworkInterface" => [
        %{
          "AssociatePublicIpAddress" => true,
          "DeleteOnTermination" => true,
          "DeviceIndex" => 0,
          "SubnetId" => config.subnet_id,
          "SecurityGroupId" => config.security_group_ids
        }
      ],
      "InstanceType" => config.instance_type,
      "IamInstanceProfile" => %{
        "Arn" => config.iam_instance_profile
      },
      "InstanceInitiatedShutdownBehavior" => "terminate",
      "UserData" => Base.encode64(start_script)
    }

    base_params
    |> Map.merge(creation_details_params(config))
    |> maybe_put_root_volume(config)
  end

  defp maybe_put_root_volume(params, %Config{root_volume_size: size}) when is_integer(size) do
    Map.put(params, "BlockDeviceMapping", [
      %{
        "DeviceName" => "/dev/sda1",
        "Ebs" => %{
          "VolumeSize" => size,
          "VolumeType" => "gp3",
          "DeleteOnTermination" => true
        }
      }
    ])
  end

  defp maybe_put_root_volume(params, %Config{}) do
    params
  end

  defp creation_details_params(%Config{launch_template_id: launch_template_id} = config)
       when is_binary(launch_template_id) and launch_template_id != "" do
    %{
      "LaunchTemplate" => %{
        "LaunchTemplateId" => launch_template_id,
        "Version" => config.launch_template_version
      }
    }
  end

  defp creation_details_params(%Config{image_id: image_id}) when is_binary(image_id) and image_id != "" do
    %{
      "ImageId" => image_id
    }
  end

  # --- Shared helpers ---

  defp ec2_uri(%Config{} = config) do
    URI.to_string(URI.new!(config.ec2_service_endpoint))
  end

  defp fetch_credentials! do
    case :aws_credentials.get_credentials() do
      :undefined -> raise "No AWS credentials found in env or in credentials cache"
      %{} = creds -> creds
    end
  end

  defp ec2_request!(uri, params, credentials) do
    case ec2_request(uri, params, credentials) do
      {:ok, resp} -> resp
      {:error, {:http, status, body}} ->
        raise "EC2 API failed: status #{status}, body: #{inspect(body)}"
      {:error, reason} ->
        raise "EC2 API failed: #{inspect(reason)}"
    end
  end

  defp ec2_request(uri, params, credentials) do
    result =
      [
        url: uri,
        method: :post,
        form: params,
        aws_sigv4: Map.put_new(credentials, :service, "ec2")
      ]
      |> Req.new()
      |> Req.request()

    case result do
      {:ok, %Req.Response{status: status, body: body}} when status >= 300 ->
        {:error, {:http, status, body}}

      {:ok, %Req.Response{} = resp} ->
        resp = if xml?(resp), do: update_in(resp.body, &XML.decode!/1), else: resp
        {:ok, resp.body}

      {:error, exception} ->
        {:error, exception}
    end
  end

  defp insufficient_capacity?(body) when is_binary(body) do
    String.contains?(body, "InsufficientInstanceCapacity")
  end

  defp insufficient_capacity?(_), do: false

  # --- Instance management ---

  @doc """
  Describe running instances with a specific tag value.
  """
  def describe_instances_by_tag(tag_key, tag_value) do
    credentials = fetch_credentials!()
    region = Map.get(credentials, :region, "eu-west-1")
    uri = "https://ec2.#{region}.amazonaws.com/"

    params = %{
      "Action" => "DescribeInstances",
      "Version" => "2016-11-15",
      "Filter.1.Name" => "tag:#{tag_key}",
      "Filter.1.Value.1" => tag_value,
      "Filter.2.Name" => "instance-state-name",
      "Filter.2.Value.1" => "running"
    }

    case ec2_request(uri, params, credentials) do
      {:ok, resp} ->
        instances = parse_describe_instances(resp)
        {:ok, instances}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_describe_instances(resp) do
    case get_in(resp, ["DescribeInstancesResponse", "reservationSet"]) do
      nil -> []
      %{"item" => items} when is_list(items) -> Enum.flat_map(items, &parse_reservation/1)
      %{"item" => item} when is_map(item) -> parse_reservation(item)
      _ -> []
    end
  end

  defp parse_reservation(%{"instancesSet" => %{"item" => items}}) when is_list(items) do
    Enum.map(items, &parse_instance/1)
  end
  defp parse_reservation(%{"instancesSet" => %{"item" => item}}) when is_map(item) do
    [parse_instance(item)]
  end
  defp parse_reservation(_), do: []

  defp parse_instance(item) do
    %{instance_id: Map.get(item, "instanceId"), private_ip: Map.get(item, "privateIpAddress")}
  end

  @doc """
  Terminate EC2 instances by instance IDs.
  """
  def terminate_instances(instance_ids) when is_list(instance_ids) do
    credentials = fetch_credentials!()
    region = Map.get(credentials, :region, "eu-west-1")
    uri = "https://ec2.#{region}.amazonaws.com/"

    id_params =
      instance_ids
      |> Enum.with_index(1)
      |> Map.new(fn {id, i} -> {"InstanceId.#{i}", id} end)

    params = Map.merge(%{"Action" => "TerminateInstances", "Version" => "2016-11-15"}, id_params)

    case ec2_request(uri, params, credentials) do
      {:ok, _} -> :ok
      {:error, reason} ->
        Logger.error("TerminateInstances failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Adapted from https://github.com/livebook-dev/livebook/blob/v0.14.5/lib/livebook/file_system/s3/client.ex
  defp xml?(response) do
    guess_xml? = String.starts_with?(response.body, "<?xml")

    case FlameEC2.EC2Api.Utils.fetch_content_type(response) do
      {:ok, content_type} when content_type in ["text/xml", "application/xml"] -> true
      :error when guess_xml? -> true
      _otherwise -> false
    end
  end
end
