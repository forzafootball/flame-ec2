defmodule FlameEC2.EC2Api do
  @moduledoc false

  import FlameEC2.Utils

  alias FlameEC2.BackendState
  alias FlameEC2.Config
  alias FlameEC2.EC2Api.XML
  alias FlameEC2.Templates

  require Logger

  def run_instances!(%BackendState{} = state) do
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
      |> build_params_from_state()
      |> Map.put("InstanceType", instance_type)
      |> Map.put("NetworkInterface.1.SubnetId", subnet_id)

    uri = URI.to_string(URI.new!(state.config.ec2_service_endpoint))
    credentials = :aws_credentials.get_credentials()

    case credentials do
      :undefined ->
        raise "No AWS credentials found in env or in credentials cache"

      %{} ->
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
            if insufficient_capacity?(body) and remaining != [] do
              {next_type, next_subnet} = hd(remaining)

              Logger.warning(
                "No capacity for #{instance_type} in #{subnet_id}, " <>
                  "trying #{next_type} in #{next_subnet}"
              )

              run_instances_with_fallback!(state, remaining)
            else
              Logger.error("Failed to create instance with status #{status} and errors: #{inspect(body)}")
              raise "Bad status #{status} with errors: #{inspect(body)}"
            end

          {:ok, %Req.Response{} = resp} ->
            resp = if xml?(resp), do: update_in(resp.body, &XML.decode!/1), else: resp

            resp.body
            |> Map.fetch!("RunInstancesResponse")
            |> Map.fetch!("instancesSet")
            |> Map.fetch!("item")

          {:error, exception} ->
            Logger.error("Failed to create instance with exception: #{inspect(exception)}")
            raise "Failed to create instance: #{inspect(exception)}"
        end
    end
  end

  defp insufficient_capacity?(body) when is_binary(body) do
    String.contains?(body, "InsufficientInstanceCapacity")
  end

  defp insufficient_capacity?(_), do: false

  def build_params_from_state(%BackendState{} = state) do
    state.config
    |> params_from_config(state.runner_env)
    |> Map.merge(instance_tags(state))
    |> Map.put("Action", "RunInstances")
    |> flatten_json_object()
    |> Map.filter(fn {_k, v} -> not is_nil(v) end)
  end

  defp instance_tags(%BackendState{} = state) do
    %{
      "TagSpecification" => [
        %{
          "ResourceType" => "instance",
          "Tag" => [
            %{
              "Key" => "Name",
              "Value" => "#{state.config.app}-flame-worker"
            },
            %{
              "Key" => "FLAME_PARENT_IP",
              "Value" => state.config.local_ip
            },
            %{
              "Key" => "FLAME_PARENT_APP",
              "Value" => state.config.app
            }
          ]
        }
      ]
    }
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
    |> maybe_put_spot_options(config)
    |> maybe_put_root_volume(config)
  end

  defp maybe_put_spot_options(params, %Config{spot: true} = config) do
    Map.put(params, "InstanceMarketOptions", %{
      "MarketType" => "spot",
      "SpotOptions" => %{
        "MaxPrice" => config.spot_max_price,
        "SpotInstanceType" => "one-time",
        "InstanceInterruptionBehavior" => "terminate"
      }
    })
  end

  defp maybe_put_spot_options(params, %Config{}) do
    params
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

  defp raise_or_response!({:ok, %Req.Response{status: status, body: body}}) when status >= 300 do
    Logger.error("Failed to create instance with status #{status} and errors: #{inspect(body)}")
    raise "Bad status #{status} with errors: #{inspect(body)}"
  end

  defp raise_or_response!({:ok, %Req.Response{} = resp}) do
    if xml?(resp) do
      update_in(resp.body, &XML.decode!/1)
    else
      resp
    end
  end

  defp raise_or_response!({:error, exception}) do
    Logger.error("Failed to create instance with exception: #{inspect(exception)}")
    raise exception
  end

  @doc """
  Describe running instances with a specific tag value.
  Returns {:ok, [%{instance_id: id, private_ip: ip}]} or {:error, reason}.
  """
  def describe_instances_by_tag(tag_key, tag_value) do
    credentials = :aws_credentials.get_credentials()

    case credentials do
      :undefined -> {:error, :no_credentials}
      %{} ->
        # Get region from credentials or default
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

        result =
          [url: uri, method: :post, form: params, aws_sigv4: Map.put_new(credentials, :service, "ec2")]
          |> Req.new()
          |> Req.request()

        case result do
          {:ok, %Req.Response{status: 200, body: body}} ->
            resp = if String.starts_with?(body, "<?xml"), do: XML.decode!(body), else: body
            instances = parse_describe_instances(resp)
            {:ok, instances}

          {:ok, %Req.Response{status: status, body: body}} ->
            {:error, "DescribeInstances failed: #{status} #{inspect(body)}"}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp parse_describe_instances(resp) do
    case get_in(resp, ["DescribeInstancesResponse", "reservationSet"]) do
      nil -> []
      %{"item" => items} when is_list(items) ->
        Enum.flat_map(items, &parse_reservation/1)
      %{"item" => item} when is_map(item) ->
        parse_reservation(item)
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
    %{
      instance_id: Map.get(item, "instanceId"),
      private_ip: Map.get(item, "privateIpAddress")
    }
  end

  @doc """
  Terminate EC2 instances by instance IDs.
  """
  def terminate_instances(instance_ids) when is_list(instance_ids) do
    credentials = :aws_credentials.get_credentials()

    case credentials do
      :undefined -> {:error, :no_credentials}
      %{} ->
        region = Map.get(credentials, :region, "eu-west-1")
        uri = "https://ec2.#{region}.amazonaws.com/"

        id_params =
          instance_ids
          |> Enum.with_index(1)
          |> Map.new(fn {id, i} -> {"InstanceId.#{i}", id} end)

        params = Map.merge(%{"Action" => "TerminateInstances", "Version" => "2016-11-15"}, id_params)

        result =
          [url: uri, method: :post, form: params, aws_sigv4: Map.put_new(credentials, :service, "ec2")]
          |> Req.new()
          |> Req.request()

        case result do
          {:ok, %Req.Response{status: 200}} -> :ok
          {:ok, %Req.Response{status: status, body: body}} ->
            Logger.error("TerminateInstances failed: #{status} #{inspect(body)}")
            {:error, "TerminateInstances failed: #{status}"}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  # Adapted from https://github.com/livebook-dev/livebook/blob/v0.14.5/lib/livebook/file_system/s3/client.ex
  defp xml?(response) do
    guess_xml? = String.starts_with?(response.body, "<?xml")

    case FlameEC2.EC2Api.Utils.fetch_content_type(response) do
      {:ok, content_type} when content_type in ["text/xml", "application/xml"] -> true
      # Apparently some requests return XML without content-type
      :error when guess_xml? -> true
      _otherwise -> false
    end
  end
end
