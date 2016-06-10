defmodule KafkaEx.ServerBase do
  defmacro helpers do
    quote do
      alias KafkaEx.Protocol, as: Proto
      alias KafkaEx.ServerBase.State
      require Logger
      use GenServer
      require Logger
      alias KafkaEx.Protocol, as: Proto

      @client_id "kafka_ex"
      @retry_count 3
      @metadata_update_interval 30_000
      @sync_timeout 1_000
      @wait_time 10
      @min_bytes 1
      @max_bytes 1_000_000

      defmodule State do
        defstruct(metadata: %Proto.Metadata.Response{},
        brokers: [],
        event_pid: nil,
        consumer_metadata: %Proto.ConsumerMetadata.Response{},
        correlation_id: 0,
        consumer_group: nil,
        metadata_update_interval: nil,
        consumer_group_update_interval: nil,
        worker_name: KafkaEx.Server,
        sync_timeout: nil)
      end

      def start_link(args), do: start_link(args, KafkaEx.server)
      def start_link(args, :no_name) do
        GenServer.start_link(KafkaEx.server, [args])
      end
      def start_link(args, name) do
        GenServer.start_link(__MODULE__, [args, name], [name: name])
      end

      def init([args]) do
        init([args, self])
      end

      def handle_call({:metadata, topic}, _from, state) do
        {correlation_id, metadata} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout, topic)
        state = %{state | metadata: metadata, correlation_id: correlation_id}
        {:reply, metadata, state}
      end

      def handle_call({:produce, produce_request}, _from, state) do
        correlation_id = state.correlation_id + 1
        produce_request_data = Proto.Produce.create_request(correlation_id, @client_id, produce_request)
        {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, produce_request.topic, produce_request.partition) do
          nil    ->
            {correlation_id, _} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout, produce_request.topic)
            state = %{update_metadata(state) | correlation_id: correlation_id}
            {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, produce_request.topic, produce_request.partition), state}
          broker -> {broker, state}
        end

        response = case broker do
          nil    ->
            Logger.log(:error, "Leader for topic #{produce_request.topic} is not available")
            :leader_not_available
          broker -> case produce_request.required_acks do
            0 ->  KafkaEx.NetworkClient.send_async_request(broker, produce_request_data)
            _ -> KafkaEx.NetworkClient.send_sync_request(broker, produce_request_data, state.sync_timeout) |> Proto.Produce.parse_response
          end
        end

        state = %{state | correlation_id: correlation_id + 1}
        {:reply, response, state}
      end

      def handle_call({:offset, topic, partition, time}, _from, state) do
        offset_request = Proto.Offset.create_request(state.correlation_id, @client_id, topic, partition, time)
        {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition) do
          nil    ->
            state = update_metadata(state)
            {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition), state}
          broker -> {broker, state}
        end

        {response, state} = case broker do
          nil ->
            Logger.log(:error, "Leader for topic #{topic} is not available")
            {:topic_not_found, state}
          _ ->
            response = broker
             |> KafkaEx.NetworkClient.send_sync_request(offset_request, state.sync_timeout)
             |> Proto.Offset.parse_response
            state = %{state | correlation_id: state.correlation_id + 1}
            {response, state}
        end

        {:reply, response, state}
      end

      def handle_call({:create_stream, handler, handler_init}, _from, state) do
        if state.event_pid && Process.alive?(state.event_pid) do
          info = Process.info(self)
          Logger.log(:warn, "'#{info[:registered_name]}' already streaming handler '#{handler}'")
        else
          {:ok, event_pid}  = GenEvent.start_link
          state = %{state | event_pid: event_pid}
          :ok = GenEvent.add_handler(state.event_pid, handler, handler_init)
        end
        {:reply, GenEvent.stream(state.event_pid), state}
      end

      def handle_info(:stop_streaming, state) do
        Logger.log(:debug, "Stopped worker #{inspect state.worker_name} from streaming")
        GenEvent.stop(state.event_pid)
        {:noreply, %{state | event_pid: nil}}
      end

      def handle_info(:update_metadata, state) do
        {:noreply, update_metadata(state)}
      end

      def terminate(_, state) do
        Logger.log(:debug, "Shutting down worker #{inspect state.worker_name}")
        if state.event_pid do
          GenEvent.stop(state.event_pid)
        end
        Enum.each(state.brokers, fn(broker) -> KafkaEx.NetworkClient.close_socket(broker.socket) end)
      end

      defp init_helper([args, name]) do
        uris = Keyword.get(args, :uris, [])
        metadata_update_interval = Keyword.get(args, :metadata_update_interval, @metadata_update_interval)
        brokers = Enum.map(uris, fn({host, port}) -> %Proto.Metadata.Broker{host: host, port: port, socket: KafkaEx.NetworkClient.create_socket(host, port)} end)
        sync_timeout = Keyword.get(args, :sync_timeout, Application.get_env(:kafka_ex, :sync_timeout, @sync_timeout))
        {correlation_id, metadata} = retrieve_metadata(brokers, 0, sync_timeout)
        state = %State{metadata: metadata, brokers: brokers, correlation_id: correlation_id, metadata_update_interval: metadata_update_interval, worker_name: name, sync_timeout: sync_timeout}
        {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

        {:ok, state}
      end

      def update_metadata(state) do
        {correlation_id, metadata} = retrieve_metadata(state.brokers, state.correlation_id, state.sync_timeout)
        metadata_brokers = metadata.brokers
        brokers = state.brokers
          |> remove_stale_brokers(metadata_brokers)
          |> add_new_brokers(metadata_brokers)
        %{state | metadata: metadata, brokers: brokers, correlation_id: correlation_id + 1}
      end

      def retrieve_metadata(brokers, correlation_id, sync_timeout, topic \\ []), do: retrieve_metadata(brokers, correlation_id, sync_timeout, topic, @retry_count, 0)
      def retrieve_metadata(_, correlation_id, _sync_timeout, topic, 0, error_code) do
        Logger.log(:error, "Metadata request for topic #{inspect topic} failed with error_code #{inspect error_code}")
        {correlation_id, %Proto.Metadata.Response{}}
      end
      def retrieve_metadata(brokers, correlation_id, sync_timeout, topic, retry, _error_code) do
        metadata_request = Proto.Metadata.create_request(correlation_id, @client_id, topic)
        data = first_broker_response(metadata_request, brokers, sync_timeout)
        response = case data do
                     nil ->
                       Logger.log(:error, "Unable to fetch metadata from any brokers.  Timeout is #{sync_timeout}.")
                       raise "Unable to fetch metadata from any brokers.  Timeout is #{sync_timeout}."
                       :no_metadata_available
                     data ->
                       Proto.Metadata.parse_response(data)
                   end

                   case Enum.find(response.topic_metadatas, &(&1.error_code == :leader_not_available)) do
          nil  -> {correlation_id + 1, response}
          topic_metadata ->
            :timer.sleep(300)
            retrieve_metadata(brokers, correlation_id + 1, sync_timeout, topic, retry - 1, topic_metadata.error_code)
        end
      end

      defp remove_stale_brokers(brokers, metadata_brokers) do
        {brokers_to_keep, brokers_to_remove} = Enum.partition(brokers, fn(broker) ->
          Enum.find_value(metadata_brokers, &(broker.host == &1.host && broker.port == &1.port && broker.socket && Port.info(broker.socket)))
        end)
        case length(brokers_to_keep) do
          0 -> brokers_to_remove
          _ -> Enum.each(brokers_to_remove, fn(broker) ->
            Logger.log(:info, "Closing connection to broker #{inspect broker.host} on port #{inspect broker.port}")
            KafkaEx.NetworkClient.close_socket(broker.socket)
          end)
            brokers_to_keep
        end
      end

      defp add_new_brokers(brokers, []), do: brokers
      defp add_new_brokers(brokers, [metadata_broker|metadata_brokers]) do
        case Enum.find(brokers, &(metadata_broker.host == &1.host && metadata_broker.port == &1.port)) do
          nil -> Logger.log(:info, "Establishing connection to broker #{inspect metadata_broker.host} on port #{inspect metadata_broker.port}")
            add_new_brokers([%{metadata_broker | socket: KafkaEx.NetworkClient.create_socket(metadata_broker.host, metadata_broker.port)} | brokers], metadata_brokers)
          _ -> add_new_brokers(brokers, metadata_brokers)
        end
      end


      defp first_broker_response(request, state) do
        first_broker_response(request, state.brokers, state.sync_timeout)
      end
      defp first_broker_response(request, brokers, sync_timeout) do
        Enum.find_value(brokers, fn(broker) ->
          if Proto.Metadata.Broker.connected?(broker) do
            KafkaEx.NetworkClient.send_sync_request(broker, request, sync_timeout)
          end
        end)
      end
    end
  end
end
