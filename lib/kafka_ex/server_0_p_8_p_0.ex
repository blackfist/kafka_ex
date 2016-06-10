defmodule KafkaEx.Server0P8P0 do
  require KafkaEx.ServerBase
  KafkaEx.ServerBase.helpers

  ### GenServer Callbacks

  def init([args, name]) do
    init_helper([args, name])
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes, auto_commit}, _from, state) do
    {response, state} = fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, state, auto_commit)

    {:reply, response, state}
  end

  def handle_call({:offset_fetch, _}, _from, _state), do: raise "Offset Fetch is not supported in 0.8.0 version of kafka"
  def handle_call({:offset_commit, _}, _from, _state), do: raise "Offset Commit is not supported in 0.8.0 version of kafka"
  def handle_call(:consumer_group, _from, _state), do: raise "Consumer Group is not supported in 0.8.0 version of kafka"
  def handle_call({:consumer_group_metadata, _}, _from, _state), do: raise "Consumer Group Metadata is not supported in 0.8.0 version of kafka"
  def handle_call({:join_group, _, _}, _from, _state), do: raise "Join Group is not supported in 0.8.0 version of kafka"
  def handle_call({:sync_group, _, _, _, _}, _from, _state), do: raise "Sync Group is not supported in 0.8.0 version of kafka"
  def handle_call({:heartbeat, _, _, _}, _from, _state), do: raise "Heartbeat is not supported in 0.8.0 version of kafka"

  def handle_info({:start_streaming, _topic, _partition, _offset, _handler, _auto_commit},
                  state = %State{event_pid: nil}) do
    # our streaming could have been canceled with a streaming update in-flight
    {:noreply, state}
  end

  def handle_info({:start_streaming, topic, partition, offset, handler, auto_commit}, state) do
    {response, state} = fetch(topic, partition, offset, @wait_time, @min_bytes, @max_bytes, state, auto_commit)
    offset = case response do
               :topic_not_found ->
                 offset
               _ ->
                 message = response |> hd |> Map.get(:partitions) |> hd
                 Enum.each(message.message_set, fn(message_set) -> GenEvent.notify(state.event_pid, message_set) end)
                 case message.last_offset do
                   nil         -> offset
                   last_offset -> last_offset + 1
                 end
             end

    Process.send_after(self, {:start_streaming, topic, partition, offset, handler, auto_commit}, 500)

    {:noreply, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  defp fetch(topic, partition, offset, wait_time, min_bytes, max_bytes, state, _auto_commit) do
    fetch_request = Proto.Fetch.create_request(state.correlation_id, @client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    {broker, state} = case Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition) do
      nil    ->
        state = update_metadata(state)
        {Proto.Metadata.Response.broker_for_topic(state.metadata, state.brokers, topic, partition), state}
      broker -> {broker, state}
    end

    case broker do
      nil ->
        Logger.log(:error, "Leader for topic #{topic} is not available")
        {:topic_not_found, state}
      _ ->
        response = broker
          |> KafkaEx.NetworkClient.send_sync_request(fetch_request, state.sync_timeout)
          |> Proto.Fetch.parse_response
        state = %{state | correlation_id: state.correlation_id + 1}
        {response, state}
    end
  end
end
