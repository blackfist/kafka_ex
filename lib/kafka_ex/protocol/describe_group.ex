defmodule KafkaEx.Protocol.DescribeGroup do
  # described here: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-DescribeGroupsRequest
  import KafkaEx.Protocol.Common

  defmodule Response do
    # described here: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-DescribeGroupsResponse
    defstruct error_code: nil,
      group_id: nil,
      state: nil,
      protocol_type: nil,
      protocol: nil,
      members: []

    @type t :: %Response{error_code: atom | integer,
      group_id: binary,
      state: binary,
      protocol_type: binary,
      protocol: binary,
      members: [binary]}
  end

  @spec create_request(binary) :: binary
  def create_request(correlation_id, client_id, group_name) do
    KafkaEx.Protocol.create_request(:describe_group, correlation_id, client_id) <>
    << byte_size(group_name) :: 16-signed, group_name :: binary >>
  end
end
