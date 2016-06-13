defmodule KafkaEx.Protocol.DescribeGroup do
  import KafkaEx.Protocol.Common

  defmodule Response do
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
  def create_request(group_name) do
    << byte_size(group_name) :: 16-signed, group_name :: binary >>
  end
end
