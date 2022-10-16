defmodule RocksDBElixir.Conn do
  @moduledoc "Module that holds the connection to the database"
  defstruct [:resource, :path]

  @type t :: %__MODULE__{path: binary, resource: reference | nil}
end
