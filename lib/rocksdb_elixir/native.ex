defmodule RocksDBElixir.Native do
  @moduledoc "Module that contains the NIFs"
  use Rustler, otp_app: :rocksdb_elixir, crate: "rocksdb_elixir"

  def new(_path), do: :erlang.nif_error(:nif_not_loaded)
  def flush(_conn), do: :erlang.nif_error(:nif_not_loaded)
  def close(_conn), do: :erlang.nif_error(:nif_not_loaded)
  def destroy(_path), do: :erlang.nif_error(:nif_not_loaded)

  def put(_conn, _key, _value), do: :erlang.nif_error(:nif_not_loaded)
  def get(_conn, _key), do: :erlang.nif_error(:nif_not_loaded)
  def delete(_conn, _key), do: :erlang.nif_error(:nif_not_loaded)
end
