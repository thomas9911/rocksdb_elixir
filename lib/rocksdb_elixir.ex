defmodule RocksDBElixir do
  @moduledoc """
  Documentation for `RocksDBElixir`.

  ## Example

  ```elixir
  iex> {:ok, conn} = RocksDBElixir.new("test/module_doc")
  iex> RocksDBElixir.get(conn, "key")
  {:ok, nil}
  iex> {:ok, conn} = RocksDBElixir.put(conn, "key", "value")
  iex> RocksDBElixir.get(conn, "key")
  {:ok, "value"}
  iex> {:ok, conn} = RocksDBElixir.delete(conn, "key")
  iex> {:ok, _conn} = RocksDBElixir.close(conn)
  iex> RocksDBElixir.destroy("test/module_doc")
  :ok
  ```
  """

  alias RocksDBElixir.Conn

  ## User interface

  @doc "Open a new connection, will create database if it does not exist"
  @spec new(binary) :: {:ok, Conn.t()} | {:error, binary}
  defdelegate new(path), to: RocksDBElixir.Native

  @doc "Get data from the database"
  @spec get(Conn.t(), term) :: {:ok, term | nil} | {:error, binary}
  def get(conn, key) do
    case RocksDBElixir.Native.get(conn, key_hash(key)) do
      {:ok, nil} -> {:ok, nil}
      {:ok, data} -> {:ok, to_t(data)}
      err -> err
    end
  end

  @doc "Put data into the database"
  @spec put(Conn.t(), term, term) :: {:ok, Conn.t()} | {:error, binary}
  def put(conn, key, value) do
    RocksDBElixir.Native.put(conn, key_hash(key), to_b(value))
  end

  @doc "Delete data from the database"
  @spec delete(Conn.t(), term) :: {:ok, Conn.t()} | {:error, binary}
  def delete(conn, key) do
    RocksDBElixir.Native.delete(conn, key_hash(key))
  end

  ## Administation

  @doc "Close connection to the database"
  @spec close(Conn.t()) :: {:ok, Conn.t()} | {:error, binary}
  def close(conn) do
    # overwriting conn is important
    {:ok, conn} = RocksDBElixir.Native.close(conn)

    # force garbage_collect so resource will be cleaned
    true = :erlang.garbage_collect(self())
    Process.sleep(10)

    {:ok, conn}
  end

  @doc "Force flush the database to disk"
  @spec flush(Conn.t()) :: :ok | {:error, binary}
  def flush(conn) do
    case RocksDBElixir.Native.flush(conn) do
      {:ok, {}} -> :ok
      e -> e
    end
  end

  @doc """
  Destroy and removes the database, can only be done when no connections are open to it
  """
  @spec destroy(binary) :: :ok | {:error, binary}
  def destroy(path) do
    case RocksDBElixir.Native.destroy(path) do
      {:ok, {}} -> :ok
      e -> e
    end
  end

  ## Helpers

  @spec key_hash(term) :: binary
  def key_hash(data) do
    data
    |> :erlang.phash2(4_294_967_295)
    |> :binary.encode_unsigned(:big)
  end

  defdelegate to_b(data), to: :erlang, as: :term_to_binary
  defdelegate to_t(data), to: :erlang, as: :binary_to_term
end
