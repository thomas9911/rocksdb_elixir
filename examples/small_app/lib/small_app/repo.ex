defmodule SmallApp.Repo do
  use Supervisor

  @ets :rocksdb_app
  @rocksdb_name "data"

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    Process.flag(:trap_exit, true)
    :ets.new(@ets, [:set, :public, {:read_concurrency, true}, :named_table])
    reconnect()
    Supervisor.init([], strategy: :one_for_one)
  end

  def conn do
    case :ets.lookup(@ets, :conn) do
      [{:conn, pid}] -> pid
      [] -> nil
    end
  end

  def reconnect do
    :ok =
      case RocksDBElixir.new(@rocksdb_name) do
        {:ok, conn} ->
          :ets.insert(@ets, {:conn, conn})
          :ok

        e ->
          e
      end
  end

  def close do
    case :ets.take(@ets, :conn) do
      [{:conn, conn}] ->
        {:ok, _conn} = RocksDBElixir.close(conn)
        :ok

      _ ->
        :ok
    end
  end

  def get(key) do
    RocksDBElixir.get(conn(), key)
  end

  def put(key, value) do
    RocksDBElixir.put(conn(), key, value)
  end

  def delete(key) do
    RocksDBElixir.delete(conn(), key)
  end
end
