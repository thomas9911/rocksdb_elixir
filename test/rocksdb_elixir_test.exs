defmodule RocksdbElixirTest do
  use ExUnit.Case, async: true
  doctest RocksdbElixir

  @tag :tmp_dir
  test "roundtrip", %{tmp_dir: tmp_dir} do
    path = "#{tmp_dir}/testing"
    {:ok, conn} = RocksdbElixir.new(path)
    assert {:ok, nil} == RocksdbElixir.get(conn, "key")
    {:ok, conn} = RocksdbElixir.put(conn, "key", "value")
    assert {:ok, "value"} == RocksdbElixir.get(conn, "key")
    {:ok, conn} = RocksdbElixir.delete(conn, "key")
    assert {:ok, nil} == RocksdbElixir.get(conn, "key")

    assert conn.path == path
  end

  @tag :tmp_dir
  test "async", %{tmp_dir: tmp_dir} do
    path = "#{tmp_dir}/async_testing"
    {:ok, conn} = RocksdbElixir.new(path)

    0..1000
    |> Task.async_stream(
      fn x ->
        assert {:ok, _} = RocksdbElixir.put(conn, "key_#{x}", x)
      end,
      ordered: false,
      max_concurrency: 8
    )
    |> Stream.run()

    assert {:ok, 200} == RocksdbElixir.get(conn, "key_200")
    assert {:ok, 900} == RocksdbElixir.get(conn, "key_900")
  end

  test "flush and destroy" do
    # dont use tmp folder this test should clean itself
    path = "test/flush_testing"
    {:ok, conn} = RocksdbElixir.new(path)
    RocksdbElixir.put(conn, "key", "value")

    assert :ok == RocksdbElixir.flush(conn)
    assert {:ok, conn} = RocksdbElixir.close(conn)
    assert :ok == RocksdbElixir.destroy(conn.path)
  end

  @tag :tmp_dir
  test "flush and destroy without overwriting conn", %{tmp_dir: tmp_dir} do
    path = "#{tmp_dir}/flush_without_overwriting_testing"
    {:ok, conn} = RocksdbElixir.new(path)
    RocksdbElixir.put(conn, "key", "value")

    assert :ok == RocksdbElixir.flush(conn)
    assert {:ok, conn_copied} = RocksdbElixir.close(conn)
    assert %RocksdbElixir.Conn{} = conn
    assert {:error, _} = RocksdbElixir.destroy(conn_copied.path)
  end
end
