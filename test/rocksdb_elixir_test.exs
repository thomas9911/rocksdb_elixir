defmodule RocksDBElixirTest do
  use ExUnit.Case, async: true
  doctest RocksDBElixir

  @tag :tmp_dir
  test "roundtrip", %{tmp_dir: tmp_dir} do
    path = "#{tmp_dir}/testing"
    {:ok, conn} = RocksDBElixir.new(path)
    assert {:ok, nil} == RocksDBElixir.get(conn, "key")
    {:ok, conn} = RocksDBElixir.put(conn, "key", "value")
    assert {:ok, "value"} == RocksDBElixir.get(conn, "key")
    {:ok, conn} = RocksDBElixir.delete(conn, "key")
    assert {:ok, nil} == RocksDBElixir.get(conn, "key")

    assert conn.path == path
  end

  @tag :tmp_dir
  test "async", %{tmp_dir: tmp_dir} do
    path = "#{tmp_dir}/async_testing"
    {:ok, conn} = RocksDBElixir.new(path)

    0..1000
    |> Task.async_stream(
      fn x ->
        assert {:ok, _} = RocksDBElixir.put(conn, "key_#{x}", x)
      end,
      ordered: false,
      max_concurrency: 8
    )
    |> Stream.run()

    assert {:ok, 200} == RocksDBElixir.get(conn, "key_200")
    assert {:ok, 900} == RocksDBElixir.get(conn, "key_900")
  end

  test "flush and destroy" do
    # dont use tmp folder this test should clean itself
    path = "test/flush_testing"
    {:ok, conn} = RocksDBElixir.new(path)
    RocksDBElixir.put(conn, "key", "value")

    assert :ok == RocksDBElixir.flush(conn)
    assert {:ok, conn} = RocksDBElixir.close(conn)
    assert :ok == RocksDBElixir.destroy(conn.path)
  end

  @tag :tmp_dir
  test "flush and destroy without overwriting conn", %{tmp_dir: tmp_dir} do
    path = "#{tmp_dir}/flush_without_overwriting_testing"
    {:ok, conn} = RocksDBElixir.new(path)
    RocksDBElixir.put(conn, "key", "value")

    assert :ok == RocksDBElixir.flush(conn)
    assert {:ok, conn_copied} = RocksDBElixir.close(conn)
    assert %RocksDBElixir.Conn{} = conn
    assert {:error, _} = RocksDBElixir.destroy(conn_copied.path)
  end
end
