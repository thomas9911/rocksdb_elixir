defmodule SmallAppTest do
  use ExUnit.Case
  doctest SmallApp

  test "greets the world" do
    assert SmallApp.hello() == :world
  end
end
