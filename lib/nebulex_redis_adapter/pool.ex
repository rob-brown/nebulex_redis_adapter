defmodule NebulexRedisAdapter.Pool do
  @moduledoc false

  ## API

  @spec register_names(atom, term, pos_integer, ({:via, module, term} -> term)) :: [term]
  def register_names(registry, key, pool_size, fun) do
    for index <- 0..(pool_size - 1) do
      fun.({:via, Registry, {registry, {key, index}}})
    end
  end

  @spec get_conn(atom, term, pos_integer) :: pid
  def get_conn(registry, key, pool_size) do
    # Ensure selecting the same connection based on the caller PID
    index = :erlang.phash2(self(), pool_size)

    registry
    |> Registry.lookup({key, index})
    |> hd()
    |> elem(0)
  end

  def get_role_conn(registry, slot_id, pool_size) do
    # Ensure selecting the same connection based on the caller PID
    index = :erlang.phash2(self(), pool_size)

    # Ensure the same role based on the caller PID
    case :erlang.phash2(self(), 2) do
      0 ->
        # Fallback to replica if master offline
        get_master_conn(registry, slot_id, index) || get_replica_conn(registry, slot_id, index)

      1 ->
        # Fallback to master if replica offline
        get_replica_conn(registry, slot_id, index) || get_master_conn(registry, slot_id, index)
    end
  end

  defp get_master_conn(registry, slot_id, index) do
    registry
    |> Registry.lookup({{slot_id, "master"}, index})
    |> case do
      [{pid, _value} | _] -> pid
      [] -> nil
    end
  end

  defp get_replica_conn(registry, slot_id, index) do
    registry
    |> Registry.lookup({{slot_id, "replica"}, index})
    |> case do
      [{pid, _value} | _] -> pid
      [] -> nil
    end
  end
end
