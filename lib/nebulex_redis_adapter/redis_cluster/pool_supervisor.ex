defmodule NebulexRedisAdapter.RedisCluster.PoolSupervisor do
  @moduledoc """
  Redis Cluster Node/Slot Supervisor.
  """

  use Supervisor

  alias NebulexRedisAdapter.Pool

  ## API

  @doc false
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  ## Supervisor Callbacks

  @impl true
  def init(opts) do
    slot_id = Keyword.fetch!(opts, :slot_id)
    registry = Keyword.fetch!(opts, :registry)
    pool_size = Keyword.fetch!(opts, :pool_size)
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    role = Keyword.fetch!(opts, :role)

    key = {slot_id, role}

    conn_opts =
      opts
      |> Keyword.fetch!(:conn_opts)
      |> Keyword.delete(:url)
      |> Keyword.put(:host, host)
      |> Keyword.put(:port, port)

    children =
      Pool.register_names(registry, key, pool_size, fn conn_name ->
        conn_opts = Keyword.put(conn_opts, :name, conn_name)

        Supervisor.child_spec({Redix, conn_opts}, id: {Redix, conn_name})
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
