# State transitions:
#  :waiting_for_message => receive PING, send PONG => :waiting_for_message
#  :waiting_for_message => receive MSG... -> :waiting_for_message

defmodule Gnat.Stream do
  use GenServer
  require Logger
  alias Gnat.{Command, Parsec}
  alias Gnat.Stream.Proto
  alias Proto.{Heartbeat, ConnectResponse, SubscriptionResponse, MsgProto, PubAck}

  @type message :: %{topic: String.t, body: String.t, sid: non_neg_integer(), reply_to: String.t}

  @default_connection_settings %{
    host: 'localhost',
    port: 4223,
    tcp_opts: [:binary],
    connection_timeout: 3_000,
    ssl_opts: [],
    tls: false,
    cluster_id: "test-cluster",
    client_id: "gnat",
    discover_prefix: "_STAN.discover",
    deliver_to: nil

  }

  def start_link(connection_settings \\ %{}, opts \\ []) do
    Logger.debug ">>>> Gnat.Stream.start_link connection_settings=#{inspect connection_settings} opts=#{inspect opts}"
    GenServer.start_link(__MODULE__, connection_settings, opts)
  end

  @impl GenServer
  def init(connection_settings) do
    Logger.debug ">>>> Gnat.Stream.init connection_settings=#{inspect connection_settings}"
    connection_settings = Map.merge(@default_connection_settings, connection_settings)

    {:ok, conn} = Gnat.start_link(connection_settings)

    sid = :crypto.strong_rand_bytes(4) |> Base.encode16()
    heart_inbox = "Heartbeat.#{sid}"

    {:ok, heart_sid} = Gnat.sub(conn, self(), heart_inbox)

    options = Enum.into(connection_settings, %{})
    %{
      client_id: client_id,
      cluster_id: cluster_id,
      discover_prefix: discover_prefix
    } = options

    Logger.debug ">>>>>>>> Gnat.Stream.init options=#{inspect options}"

    connect_request = Proto.connect_request(clientID: client_id, heartbeatInbox: heart_inbox)

    Logger.debug ">>>>>>>> Gnat.Stream.init connect_request=#{inspect connect_request}"

    {:ok, %{body: nats_msg}} = Gnat.request(conn, "#{discover_prefix}.#{cluster_id}", connect_request)

    Logger.debug ">>>>>>>> Gnat.Stream.init nats_msg=#{inspect nats_msg}"

    response = ConnectResponse.new(nats_msg)

    connect_attributes = %{
      close_requests: response.close_requests,
      pub_prefix: response.pub_prefix,
      sub_close_requests: response.sub_close_requests,
      sub_requests: response.sub_requests,
      unsub_requests: response.unsub_requests
    }


    if String.length(response.error) != 0 do
      GenServer.stop(conn)
      {:stop, response.error}
    else
      state = options
        |> Map.merge(connect_attributes)
        |> Map.merge(%{
          heart_sid: heart_sid,
          conn: conn,
          subscriptions: %{},
          msg_protos: []
        })

      Logger.debug ">>>> state=#{inspect state}"
      {:ok, state}
    end
  end

  @impl GenServer
  def handle_info({:msg, %{reply_to: reply_to, topic: <<"Heartbeat." <> _heart_sid>>}}, state) do
    Logger.debug ">>>> RCVD Heartbeat reply_to=#{inspect reply_to}"
    Gnat.pub(state.conn, reply_to, "")
    Logger.debug("->> Pulse")

    {:noreply, state}
  end

end
