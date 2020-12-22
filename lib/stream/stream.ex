# State transitions:
#  :waiting_for_message => receive PING, send PONG => :waiting_for_message
#  :waiting_for_message => receive MSG... -> :waiting_for_message

defmodule Gnat.Stream do
  use GenServer
  require Logger
  alias Gnat.Stream.Subscription
  alias Gnat.Stream.Proto
  alias Proto.{ConnectResponse, SubscriptionResponse, MsgProto, PubAck}
  import Gnat, only: [new_sid: 0]

  @type message :: %{topic: String.t, body: String.t, sid: non_neg_integer(), reply_to: String.t}

  @default_connection_settings %{
    host: 'localhost',
    port: 4223,
    tcp_opts: [:binary],
    connection_timeout: 3_000,
    ssl_opts: [],
    tls: false,
    cluster_id: "test-cluster",
    client_id: "nats_ex",
    discover_prefix: "_STAN.discover",
    deliver_to: nil
  }

  def start_link(connection_settings \\ %{}, opts \\ []) do
    GenServer.start_link(__MODULE__, connection_settings, opts)
  end

  @impl GenServer
  def init(connection_settings) do
    connection_settings = @default_connection_settings
    |> Map.merge(connection_settings)

    {:ok, conn} = Gnat.start_link(connection_settings)

    sid = new_sid()
    heart_inbox = "Heartbeat.#{sid}"

    {:ok, heart_sid} = Gnat.sub(conn, self(), heart_inbox)

    options = Enum.into(connection_settings, %{})
    %{
      client_id: client_id,
      cluster_id: cluster_id,
      discover_prefix: discover_prefix
    } = options

    connect_request = Proto.connect_request(clientID: client_id, heartbeatInbox: heart_inbox)
    {:ok, %{body: nats_msg}} = Gnat.request(conn, "#{discover_prefix}.#{cluster_id}", connect_request)
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

      {:ok, state}
    end
  end

  @doc """
  Publish a message to a topic.

  ## Options

    * `:ack` - Block until the server acknowledges the publish. Default: `true`

  ## Example

      # Receive an ack from server before returning.
      Gnat.Stream.publish(conn, "foo", "hello")

      # Publish without waiting for an ack.
      Gnat.Stream.publish(conn, "foo", "bye", ack: false)

  """
  @defaults %{
    ack: true
  }
  def pub(pid, topic, message, opts \\ []) do
    opts_map = Enum.into(opts, %{})
    opts = Map.merge(@defaults, opts_map)
    GenServer.call(pid, {:pub, topic, message, opts})
  end

  @doc """
  Subscribe to a topic.

  ## Options

    * `:max_in_flight` - Max in flight messages. Default: `1`
    * `:ack_wait_in_secs` - How many seconds to wait for an ack before redelivering. Default: `30`
    * `:start_position` - Where in the topic to start receiving messages from. Default: `:new_only`
    * `:start_sequence` - Start sequence number. Default: `nil`
    * `:start_time_delta` - Start time. Default: `nil`

  ## Options for `:start_position`

    * `:new_only` - Send only new messages.
    * `:last_received` - Send only the last received message.
    * `:time_delta_start` - Send messages from duration specified in the `:start_time_delta` field.
    * `:sequence_start` - Send messages starting from the sequence in the `:start_sequence` field.
    * `:first` - Send all available messages.

  """
  @defaults %{
    max_in_flight: 1,
    ack_wait_in_secs: 30,
    start_position: :new_only
  }
  def sub(pid, subscriber, topic, opts \\ []) do
    opts_map = Enum.into(opts, %{})
    opts = Map.merge(@defaults, opts_map)
    GenServer.call(pid, {:sub, subscriber, topic, opts})
  end

    @doc """
  Acknowledge a message.

  Messages must be acknowledged so the server knows when to send the next
  message and also not to redeliver messages.

  ## Example

      receive do
        {:nats_stream_msg, msg} -> Gnat.Stream.ack(conn, msg)
      end

  """
  def ack(conn, msg) do
    GenServer.call(conn, {:ack, msg})
  end

  @impl GenServer
  def handle_call({:pub, topic, message, opts}, _from, state) do
    %{
      conn: conn,
      client_id: client_id,
      pub_prefix: pub_prefix,
    } = state

    {_guid, pub_msg} = Proto.pub_msg(client_id, topic, message)
    nats_subject = "#{pub_prefix}.#{topic}"

    if opts[:ack] do
      # Request/response because we want the ack.
      {:ok, nats_msg} = Gnat.request(conn, nats_subject, pub_msg)
      ack = PubAck.new(nats_msg[:body])
      Logger.debug("<<- PubAck (#{ack.guid})")
    else
      # Send the publish message without waiting for an ack.
      Gnat.pub(conn, nats_subject, pub_msg)
    end

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:sub, subscriber, topic, opts}, _from, state) do
    %{
      conn: conn,
      client_id: client_id,
      sub_requests: sub_requests,
      subscriptions: subscriptions
    } = state

    # Subscribe to subscription's inbox.
    subscription = Subscription.new(topic: topic)
    {:ok, _heart_sid} = Gnat.sub(conn, self(), subscription.inbox)

    # Make the subscription request.
    Logger.debug "->> SubscriptionRequest (#{topic})"
    subscription_request = Proto.subscription_request(subscription, client_id, opts)
    {:ok, nats_msg} = Gnat.request(conn, sub_requests, subscription_request)
    response = SubscriptionResponse.new(nats_msg[:body])
    new_subscription = put_in(subscription.ack_inbox, response.ack_inbox)
    Logger.debug "<<- SubscriptionResponse (#{topic})"

    # Record the subscription so we can match it up to the SubscriptionResponse.
    new_subscriptions = put_in(subscriptions, [topic], new_subscription)

    # {:reply, :ok, state}
    # :noreply so that the caller is blocked until we get the SubscriptionResponse.
    {:reply, :ok, %{state | subscriptions: new_subscriptions, deliver_to: subscriber}}
  end

  def handle_call({:ack, msg_proto}, _from, state) do
    %{conn: conn, subscriptions: subscriptions} = state

    # Find the corresponding subscription.
    subscription = subscriptions[msg_proto.subject]

    # Send the ack.
    ack_payload = Proto.ack(msg_proto.subject, msg_proto.sequence)
    Logger.debug ">>>> Ack ack_payload = #{inspect ack_payload}"

    Gnat.pub(conn, subscription.ack_inbox, ack_payload)

    # Log something nice.
    Logger.debug "->> Ack (#{msg_proto.subject}, #{msg_proto.sequence})"

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({:msg, %{reply_to: reply_to, topic: <<"Heartbeat." <> _heart_sid>>}}, state) do
    Gnat.pub(state.conn, reply_to, "")
    Logger.debug("->> Pulse")

    {:noreply, state}
  end

  def handle_info({:msg, %{topic: <<"MsgProto." <> _heart_sid>>, body: body}}, state) do
    msg_proto = MsgProto.new(body)

    if state.deliver_to do
      send(state.deliver_to, {:nats_stream_msg, msg_proto})
      {:noreply, state}
    else
      %{msg_protos: msg_protos} = state
      msg_protos = [msg_proto | msg_protos]
      {:noreply, %{state | msg_protos: msg_protos}}
    end

    {:noreply, state}
  end

  def handle_info({:msg, %{reply_to: reply_to, topic: topic, body: body}}, state) do
    Logger.debug ">>>> RCVD MSG topic=#{inspect topic} \r\nreply_to=#{inspect reply_to} \r\nbody=#{inspect body}"

    {:noreply, state}
  end

end
