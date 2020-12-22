defmodule Gnat.Handshake do
  alias Gnat.Parsec
  require Logger

  @moduledoc """
  This module provides a single function which handles all of the variations of establishing a connection to a gnatsd server and just returns {:ok, socket} or {:error, reason}
  """
  def connect(settings) do
    host = settings.host |> to_charlist
    case :gen_tcp.connect(host, settings.port, settings.tcp_opts, settings.connection_timeout) do
      {:ok, tcp} -> perform_handshake(tcp, settings)
      result -> result
    end
  end

  defp perform_handshake(tcp, connection_settings) do
    receive do
      {:tcp, ^tcp, operation} ->
        {_, [{:info, options}]} = Parsec.parse(Parsec.new(), operation)
        {:ok, socket} = upgrade_connection(tcp, options, connection_settings)
        send_connect_message(socket, options, connection_settings)
        {:ok, socket}
      after 1000 ->
        {:error, "timed out waiting for info"}
    end
  end

  # defp socket_write(%{tls: true}, socket, iodata), do: :ssl.send(socket, iodata)
  # defp socket_write(_, socket, iodata), do: :gen_tcp.send(socket, iodata)
  defp socket_write(%{tls: true}, socket, iodata) do
    :ssl.send(socket, iodata)
  end
  defp socket_write(_, socket, iodata) do
    :gen_tcp.send(socket, iodata)
  end

  defp send_connect_message(socket, %{auth_required: true}=_options, %{username: username, password: password}=connection_settings) do
    opts = Jason.encode!(%{user: username, pass: password, verbose: false}, maps: :strict)
    socket_write(connection_settings, socket, "CONNECT #{opts}\r\n")
  end
  defp send_connect_message(socket, %{auth_required: true}=_options, %{token: token}=connection_settings) do
    opts = Jason.encode!(%{auth_token: token, verbose: false}, maps: :strict)
    socket_write(connection_settings, socket, "CONNECT #{opts}\r\n")
  end
  defp send_connect_message(socket, %{auth_required: true, nonce: nonce}=_options, %{nkey_seed: seed}=connection_settings) do
    {:ok, nkey} = NKEYS.from_seed(seed)

    signature = NKEYS.sign(nkey, nonce) |> Base.url_encode64() |> String.replace("=", "")
    params = %{sig: signature, verbose: false, protocol: 1}
    params = if Map.has_key?(connection_settings, :jwt) do
      Map.put(params, :jwt, Map.get(connection_settings, :jwt))
    else
      public = NKEYS.public_nkey(nkey)
      Map.put(params, :nkey, public)
    end
    opts = Jason.encode!(params, maps: :strict)
    socket_write(connection_settings, socket, "CONNECT #{opts}\r\n")
  end
  defp send_connect_message(socket, _options, connection_settings) do
    socket_write(connection_settings, socket, "CONNECT {\"verbose\": false}\r\n")
  end

  defp upgrade_connection(tcp, %{tls_required: true}, %{tls: true, ssl_opts: opts}) do
    :ok = :inet.setopts(tcp, [active: true])
    :ssl.connect(tcp, opts, 1_000)
  end
  defp upgrade_connection(tcp, _server_settings, _connection_settions), do: {:ok, tcp}
end
