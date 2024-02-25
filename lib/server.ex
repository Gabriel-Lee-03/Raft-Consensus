
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Server do

# _________________________________________________________ Server.start()
def start(config, server_num) do

  config = config
    |> Configuration.node_info("Server", server_num)
    |> Debug.node_starting()

  receive do
  { :BIND, servers, databaseP } ->
    config
    |> State.initialise(server_num, servers, databaseP)
    |> Timer.restart_election_timer()
    # |> Debug.info("election done?")
    |> Server.next()
  end # receive
end # start

# _________________________________________________________ next()
def next(server) do

  # invokes functions in AppendEntries, Vote, ServerLib etc

  server = receive do

  { :APPEND_ENTRIES_REQUEST, msg} ->
    server |> AppendEntries.handle_append_entries_request(msg)

  { :APPEND_ENTRIES_REPLY, msg} ->
    server |> AppendEntries.handle_append_entries_reply(msg)

  { :APPEND_ENTRIES_TIMEOUT, msg } ->
    server |> ServerLib.handle_RPC_timeout(msg)

  { :VOTE_REQUEST, msg } ->
    server |> Vote.handle_vote_request(msg)

  { :VOTE_REPLY, msg } ->
    server |> Vote.handle_vote_reply(msg)

  { :ELECTION_TIMEOUT, %{term: term, election: election} } ->
    server |> Vote.hold_election(term)

  { :CLIENT_REQUEST, msg } -> server

   unexpected ->
      Helper.node_halt("***** Server: unexpected message #{inspect unexpected}")

  end # receive

  server |> Server.next()

end # next

end # Server
