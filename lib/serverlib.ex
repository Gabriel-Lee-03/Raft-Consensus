
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2
# Gabriel Lee (gl721) and Hou Wang Wong (hww21)

defmodule ServerLib do

# Library of functions called by other server-side modules

def handle_RPC_timeout(server, %{term: term, followerP: followerP }) do
  case server.role do
    :CANDIDATE ->
      server |> Vote.request_vote(followerP)
    :LEADER ->
      server |> AppendEntries.send_append_entries(followerP)
    _ ->
      server
  end
end

def stepdown(server, term) do
  if term > server.curr_term do
    server
    |> Debug.info("stepping down from #{term}", 3)
    |> State.curr_term(term)
    |> State.role(:FOLLOWER)
    |> State.voted_for(nil)
    |> State.leaderP(nil)
    |> Timer.restart_election_timer()
    |> Debug.info("stepped down, curr_term is ##{server.curr_term}", 3)
  else
    server
  end
end

def become_leader(server) do
  server
  |> State.role(:LEADER)
  |> State.leaderP(server.selfP)
  |> State.init_next_index()
  |> State.init_match_index()
  |> Timer.send_sleep_timer()
  |> Debug.info("#{server.server_num} is now leader at term #{server.curr_term}", 2)
end

def send_all_database_request(server) do
  if server.commit_index > server.last_applied do
    Debug.info(server, "Server #{server.server_num} has commit_index #{server.commit_index} larger than last applied #{server.last_applied}.", 3)
    Enum.reduce((Enum.to_list(server.last_applied + 1..server.commit_index)), server, fn idx, serv -> serv |> ServerLib.send_database_request(idx) end)
  else
    server
  end
end

def send_database_request(server, index) do
  Debug.info(server, "Server #{server.server_num} has log size #{Log.last_index(server)}, searching index #{index}", 3)
  request_entry = Log.request_at(server, index)
  cid = request_entry.cid
  cmd = request_entry.cmd
  unless MapSet.member?(server.applied, cid) do
    server =
      server
      |> State.applied(cid)
    send(server.databaseP, { :DB_REQUEST, %{cmd: cmd} })
    server
  else
    server
  end
end

def handle_db_reply(server, db_result) do
  server = server|> State.last_applied(server.last_applied + 1)
  if server.role == :LEADER do
    request_entry = Log.request_at(server, server.last_applied)
    send(request_entry.clientP, { :CLIENT_REPLY, %{cid: request_entry.cid, reply: db_result, leaderP: server.selfP}})
  end
  server
end

end # ServerLib
