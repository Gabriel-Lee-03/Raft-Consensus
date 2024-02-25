
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule ServerLib do

# Library of functions called by other server-side modules

def handle_RPC_timeout(server, %{term: term, followerP: followerP }) do
  case server.state do
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
    |> Debug.info("stepping down from #{term}")
    |> State.curr_term(term)
    |> State.role(:FOLLOWER)
    |> State.voted_for(nil)
    |> State.leaderP(nil)
    |> Timer.restart_election_timer()
    |> Debug.info("stepped down, curr_term is ##{server.curr_term}")
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
  |> Debug.info("#{server.server_num} is now leader")
end

end # ServerLib
