
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Vote do

def hold_election(server) do
  if server.role == :CANDIDATE || server.role == :FOLLOWER do
    server
    |> Timer.restart_election_timer()
    |> State.inc_term()
    |> State.role(:CANDIDATE)
    |> State.voted_for(server.selfP)
    |> State.new_voted_by()
    |> State.add_to_voted_by(server.selfP)
    |> Vote.request_vote_all()
  end
  server
end

def request_vote_all(server) do
  server |> Timer.cancel_all_append_entries_timers()

  for voterP <- server.servers, voterP != server.selfP do
    if voterP != server.selfP do
      server |> Vote.request_vote(voterP)
    end
  end
  server
end

def request_vote(server, voterP) do
  vote_req_msg = { :VOTE_REQUEST, %{from: server.selfP, term: server.curr_term} }
  server |> Timer.restart_append_entries_timer(voterP)
  Process.send(voterP, vote_req_msg)
  server
end

def handle_vote_request(server, %{from: req_id, term: req_term}) do
  if req_term > server.curr_term do
    server |> Vote.stepdown(req_term)
  end

  if req_term == server.curr_term do
    if server.voted_for == req_id || server.voted_for == nil do
      server
      |> State.voted_for(req_id)
      |> Timer.restart_election_timer()
    end
  end
  Process.send(req_id, { :VOTE_REPLY, %{from: server.selfP, term: server.curr_term, voted_for: server.voted_for} })
  server
end
  
def handle_vote_reply(server, %{from: rep_from, term: rep_term, voted_for: rep_voted_for}) do
  if rep_term > server.curr_term do
    server |> Vote.stepdown(rep_term)
  end

  if rep_term == server.curr_term and server.role == :CANDIDATE do
    if rep_voted_for == server.selfP do
      server |> State.add_to_voted_by(rep_from)
    end
    server |> Timer.cancel_append_entries_timer(rep_from)

    if State.vote_tally(server) >= server.majority do
      server
      |> State.role(:LEADER)
      |> State.leader(server.selfP)
      for followerP <- server.servers, followerP != server.selfP do
        server |> AppendEntries.send_append_entries(followerP)
      end
    end
  end
end

def stepdown(server, term) do
  server
  |> State.curr_term(term)
  |> State.role(:FOLLOWER)
  |> State.voted_for(nil)
  |> Timer.restart_election_timer()
end

end # Vote
