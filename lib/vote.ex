
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2
# Gabriel Lee (gl721) and Hou Wang Wong (hww21)

defmodule Vote do

def hold_election(server, term) do
  Debug.assert(server, term <= server.curr_term, "Election timeout term #{term} greater than server curr_term #{server.curr_term}!")

  if server.role == :CANDIDATE || server.role == :FOLLOWER do
    server
    |> Debug.info("Holding election for #{server.server_num} at term #{server.curr_term} with timeout term #{term}", 2)
    |> Timer.restart_election_timer()
    |> State.inc_term()
    |> State.role(:CANDIDATE)
    |> State.voted_for(server.selfP)
    |> State.new_voted_by()
    |> State.add_to_voted_by(server.selfP)
    |> Vote.request_vote_all()
    |> Debug.info("#{server.server_num} has held election", 3)
  else
    server
  end
end

def request_vote_all(server) do
  server = server
  |> Timer.cancel_all_append_entries_timers()
  |> Debug.info("#{server.server_num} in request_vote_all", 3)
  Enum.reduce(Enum.filter(server.servers, fn voterP -> voterP != server.selfP end), server, fn voterP, acc -> acc |> Vote.request_vote(voterP) end)
end

def request_vote(server, voterP) do
  vote_req_msg = { :VOTE_REQUEST, %{from: server.selfP, term: server.curr_term, last_log_term: Log.last_index(server), last_log_idx: Log.last_term(server)} }
  server = server
  |> Timer.restart_append_entries_timer(voterP)
  |> Debug.info("#{server.server_num} request vote from voter", 3)
  send(voterP, vote_req_msg)
  server
end

def handle_vote_request(server, %{from: req_id, term: req_term, last_log_term: last_log_term, last_log_idx: last_log_idx}) do
  server = server |> ServerLib.stepdown(req_term)

  server =
    if req_term == server.curr_term and
      (server.voted_for == req_id || server.voted_for == nil) and
      (last_log_term > Log.last_term(server) or
        (last_log_term == Log.last_term(server) and last_log_idx >= Log.last_index(server))) do
      server
      |> State.voted_for(req_id)
      |> Timer.restart_election_timer()
      |> Debug.info("vote requested from #{server.server_num}", 3)
    else
      server
    end

  send(req_id, { :VOTE_REPLY, %{from: server.selfP, term: server.curr_term, voted_for: server.voted_for} })

  server
end

def handle_vote_reply(server, %{from: rep_from, term: rep_term, voted_for: rep_voted_for}) do
  server = server |> ServerLib.stepdown(rep_term)

  if rep_term == server.curr_term and server.role == :CANDIDATE do
    server =
      if rep_voted_for == server.selfP do
        server
        |> State.add_to_voted_by(rep_from)
        |> Debug.info("Someone voted for #{server.server_num}", 3)
      else
        server
      end
    server = server |> Timer.cancel_append_entries_timer(rep_from)
    if State.vote_tally(server) >= server.majority do
      Debug.info(server, "#{server.server_num} elected at term #{server.curr_term}", 3)
      server = server |> ServerLib.become_leader()
      Enum.reduce(Enum.filter(server.servers, fn followerP -> followerP != server.selfP end), server, fn followerP, acc -> acc |> AppendEntries.send_append_entries(followerP) end)
    else
      server
    end
  else
    server
  end
end

end # Vote
