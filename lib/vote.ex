
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule Vote do

def hold_election(server, term) do
  # Debug.info(server, "Holding election")
  # IO.puts "Holding election for #{server.server_num} at term #{server.curr_term} with timeout term #{term}"
  # Debug.assert(server, term <= server.curr_term, "Election timeout term #{term} greater than server curr_term #{server.curr_term}!")

  if server.role == :CANDIDATE || server.role == :FOLLOWER do
    server
    |> Timer.restart_election_timer()
    |> State.inc_term()
    |> State.role(:CANDIDATE)
    |> State.voted_for(server.selfP)
    |> State.new_voted_by()
    |> State.add_to_voted_by(server.selfP)
    |> Vote.request_vote_all()
    |> Debug.info("#{server.server_num} has held election")
  else
    server
  end
end

def request_vote_all(server) do
  updated = server
  |> Timer.cancel_all_append_entries_timers()
  |> Debug.info("#{server.server_num} in request_vote_all")
  Enum.reduce(Enum.filter(updated.servers, fn voterP -> voterP != updated.selfP end), updated, fn voterP, acc -> acc |> Vote.request_vote(voterP) end)
end

def request_vote(server, voterP) do
  vote_req_msg = { :VOTE_REQUEST, %{from: server.selfP, term: server.curr_term} }
  updated = server
  |> Timer.restart_append_entries_timer(voterP)
  |> Debug.info("#{server.server_num} request vote from voter")
  send(voterP, vote_req_msg)
  updated
end

def handle_vote_request(server, %{from: req_id, term: req_term}) do
  updated =
    if req_term > server.curr_term do
      server |> Vote.stepdown(req_term)
    else
      server
    end

  updated =
    if req_term == updated.curr_term and (updated.voted_for == req_id || updated.voted_for == nil) do
      updated
      |> State.voted_for(req_id)
      |> Timer.restart_election_timer()
      |> Debug.info("vote requested from #{server.server_num}")
    else
      updated
    end

  send(req_id, { :VOTE_REPLY, %{from: updated.selfP, term: updated.curr_term, voted_for: updated.voted_for} })

  updated
end

def handle_vote_reply(server, %{from: rep_from, term: rep_term, voted_for: rep_voted_for}) do
  updated =
    if rep_term > server.curr_term do
      server |> Vote.stepdown(rep_term)
    else
      server
    end

  if rep_term == updated.curr_term and updated.role == :CANDIDATE do
    updated =
      if rep_voted_for == updated.selfP do
        updated
        |> State.add_to_voted_by(rep_from)
        |> Debug.info("Someone voted for #{server.server_num}")
      else
        updated
      end
    updated = updated |> Timer.cancel_append_entries_timer(rep_from)
    if State.vote_tally(updated) >= updated.majority do
      IO.puts "#{updated.server_num} elected at term #{updated.curr_term}"
      updated =
        updated
        |> State.role(:LEADER)
        |> State.leaderP(updated.selfP)
        |> Debug.info("#{updated.server_num} elected")
      Enum.reduce(Enum.filter(updated.servers, fn followerP -> followerP != updated.selfP end), updated, fn followerP, acc -> acc |> AppendEntries.send_append_entries(followerP) end)
    else
      updated
    end
  else
    updated
  end
end

def stepdown(server, term) do
  server
  |> Debug.info("stepping down from #{term}")
  |> State.curr_term(term)
  |> State.role(:FOLLOWER)
  |> State.voted_for(nil)
  |> State.leaderP(nil)
  |> Timer.restart_election_timer()
  |> Debug.info("stepped down, curr_term is ##{server.curr_term}")
end

end # Vote
