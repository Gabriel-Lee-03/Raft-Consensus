
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2
# Gabriel Lee (gl721) and Hou Wang Wong (hww21)

defmodule AppendEntries do

def send_append_entries(server, followerP) do
  server = server |> Timer.restart_append_entries_timer(followerP)
  next_log_index = server.next_index[followerP]
  last_log_index = next_log_index - 1
  last_term = Log.term_at(server, last_log_index)
  entries = Log.get_entries_from(server, next_log_index)
  Debug.info(server, "Server #{server.server_num} sending append entries at index #{next_log_index} with #{map_size(entries)} entries, total #{map_size(server.log)}", 2)
  send(followerP, { :APPEND_ENTRIES_REQUEST, %{term: server.curr_term, leader_id: server.selfP, last_log_index: last_log_index, last_term: last_term, entries: entries, leader_commit: server.commit_index} } )
  server
end

def handle_append_entries_request(server, %{term: term, leader_id: leader_id, last_log_index: last_log_index, last_term: last_term, entries: entries, leader_commit: leader_commit}) do
  server = server |> ServerLib.stepdown(term)

  if term < server.curr_term do
    send(leader_id, { :APPEND_ENTRIES_REPLY, %{from: server.selfP, term: server.curr_term, success: false, index: nil} })
    server
  else
    server = server |> Timer.restart_election_timer()
    success = last_log_index == 0 or (last_log_index <= Log.last_index(server) and Log.term_at(server, last_log_index) == last_term)

    server =
      if success do
        server
        |> Debug.info("Appending entries at Server #{server.server_num} from index #{last_log_index}", 2)
        |> Log.delete_entries_from(last_log_index + 1)
        |> Log.merge_entries(entries)
        |> AppendEntries.update_follower_commit_index(leader_commit)
      else
        server
      end
    reply_index = if success do Log.last_index(server) else nil end
    send(leader_id, { :APPEND_ENTRIES_REPLY, %{from: server.selfP, term: server.curr_term, success: success, index: reply_index} })
    Debug.info(server, "Sent append entries reply from #{server.server_num} with term #{server.curr_term} with success #{success} and reply_index #{reply_index}", 3)
    server
  end
end

def handle_append_entries_reply(server, %{from: _, term: term, success: _, index: _}) when term > server.curr_term do
  server |> ServerLib.stepdown(term)
end

def handle_append_entries_reply(server, %{from: followerP, term: term, success: success, index: index}) when server.role == :LEADER and server.curr_term == term do
  Debug.info(server, "Server #{server.server_num} received reply with success #{success} at index #{index}", 2)
  server =
    if success do
      server
      |> State.next_index(followerP, index + 1)
      |> State.match_index(followerP, index)
    else
      server
      |> State.next_index(followerP, server.next_index[followerP] - 1)
    end
  server = server |> AppendEntries.update_leader_commit_index(index)
  if server.next_index[followerP] <= Log.last_index(server) do
    server |> AppendEntries.send_append_entries(followerP)
  else
    server
  end
end

def handle_append_entries_reply(server, %{from: _, term: _, success: _, index: _}) do server end

def update_leader_commit_index(server, index) do
  if Log.term_at(server, index) == server.curr_term do
    count = Enum.count(server.match_index, fn {_key, value} -> value >= index end)
    if count >= server.majority do
      server
      |> State.commit_index(index)
      |> Debug.info("Leader #{server.server_num} has updated commit index to #{index}", 2)
      |> ServerLib.send_all_database_request()
    else
      server
      |> Debug.info("Leader #{server.server_num} did not update commit index.", 3)
    end
  else
    server
  end
end

def update_follower_commit_index(server, leader_commit) do
  if leader_commit > server.commit_index do
    server
    |> State.commit_index(min(leader_commit, Log.last_index(server)))
    |> Debug.info("Follower #{server.server_num} has updated commit index to #{server.commit_index}", 3)
    |> ServerLib.send_all_database_request()
  else
    server
  end
end

end # AppendEntries
