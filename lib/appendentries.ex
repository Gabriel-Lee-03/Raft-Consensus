
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2

defmodule AppendEntries do

def send_append_entries(server, followerP) do
  server = server |> restart_append_entry_timer(followerP)
  next_log_index = server.next_index.followerP
  last_log_index = next_log_index - 1
  last_term = Log.term_at(server, last_log_index)
  entries = Log.get_entries_from(next_log_index)
  send({ :APPEND_ENTRIES_REQUEST, %{term: server.curr_term, leader_id: server.selfP, last_log_index: last_log_index, last_term: last_term, entries: entries, leader_commit: server.commit_index} } )
  server

end

def handle_append_entries_request(server, %{term: term, leader_id: leader_id, last_log_index: last_log_index, last_term: last_term, entries: entries, leader_commit: leader_commit}) do
  server =
    if term > server.curr_term do
      server |> Vote.stepdown(term)
    else
      server
    end

  if term < server.curr_term do
    send({ :APPEND_ENTRIES_REPLY, %{from: server.selfP, term: server.curr_term, success: False} })
    server
  else
    index = 0
    success = last_log_index == 0 or (last_log_index <= Log.last_index(server) and Log.term_at(server, last_log_index) == last_term)

    if success do
      server
      |> Log.delete_from(last_log_index + 1)
      |> Log.merge(entries)
      |> AppendEntries.update_commit_index(leader_commit)
    else
      server
    end
  end
end

# .. omitted

def update_commit_index(server, leader_commit) do
  if leader_commit > server.commit_index do
    server |> State.commit_index(min(leader_commit, Log.last_index(server)))
  else
    server
  end

end # AppendEntries
