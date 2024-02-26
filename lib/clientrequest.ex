
# distributed algorithms, n.dulay, 14 jan 2024
# coursework, raft consensus, v2
# Gabriel Lee (gl721) and Hou Wang Wong (hww21)

defmodule ClientRequest do

def handle_client_request(server, msg) when server.role == :LEADER do
  entry = %{term: server.curr_term, request: msg}
  server =
    server
    |> Monitor.send_msg({ :CLIENT_REQUEST, server.server_num })
    |> Log.append_entry(entry)
  Debug.info(server, "Log appended, size now #{map_size(server.log)}", 2)
  server
end

def handle_client_request(server, msg) do
  send(msg.clientP, { :CLIENT_REPLY, %{cid: msg.cid, reply: :NOT_LEADER, leaderP: server.leaderP}})
  server
end

end # ClientRequest
