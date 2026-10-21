const std = @import("std");
const accounts = @import("accounts.zig");
const zed = @import("zed.zig");
const proxy = @import("proxy.zig");
const providers = @import("providers.zig");
const socket = @import("socket.zig");

/// State for translating upstream streams into Responses SSE events.
const ResponsesState = struct {
    // Upstream (open_ai provider) emits native Responses events: pass through
    // verbatim and skip our own synthesis once a terminal event arrives.
    saw_completed: bool = false,
    // Anthropic-provider tool_use block being converted into a function_call item
    in_tool: bool = false,
    tool_id: []u8 = &.{},
    tool_name: []u8 = &.{},
    tool_args: std.ArrayListUnmanaged(u8) = .empty,
    // output_index for synthesized items (0 = the message item)
    next_index: usize = 1,
    // Completed output items (comma-separated JSON fragments) for the fallback
    // response.completed synthesis
    items: std.io.Writer.Allocating = undefined,
    // Names of tools declared as Responses "custom" tools in the request;
    // matching upstream function_call items are rewritten to custom_tool_call.
    custom_names: []const []const u8 = &.{},
    // item_id of an in-progress custom tool call (arguments deltas buffered,
    // unwrapped and emitted once at arguments.done)
    custom_item_id: []u8 = &.{},
    custom_args: std.ArrayListUnmanaged(u8) = .empty,
};

/// State for translating upstream tool calls into OpenAI chat tool_calls chunks
const ChatToolState = struct {
    active: bool = false,
    index: usize = 0,
    id: []u8 = &.{},
    name: []u8 = &.{},
    args: std.ArrayListUnmanaged(u8) = .empty,
};

/// Handle streaming proxy with account failover
pub fn handleStreamProxy(
    client_stream: std.net.Stream,
    body: []const u8,
    is_anthropic: bool,
    is_responses: bool,
    account_mgr: *accounts.AccountManager,
    allocator: std.mem.Allocator,
    custom_names: []const []const u8,
) void {
    if (account_mgr.list.items.len == 0) {
        socket.writeResponse(client_stream, 400, "{\"error\":\"no account configured\"}");
        return;
    }

    const total = account_mgr.list.items.len;
    var try_order: [64]usize = undefined;
    const count = @min(total, 64);
    try_order[0] = account_mgr.current;
    var idx: usize = 1;
    for (0..total) |i| {
        if (i != account_mgr.current and idx < count) {
            try_order[idx] = i;
            idx += 1;
        }
    }

    for (try_order[0..count]) |acc_idx| {
        const acc = &account_mgr.list.items[acc_idx];
        if (doStreamProxy(client_stream, acc, body, is_anthropic, is_responses, allocator, custom_names)) {
            if (acc_idx != account_mgr.current) {
                std.debug.print("[zed2api] stream failover: switched to '{s}'\n", .{acc.name});
                account_mgr.current = acc_idx;
            }
            return;
        } else {
            std.debug.print("[zed2api] stream: account '{s}' failed, trying next...\n", .{acc.name});
        }
    }

    socket.writeResponse(client_stream, 502, "{\"error\":{\"message\":\"All accounts failed\",\"type\":\"upstream_error\"}}");
}

fn doStreamProxy(client_stream: std.net.Stream, acc: *accounts.Account, body: []const u8, is_anthropic: bool, is_responses: bool, allocator: std.mem.Allocator, custom_names: []const []const u8) bool {
    const payload = providers.buildZedPayload(allocator, body, is_anthropic) catch |err| {
        std.debug.print("[stream] buildZedPayload failed: {}\n", .{err});
        return false;
    };
    defer allocator.free(payload);

    const jwt = zed.getToken(allocator, acc) catch |err| {
        std.debug.print("[stream] getToken failed: {}\n", .{err});
        return false;
    };
    const bearer = std.fmt.allocPrint(allocator, "Bearer {s}", .{jwt}) catch return false;
    defer allocator.free(bearer);

    const auth_header = std.fmt.allocPrint(allocator, "authorization: {s}", .{bearer}) catch return false;
    defer allocator.free(auth_header);

    proxy.init(allocator);

    // Write payload to temp file
    var tmp_name_buf: [64]u8 = undefined;
    const tmp_path = std.fmt.bufPrint(&tmp_name_buf, "zed2api_stream_{d}.json", .{std.time.milliTimestamp()}) catch "zed2api_stream_req.json";
    {
        const f = std.fs.cwd().createFile(tmp_path, .{}) catch return false;
        defer f.close();
        f.writeAll(payload) catch return false;
    }
    defer std.fs.cwd().deleteFile(tmp_path) catch {};

    const at_path = std.fmt.allocPrint(allocator, "@{s}", .{tmp_path}) catch return false;
    defer allocator.free(at_path);

    const proxy_url = if (proxy.getHost()) |host|
        (std.fmt.allocPrint(allocator, "http://{s}:{d}", .{ host, proxy.getPort() }) catch return false)
    else
        null;
    defer if (proxy_url) |p| allocator.free(p);

    var argv_buf: [20][]const u8 = undefined;
    var argc: usize = 0;
    argv_buf[argc] = "curl"; argc += 1;
    argv_buf[argc] = "-siN"; argc += 1;
    if (proxy_url) |p| { argv_buf[argc] = "-x"; argc += 1; argv_buf[argc] = p; argc += 1; }
    argv_buf[argc] = "-X"; argc += 1;
    argv_buf[argc] = "POST"; argc += 1;
    argv_buf[argc] = "https://cloud.zed.dev/completions"; argc += 1;
    argv_buf[argc] = "-H"; argc += 1;
    argv_buf[argc] = auth_header; argc += 1;
    argv_buf[argc] = "-H"; argc += 1;
    argv_buf[argc] = "content-type: application/json"; argc += 1;
    argv_buf[argc] = "-H"; argc += 1;
    argv_buf[argc] = "x-zed-version: 0.222.4+stable.147.b385025df963c9e8c3f74cc4dadb1c4b29b3c6f0"; argc += 1;
    argv_buf[argc] = "--data-binary"; argc += 1;
    argv_buf[argc] = at_path; argc += 1;
    argv_buf[argc] = "--max-time"; argc += 1;
    argv_buf[argc] = "300"; argc += 1;

    var child = std.process.Child.init(argv_buf[0..argc], allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;
    child.spawn() catch return false;

    const stdout = child.stdout orelse {
        _ = child.wait() catch {};
        return false;
    };

    var line_buf: [65536]u8 = undefined;
    var line_len: usize = 0;
    var block_index: usize = 0;
    var got_any_data = false;
    var headers_sent = false;
    var has_tool_use = false;
    var http_headers_done = false;
    var http_status: u16 = 0;

    const model = providers.extractModelFromBody(allocator, body) catch "claude-sonnet-4-5";
    // open_ai upstream natively speaks Responses events -> verbatim passthrough
    const upstream_responses = is_responses and std.mem.eql(u8, providers.getProvider(model), "open_ai");

    var resp = ResponsesState{ .custom_names = custom_names };
    resp.items = .init(allocator);
    defer {
        resp.items.deinit();
        if (resp.tool_id.len > 0) allocator.free(resp.tool_id);
        if (resp.tool_name.len > 0) allocator.free(resp.tool_name);
        resp.tool_args.deinit(allocator);
    }

    var cts = ChatToolState{};
    defer {
        if (cts.id.len > 0) allocator.free(cts.id);
        if (cts.name.len > 0) allocator.free(cts.name);
        cts.args.deinit(allocator);
    }

    var text_acc: std.ArrayListUnmanaged(u8) = .empty;
    defer text_acc.deinit(allocator);

    while (true) {
        var one: [1]u8 = undefined;
        const n = stdout.read(&one) catch break;
        if (n == 0) break;

        if (one[0] == '\n') {
            if (!http_headers_done) {
                // Parse HTTP response headers from curl -i
                const line = line_buf[0..line_len];
                // Trim trailing \r
                const trimmed = if (line.len > 0 and line[line.len - 1] == '\r') line[0 .. line.len - 1] else line;
                if (trimmed.len == 0) {
                    // Empty line = end of HTTP headers
                    http_headers_done = true;
                    if (http_status != 0 and http_status != 200) {
                        std.debug.print("[stream] upstream HTTP {d}\n", .{http_status});
                    }
                } else if (std.mem.startsWith(u8, trimmed, "HTTP/")) {
                    // Parse status code from "HTTP/1.1 200 OK" or "HTTP/2 200"
                    var parts = std.mem.splitScalar(u8, trimmed, ' ');
                    _ = parts.next(); // skip HTTP/x.x
                    if (parts.next()) |code_str| {
                        http_status = std.fmt.parseInt(u16, code_str, 10) catch 0;
                    }
                }
                line_len = 0;
                continue;
            }

            if (line_len > 0) {
                const line = line_buf[0..line_len];
                if (line[0] == '{') {
                    // Check if this is an error response (non-200 status)
                    if (http_status != 0 and http_status != 200) {
                        std.debug.print("[stream] upstream error HTTP {d}: {s}\n", .{ http_status, line[0..@min(line.len, 500)] });
                        line_len = 0;
                        continue;
                    }
                    if (!headers_sent) {
                        headers_sent = true;
                        const sse_header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: *\r\n\r\n";
                        socket.send(client_stream, sse_header) catch {
                            _ = child.wait() catch {};
                            return false;
                        };
                        if (is_anthropic) {
                            var msg_start_buf: [512]u8 = undefined;
                            const msg_start = std.fmt.bufPrint(&msg_start_buf, "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_zed\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"{s}\",\"content\":[],\"stop_reason\":null,\"usage\":{{\"input_tokens\":0,\"output_tokens\":0}}}}}}\n\n", .{model}) catch "";
                            socket.send(client_stream, msg_start) catch {};
                        } else if (is_responses and !upstream_responses) {
                            // open_ai upstream sends its own response.created; only
                            // synthesize for other providers
                            var rc_buf: [512]u8 = undefined;
                            const rc = std.fmt.bufPrint(&rc_buf, "event: response.created\ndata: {{\"type\":\"response.created\",\"response\":{{\"id\":\"resp_zed\",\"object\":\"response\",\"created_at\":{d},\"status\":\"in_progress\",\"model\":\"{s}\",\"output\":[],\"usage\":null}}}}\n\n", .{ std.time.timestamp(), model }) catch "";
                            socket.send(client_stream, rc) catch {};
                        }
                    }
                    got_any_data = true;
                    convertAndSendSSE(client_stream, line, &block_index, &has_tool_use, allocator, is_anthropic, is_responses, model, &text_acc, &resp, &cts) catch break;
                } else {
                    std.debug.print("[stream] non-JSON from upstream ({d} bytes): {s}\n", .{ line.len, line[0..@min(line.len, 500)] });
                }
            }
            line_len = 0;
        } else {
            if (line_len < line_buf.len) {
                line_buf[line_len] = one[0];
                line_len += 1;
            }
        }
    }

    if (headers_sent) {
        if (is_anthropic) {
            const stop_reason = if (has_tool_use) "tool_use" else "end_turn";
            var stop_buf: [256]u8 = undefined;
            const stop_msg = std.fmt.bufPrint(&stop_buf, "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{s}\"}},\"usage\":{{\"output_tokens\":1}}}}\n\n", .{stop_reason}) catch "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n";
            socket.send(client_stream, stop_msg) catch {};
            socket.send(client_stream, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n") catch {};
        } else if (is_responses and resp.saw_completed) {
            // open_ai upstream already delivered its terminal event verbatim;
            // nothing further to emit (must NOT fall into the chat [DONE] branch)
        } else if (is_responses) {
            // Fallback synthesis: upstream ended without a terminal Responses event
            var ebuf: std.io.Writer.Allocating = .init(allocator);
            defer ebuf.deinit();
            const ew = &ebuf.writer;
            ew.writeAll("event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_0\",\"output_index\":0,\"content_index\":0,\"text\":") catch return got_any_data;
            std.json.Stringify.encodeJsonString(text_acc.items, .{}, ew) catch return got_any_data;
            ew.writeAll("}\n\n") catch return got_any_data;
            ew.writeAll("event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_0\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":") catch return got_any_data;
            std.json.Stringify.encodeJsonString(text_acc.items, .{}, ew) catch return got_any_data;
            ew.writeAll("}]}}\n\n") catch return got_any_data;
            ew.print("event: response.completed\ndata: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"resp_zed\",\"object\":\"response\",\"created_at\":{d},\"status\":\"completed\",\"model\":\"{s}\",\"output\":[{{\"type\":\"message\",\"id\":\"msg_0\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{{\"type\":\"output_text\",\"text\":", .{ std.time.timestamp(), model }) catch return got_any_data;
            std.json.Stringify.encodeJsonString(text_acc.items, .{}, ew) catch return got_any_data;
            ew.writeAll("}}]}}") catch return got_any_data;
            if (resp.items.written().len > 0) {
                ew.writeAll(",") catch return got_any_data;
                ew.writeAll(resp.items.written()) catch return got_any_data;
            }
            ew.print("],\"usage\":{{\"input_tokens\":0,\"output_tokens\":{d},\"total_tokens\":{d}}}}}}}\n\n", .{ text_acc.items.len / 4, text_acc.items.len / 4 }) catch return got_any_data;
            socket.send(client_stream, ebuf.written()) catch {};
        } else {
            const fin_reason: []const u8 = if (has_tool_use) "tool_calls" else "stop";
            var fin_buf: [512]u8 = undefined;
            const fin = std.fmt.bufPrint(&fin_buf, "data: {{\"id\":\"chatcmpl-zed\",\"object\":\"chat.completion.chunk\",\"model\":\"{s}\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"{s}\"}}]}}\n\ndata: [DONE]\n\n", .{ model, fin_reason }) catch "data: {\"id\":\"chatcmpl-zed\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
            socket.send(client_stream, fin) catch {};
        }
    }

    const stderr_pipe = child.stderr;
    var stderr_buf: [2048]u8 = undefined;
    var stderr_len: usize = 0;
    if (stderr_pipe) |sp| {
        stderr_len = sp.read(&stderr_buf) catch 0;
    }
    const term = child.wait() catch {
        std.debug.print("[stream] done, {d} blocks, headers_sent={}, wait failed\n", .{ block_index, headers_sent });
        return got_any_data;
    };
    const exit_code: u32 = switch (term) {
        .Exited => |c| c,
        else => 999,
    };
    if (!got_any_data or exit_code != 0) {
        std.debug.print("[stream] done, {d} blocks, headers_sent={}, curl exit={d}, http={d}, stderr={s}\n", .{ block_index, headers_sent, exit_code, http_status, stderr_buf[0..stderr_len] });
        // If we got no data, print any remaining buffered content for debugging
        if (!got_any_data and line_len > 0) {
            std.debug.print("[stream] remaining buffer ({d} bytes): {s}\n", .{ line_len, line_buf[0..@min(line_len, 500)] });
        }
    } else {
        std.debug.print("[stream] done, {d} blocks\n", .{block_index});
    }
    return got_any_data;
}

/// Convert a single Zed streaming JSON line to Anthropic, OpenAI or Responses SSE events
fn convertAndSendSSE(client_stream: std.net.Stream, line: []const u8, block_index: *usize, has_tool_use: *bool, allocator: std.mem.Allocator, is_anthropic: bool, is_responses: bool, model: []const u8, text_acc: *std.ArrayListUnmanaged(u8), resp: *ResponsesState, cts: *ChatToolState) !void {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch return;
    defer parsed.deinit();

    const obj = if (parsed.value.object.get("event")) |event|
        (if (event == .object) event else parsed.value)
    else
        parsed.value;

    if (obj.object.get("type")) |et_val| {
        if (et_val == .string) {
            const event_type = et_val.string;

            // Native Responses events from the open_ai upstream: forward verbatim
            // as SSE (covers response.created/output_item.added/function_call
            // arguments deltas/output_item.done/response.completed)
            if (is_responses and std.mem.startsWith(u8, event_type, "response.")) {
                // Tools declared as Responses "custom" tools in the request are
                // rewritten from upstream function_call items to custom_tool_call.
                if (resp.custom_names.len > 0) {
                    if (try rewriteCustomToolEvent(client_stream, event_type, obj, resp, allocator)) return;
                }
                if (std.mem.eql(u8, event_type, "response.completed") or std.mem.eql(u8, event_type, "response.failed")) {
                    resp.saw_completed = true;
                }
                var buf: std.io.Writer.Allocating = .init(allocator);
                defer buf.deinit();
                const w = &buf.writer;
                try w.print("event: {s}\ndata: {s}\n\n", .{ event_type, line });
                try socket.send(client_stream, buf.written());
                return;
            }

            // Chat clients: open_ai upstream function_call events -> tool_calls chunks
            // (output_text.delta falls through to the text handler below)
            if (!is_anthropic and !is_responses and std.mem.startsWith(u8, event_type, "response.")) {
                if (std.mem.eql(u8, event_type, "response.output_item.added")) {
                    if (obj.object.get("item")) |item| {
                        if (item == .object) {
                            if (item.object.get("type")) |it| {
                                if (it == .string and std.mem.eql(u8, it.string, "function_call")) {
                                    has_tool_use.* = true;
                                    const id = jsonStr(item.object.get("call_id")) orelse (jsonStr(item.object.get("id")) orelse "call_zed_0");
                                    const name = jsonStr(item.object.get("name")) orelse "";
                                    try beginChatTool(client_stream, model, cts, id, name, allocator);
                                }
                            }
                        }
                    }
                    return;
                }
                if (std.mem.eql(u8, event_type, "response.function_call_arguments.delta")) {
                    if (obj.object.get("delta")) |d| {
                        if (d == .string) try appendChatToolArgs(client_stream, model, cts, d.string, allocator);
                    }
                    return;
                }
                if (std.mem.eql(u8, event_type, "response.output_item.done")) {
                    if (obj.object.get("item")) |item| {
                        if (item == .object) {
                            if (item.object.get("type")) |it| {
                                if (it == .string and std.mem.eql(u8, it.string, "function_call")) {
                                    endChatTool(cts, allocator);
                                }
                            }
                        }
                    }
                    return;
                }
                if (!std.mem.eql(u8, event_type, "response.output_text.delta")) {
                    // remaining response.* events carry nothing representable in chat
                    return;
                }
            }

            if (std.mem.eql(u8, event_type, "message_start")) {
                if (obj.object.get("message")) |msg| {
                    if (msg == .object) {
                        if (msg.object.get("model")) |m| {
                            if (m == .string) std.debug.print("[stream] zed returned model: {s}\n", .{m.string});
                        }
                    }
                }
                return;
            }

            if (std.mem.eql(u8, event_type, "content_block_start")) {
                const cb = obj.object.get("content_block") orelse return;
                if (cb != .object) return;
                const cb_type = switch (cb.object.get("type") orelse return) { .string => |s| s, else => return };
                if (std.mem.eql(u8, cb_type, "tool_use")) {
                    has_tool_use.* = true;
                    if (is_responses) {
                        // Anthropic-provider tool_use -> Responses function_call item
                        if (resp.tool_id.len > 0) allocator.free(resp.tool_id);
                        if (resp.tool_name.len > 0) allocator.free(resp.tool_name);
                        resp.in_tool = true;
                        resp.tool_args.clearRetainingCapacity();
                        resp.tool_id = allocJsonString(allocator, cb.object.get("id"), "call_zed_0") catch return;
                        resp.tool_name = allocJsonString(allocator, cb.object.get("name"), "") catch return;
                        var rbuf: std.io.Writer.Allocating = .init(allocator);
                        defer rbuf.deinit();
                        const rw = &rbuf.writer;
                        try rw.print("event: response.output_item.added\ndata: {{\"type\":\"response.output_item.added\",\"output_index\":{d},\"item\":{{\"type\":\"function_call\",\"id\":", .{resp.next_index});
                        try std.json.Stringify.encodeJsonString(resp.tool_id, .{}, rw);
                        try rw.writeAll(",\"arguments\":\"\",\"call_id\":");
                        try std.json.Stringify.encodeJsonString(resp.tool_id, .{}, rw);
                        try rw.writeAll(",\"name\":");
                        try std.json.Stringify.encodeJsonString(resp.tool_name, .{}, rw);
                        try rw.writeAll(",\"status\":\"in_progress\"}}\n\n");
                        try socket.send(client_stream, rbuf.written());
                        return;
                    }
                    if (!is_anthropic) {
                        // Chat client: tool_use block -> tool_calls chunk
                        const id = allocJsonString(allocator, cb.object.get("id"), "call_zed_0") catch return;
                        defer allocator.free(id);
                        const name = allocJsonString(allocator, cb.object.get("name"), "") catch return;
                        defer allocator.free(name);
                        try beginChatTool(client_stream, model, cts, id, name, allocator);
                        return;
                    }
                    var buf: std.io.Writer.Allocating = .init(allocator);
                    defer buf.deinit();
                    const w = &buf.writer;
                    // Pass through tool_use content_block_start with id and name
                    try w.print("event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":{d},\"content_block\":{{\"type\":\"tool_use\"", .{block_index.*});
                    if (cb.object.get("id")) |id| {
                        try w.writeAll(",\"id\":"); try std.json.Stringify.value(id, .{}, w);
                    }
                    if (cb.object.get("name")) |name| {
                        try w.writeAll(",\"name\":"); try std.json.Stringify.value(name, .{}, w);
                    }
                    try w.writeAll(",\"input\":{}}}\n\n");
                    try socket.send(client_stream, buf.written());
                    return;
                }
                if (!is_anthropic) return;
                var buf: std.io.Writer.Allocating = .init(allocator);
                defer buf.deinit();
                const w = &buf.writer;
                try w.print("event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":{d},\"content_block\":{{\"type\":\"{s}\"", .{ block_index.*, cb_type });
                if (std.mem.eql(u8, cb_type, "thinking")) try w.writeAll(",\"thinking\":\"\"") else try w.writeAll(",\"text\":\"\"");
                try w.writeAll("}}\n\n");
                try socket.send(client_stream, buf.written());
                return;
            }

            if (std.mem.eql(u8, event_type, "content_block_delta")) {
                const delta = obj.object.get("delta") orelse return;
                if (delta != .object) return;
                if (!is_anthropic) {
                    const dt = switch (delta.object.get("type") orelse return) { .string => |s| s, else => return };
                    if (std.mem.eql(u8, dt, "thinking_delta")) {
                        if (is_responses) return;
                        if (delta.object.get("thinking")) |t| {
                            if (t == .string) try emitOpenAIDelta(client_stream, t.string, block_index, model, true, allocator);
                        }
                        return;
                    }
                    if (std.mem.eql(u8, dt, "input_json_delta")) {
                        if (delta.object.get("partial_json")) |pj| {
                            if (pj == .string) {
                                if (is_responses and resp.in_tool) {
                                    resp.tool_args.appendSlice(allocator, pj.string) catch {};
                                    var dbuf: std.io.Writer.Allocating = .init(allocator);
                                    defer dbuf.deinit();
                                    const dw = &dbuf.writer;
                                    try dw.writeAll("event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":");
                                    try std.json.Stringify.encodeJsonString(resp.tool_id, .{}, dw);
                                    try dw.print(",\"output_index\":{d},\"delta\":", .{resp.next_index});
                                    try std.json.Stringify.encodeJsonString(pj.string, .{}, dw);
                                    try dw.writeAll("}\n\n");
                                    try socket.send(client_stream, dbuf.written());
                                } else if (!is_responses and !is_anthropic and cts.active) {
                                    try appendChatToolArgs(client_stream, model, cts, pj.string, allocator);
                                }
                            }
                        }
                        return;
                    }
                    if (delta.object.get("text")) |t| {
                        if (t == .string) {
                            if (is_responses) {
                                try emitResponsesDelta(client_stream, t.string, block_index, text_acc, allocator);
                            } else {
                                try emitOpenAIDelta(client_stream, t.string, block_index, model, false, allocator);
                            }
                        }
                    }
                    return;
                }
                var buf: std.io.Writer.Allocating = .init(allocator);
                defer buf.deinit();
                const w = &buf.writer;
                try w.print("event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":{d},\"delta\":", .{block_index.*});
                try std.json.Stringify.value(delta, .{}, w);
                try w.writeAll("}\n\n");
                try socket.send(client_stream, buf.written());
                return;
            }

            if (std.mem.eql(u8, event_type, "content_block_stop")) {
                if (is_anthropic) {
                    var buf: [256]u8 = undefined;
                    const msg = std.fmt.bufPrint(&buf, "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":{d}}}\n\n", .{block_index.*}) catch return;
                    try socket.send(client_stream, msg);
                    block_index.* += 1;
                    return;
                }
                if (is_responses and resp.in_tool) {
                    // Finalize the function_call item: arguments.done + output_item.done
                    var item_buf: std.io.Writer.Allocating = .init(allocator);
                    defer item_buf.deinit();
                    const itw = &item_buf.writer;
                    try itw.writeAll("{\"type\":\"function_call\",\"id\":");
                    try std.json.Stringify.encodeJsonString(resp.tool_id, .{}, itw);
                    try itw.writeAll(",\"arguments\":");
                    try std.json.Stringify.encodeJsonString(resp.tool_args.items, .{}, itw);
                    try itw.writeAll(",\"call_id\":");
                    try std.json.Stringify.encodeJsonString(resp.tool_id, .{}, itw);
                    try itw.writeAll(",\"name\":");
                    try std.json.Stringify.encodeJsonString(resp.tool_name, .{}, itw);
                    try itw.writeAll(",\"status\":\"completed\"}");

                    var fbuf: std.io.Writer.Allocating = .init(allocator);
                    defer fbuf.deinit();
                    const fw = &fbuf.writer;
                    try fw.writeAll("event: response.function_call_arguments.done\ndata: {\"type\":\"response.function_call_arguments.done\",\"item_id\":");
                    try std.json.Stringify.encodeJsonString(resp.tool_id, .{}, fw);
                    try fw.print(",\"output_index\":{d},\"arguments\":", .{resp.next_index});
                    try std.json.Stringify.encodeJsonString(resp.tool_args.items, .{}, fw);
                    try fw.writeAll("}\n\n");
                    try socket.send(client_stream, fbuf.written());

                    var sbuf: std.io.Writer.Allocating = .init(allocator);
                    defer sbuf.deinit();
                    const sw = &sbuf.writer;
                    try sw.print("event: response.output_item.done\ndata: {{\"type\":\"response.output_item.done\",\"output_index\":{d},\"item\":", .{resp.next_index});
                    try sw.writeAll(item_buf.written());
                    try sw.writeAll("}\n\n");
                    try socket.send(client_stream, sbuf.written());

                    if (resp.items.written().len > 0) resp.items.writer.writeAll(",") catch {};
                    resp.items.writer.writeAll(item_buf.written()) catch {};

                    resp.in_tool = false;
                    resp.next_index += 1;
                    allocator.free(resp.tool_id);
                    resp.tool_id = &.{};
                    allocator.free(resp.tool_name);
                    resp.tool_name = &.{};
                }
                if (!is_responses and !is_anthropic and cts.active) {
                    endChatTool(cts, allocator);
                }
                return;
            }

            if (std.mem.eql(u8, event_type, "ping")) {
                if (!is_anthropic) return;
                try socket.send(client_stream, "event: ping\ndata: {\"type\":\"ping\"}\n\n");
                return;
            }

            if (std.mem.eql(u8, event_type, "response.output_text.delta")) {
                if (obj.object.get("delta")) |d| {
                    if (d == .string and d.string.len > 0) {
                        if (is_anthropic) {
                            try emitTextDelta(client_stream, d.string, block_index, allocator);
                        } else if (is_responses) {
                            try emitResponsesDelta(client_stream, d.string, block_index, text_acc, allocator);
                        } else {
                            try emitOpenAIDelta(client_stream, d.string, block_index, model, false, allocator);
                        }
                    }
                }
                return;
            }
        }
    }

    // xAI (Grok)
    if (obj.object.get("choices")) |choices| {
        if (choices == .array and choices.array.items.len > 0) {
            const choice = choices.array.items[0];
            if (choice == .object) {
                if (choice.object.get("delta")) |delta| {
                    if (delta == .object) {
                        if (delta.object.get("content")) |c| {
                            if (c == .string and c.string.len > 0) {
                                if (is_anthropic) {
                                    try emitTextDelta(client_stream, c.string, block_index, allocator);
                                } else if (is_responses) {
                                    try emitResponsesDelta(client_stream, c.string, block_index, text_acc, allocator);
                                } else {
                                    try emitOpenAIDelta(client_stream, c.string, block_index, model, false, allocator);
                                }
                            }
                        }
                    }
                }
            }
        }
        return;
    }

    // Google (Gemini)
    if (obj.object.get("candidates")) |candidates| {
        if (candidates == .array and candidates.array.items.len > 0) {
            const cand = candidates.array.items[0];
            if (cand == .object) {
                if (cand.object.get("content")) |content| {
                    if (content == .object) {
                        if (content.object.get("parts")) |parts| {
                            if (parts == .array and parts.array.items.len > 0) {
                                const part = parts.array.items[0];
                                if (part == .object) {
                                    if (part.object.get("text")) |t| {
                                        if (t == .string and t.string.len > 0) {
                                            if (is_anthropic) {
                                                try emitTextDelta(client_stream, t.string, block_index, allocator);
                                            } else if (is_responses) {
                                                try emitResponsesDelta(client_stream, t.string, block_index, text_acc, allocator);
                                            } else {
                                                try emitOpenAIDelta(client_stream, t.string, block_index, model, false, allocator);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return;
    }
}

fn emitResponsesDelta(client_stream: std.net.Stream, text: []const u8, block_index: *usize, text_acc: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator) !void {
    if (text.len == 0) return;
    if (block_index.* == 0) {
        try socket.send(client_stream, "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_0\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n");
        block_index.* = 1;
    }
    text_acc.appendSlice(allocator, text) catch {};
    var buf: std.io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;
    try w.writeAll("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_0\",\"output_index\":0,\"content_index\":0,\"delta\":");
    try std.json.Stringify.encodeJsonString(text, .{}, w);
    try w.writeAll("}\n\n");
    try socket.send(client_stream, buf.written());
}

fn emitOpenAIDelta(client_stream: std.net.Stream, text: []const u8, block_index: *usize, model: []const u8, is_reasoning: bool, allocator: std.mem.Allocator) !void {
    if (text.len == 0) return;
    var buf: std.io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;
    try w.print("data: {{\"id\":\"chatcmpl-zed\",\"object\":\"chat.completion.chunk\",\"model\":\"{s}\",\"choices\":[{{\"index\":0,\"delta\":{{", .{model});
    if (block_index.* == 0) {
        try w.writeAll("\"role\":\"assistant\",");
        block_index.* = 1;
    }
    if (is_reasoning) try w.writeAll("\"reasoning_content\":") else try w.writeAll("\"content\":");
    try std.json.Stringify.encodeJsonString(text, .{}, w);
    try w.writeAll("}}]}\n\n");
    try socket.send(client_stream, buf.written());
}

fn emitTextDelta(client_stream: std.net.Stream, text: []const u8, block_index: *usize, allocator: std.mem.Allocator) !void {
    if (block_index.* == 0) {
        try socket.send(client_stream, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n");
        block_index.* = 1;
    }
    var buf: std.io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;
    try w.writeAll("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":");
    try std.json.Stringify.encodeJsonString(text, .{}, w);
    try w.writeAll("}}\n\n");
    try socket.send(client_stream, buf.written());
}

/// Duplicate a string field out of a parsed JSON value (lifetime: parsed dies at
/// the end of convertAndSendSSE, so anything kept must be owned)
fn allocJsonString(allocator: std.mem.Allocator, v: ?std.json.Value, default: []const u8) ![]u8 {
    if (v) |val| {
        if (val == .string) return allocator.dupe(u8, val.string);
    }
    return allocator.dupe(u8, default);
}

/// Borrow a string from a parsed JSON value (valid only while parsed is alive)
fn jsonStr(v: ?std.json.Value) ?[]const u8 {
    if (v) |val| {
        if (val == .string) return val.string;
    }
    return null;
}

/// Whether name is one of the tools declared as Responses "custom" tools
fn toolNameIsCustom(resp: *ResponsesState, name: []const u8) bool {
    for (resp.custom_names) |n| {
        if (std.mem.eql(u8, n, name)) return true;
    }
    return false;
}

/// The model was asked to wrap freeform custom-tool input as {"input": "..."}.
/// Unwrap it back to the raw text; on any deviation, return the text unchanged.
fn unwrapCustomArgs(allocator: std.mem.Allocator, args_text: []const u8) []const u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, args_text, .{}) catch return args_text;
    defer parsed.deinit();
    if (parsed.value != .object) return args_text;
    var it = parsed.value.object.iterator();
    const first = it.next() orelse return args_text;
    if (it.next() != null) return args_text; // more than one property: not our wrapper
    if (first.value_ptr.* != .string) return args_text;
    return allocator.dupe(u8, first.value_ptr.string) catch args_text;
}

/// Write a JSON value, rewriting any function_call item whose name belongs to a
/// declared custom tool into a custom_tool_call item (arguments -> raw input).
fn writeWithCustomTools(w: *std.io.Writer, v: std.json.Value, resp: *ResponsesState, allocator: std.mem.Allocator) anyerror!void {
    switch (v) {
        .object => |o| {
            var is_custom_fc = false;
            if (o.get("type")) |t| {
                if (t == .string and std.mem.eql(u8, t.string, "function_call")) {
                    if (jsonStr(o.get("name"))) |n| is_custom_fc = toolNameIsCustom(resp, n);
                }
            }
            if (is_custom_fc) {
                const raw_args = jsonStr(o.get("arguments")) orelse "";
                const input = unwrapCustomArgs(allocator, raw_args);
                defer if (input.ptr != raw_args.ptr) allocator.free(input);
                try w.writeAll("{\"type\":\"custom_tool_call\"");
                if (o.get("id")) |idv| {
                    try w.writeAll(",\"id\":");
                    try std.json.Stringify.value(idv, .{}, w);
                }
                if (o.get("call_id")) |cid| {
                    try w.writeAll(",\"call_id\":");
                    try std.json.Stringify.value(cid, .{}, w);
                } else if (o.get("id")) |idv| {
                    try w.writeAll(",\"call_id\":");
                    try std.json.Stringify.value(idv, .{}, w);
                }
                try w.writeAll(",\"name\":");
                try std.json.Stringify.value(o.get("name").?, .{}, w);
                if (o.get("status")) |st| {
                    try w.writeAll(",\"status\":");
                    try std.json.Stringify.value(st, .{}, w);
                }
                try w.writeAll(",\"input\":");
                try std.json.Stringify.encodeJsonString(input, .{}, w);
                try w.writeAll("}");
                return;
            }
            try w.writeAll("{");
            var first = true;
            var it = o.iterator();
            while (it.next()) |entry| {
                if (first) first = false else try w.writeAll(",");
                try std.json.Stringify.encodeJsonString(entry.key_ptr.*, .{}, w);
                try w.writeAll(":");
                try writeWithCustomTools(w, entry.value_ptr.*, resp, allocator);
            }
            try w.writeAll("}");
        },
        .array => |a| {
            try w.writeAll("[");
            for (a.items, 0..) |item, i| {
                if (i > 0) try w.writeAll(",");
                try writeWithCustomTools(w, item, resp, allocator);
            }
            try w.writeAll("]");
        },
        else => try std.json.Stringify.value(v, .{}, w),
    }
}

/// Intercept native upstream Responses events that reference a declared custom
/// tool and rewrite them into custom_tool_call events. Returns true when the
/// event was handled (rewritten or swallowed) and must not be forwarded verbatim.
fn rewriteCustomToolEvent(client_stream: std.net.Stream, event_type: []const u8, obj: std.json.Value, resp: *ResponsesState, allocator: std.mem.Allocator) !bool {
    if (std.mem.eql(u8, event_type, "response.output_item.added")) {
        const item = obj.object.get("item") orelse return false;
        if (item != .object) return false;
        const it = item.object.get("type") orelse return false;
        if (it != .string or !std.mem.eql(u8, it.string, "function_call")) return false;
        const name = jsonStr(item.object.get("name")) orelse return false;
        if (!toolNameIsCustom(resp, name)) return false;
        if (resp.custom_item_id.len > 0) allocator.free(resp.custom_item_id);
        resp.custom_item_id = allocator.dupe(u8, jsonStr(item.object.get("id")) orelse "") catch "";
        resp.custom_args.clearRetainingCapacity();
        var buf: std.io.Writer.Allocating = .init(allocator);
        defer buf.deinit();
        const w = &buf.writer;
        try w.writeAll("event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\"");
        if (obj.object.get("output_index")) |oi| {
            try w.writeAll(",\"output_index\":");
            try std.json.Stringify.value(oi, .{}, w);
        }
        try w.writeAll(",\"item\":{\"type\":\"custom_tool_call\",\"id\":");
        if (item.object.get("id")) |idv| try std.json.Stringify.value(idv, .{}, w) else try w.writeAll("\"ctc_0\"");
        try w.writeAll(",\"call_id\":");
        if (item.object.get("call_id")) |cid| try std.json.Stringify.value(cid, .{}, w) else if (item.object.get("id")) |idv| try std.json.Stringify.value(idv, .{}, w) else try w.writeAll("\"ctc_0\"");
        try w.writeAll(",\"name\":");
        try std.json.Stringify.value(item.object.get("name").?, .{}, w);
        try w.writeAll(",\"status\":\"in_progress\",\"input\":\"\"}}\n\n");
        try socket.send(client_stream, buf.written());
        return true;
    }

    if (std.mem.eql(u8, event_type, "response.function_call_arguments.delta")) {
        const iid = jsonStr(obj.object.get("item_id")) orelse return false;
        if (resp.custom_item_id.len == 0 or !std.mem.eql(u8, resp.custom_item_id, iid)) return false;
        // Buffer the wrapper JSON; unwrapped and emitted once at arguments.done
        if (obj.object.get("delta")) |d| {
            if (d == .string) resp.custom_args.appendSlice(allocator, d.string) catch {};
        }
        return true;
    }

    if (std.mem.eql(u8, event_type, "response.function_call_arguments.done")) {
        const iid = jsonStr(obj.object.get("item_id")) orelse return false;
        if (resp.custom_item_id.len == 0 or !std.mem.eql(u8, resp.custom_item_id, iid)) return false;
        const full_args = jsonStr(obj.object.get("arguments")) orelse resp.custom_args.items;
        const input = unwrapCustomArgs(allocator, full_args);
        defer if (input.ptr != full_args.ptr and input.ptr != resp.custom_args.items.ptr) allocator.free(input);
        var buf: std.io.Writer.Allocating = .init(allocator);
        defer buf.deinit();
        const w = &buf.writer;
        try w.writeAll("event: response.custom_tool_call_input.done\ndata: {\"type\":\"response.custom_tool_call_input.done\",\"item_id\":");
        try std.json.Stringify.encodeJsonString(iid, .{}, w);
        if (obj.object.get("output_index")) |oi| {
            try w.writeAll(",\"output_index\":");
            try std.json.Stringify.value(oi, .{}, w);
        }
        try w.writeAll(",\"input\":");
        try std.json.Stringify.encodeJsonString(input, .{}, w);
        try w.writeAll("}\n\n");
        try socket.send(client_stream, buf.written());
        resp.custom_args.clearRetainingCapacity();
        return true;
    }

    if (std.mem.eql(u8, event_type, "response.output_item.done")) {
        const item = obj.object.get("item") orelse return false;
        if (item != .object) return false;
        const it = item.object.get("type") orelse return false;
        if (it != .string or !std.mem.eql(u8, it.string, "function_call")) return false;
        const name = jsonStr(item.object.get("name")) orelse return false;
        if (!toolNameIsCustom(resp, name)) return false;
        var buf: std.io.Writer.Allocating = .init(allocator);
        defer buf.deinit();
        const w = &buf.writer;
        try w.writeAll("event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\"");
        if (obj.object.get("output_index")) |oi| {
            try w.writeAll(",\"output_index\":");
            try std.json.Stringify.value(oi, .{}, w);
        }
        try w.writeAll(",\"item\":");
        try writeWithCustomTools(w, item, resp, allocator);
        try w.writeAll("}\n\n");
        try socket.send(client_stream, buf.written());
        if (resp.custom_item_id.len > 0) {
            allocator.free(resp.custom_item_id);
            resp.custom_item_id = &.{};
        }
        resp.custom_args.clearRetainingCapacity();
        return true;
    }

    if (std.mem.eql(u8, event_type, "response.completed") or std.mem.eql(u8, event_type, "response.failed")) {
        // The terminal response object embeds all output items; rewrite any
        // custom-tool function_call items inside it as well.
        resp.saw_completed = true;
        var buf: std.io.Writer.Allocating = .init(allocator);
        defer buf.deinit();
        const w = &buf.writer;
        try w.print("event: {s}\ndata: ", .{event_type});
        try writeWithCustomTools(w, obj, resp, allocator);
        try w.writeAll("\n\n");
        try socket.send(client_stream, buf.written());
        return true;
    }

    return false;
}

/// Chat clients: emit the first tool_calls chunk for a new call
fn beginChatTool(client_stream: std.net.Stream, model: []const u8, cts: *ChatToolState, id: []const u8, name: []const u8, allocator: std.mem.Allocator) !void {
    if (cts.id.len > 0) allocator.free(cts.id);
    if (cts.name.len > 0) allocator.free(cts.name);
    cts.id = allocator.dupe(u8, id) catch return;
    cts.name = allocator.dupe(u8, name) catch return;
    cts.args.clearRetainingCapacity();
    cts.active = true;
    var buf: std.io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;
    try w.print("data: {{\"id\":\"chatcmpl-zed\",\"object\":\"chat.completion.chunk\",\"model\":\"{s}\",\"choices\":[{{\"index\":0,\"delta\":{{\"tool_calls\":[{{\"index\":{d},\"id\":", .{ model, cts.index });
    try std.json.Stringify.encodeJsonString(cts.id, .{}, w);
    try w.writeAll(",\"type\":\"function\",\"function\":{\"name\":");
    try std.json.Stringify.encodeJsonString(cts.name, .{}, w);
    try w.writeAll(",\"arguments\":\"\"}}]}}]}\n\n");
    try socket.send(client_stream, buf.written());
}

/// Chat clients: stream tool arguments deltas
fn appendChatToolArgs(client_stream: std.net.Stream, model: []const u8, cts: *ChatToolState, delta: []const u8, allocator: std.mem.Allocator) !void {
    if (!cts.active) return;
    cts.args.appendSlice(allocator, delta) catch {};
    var buf: std.io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;
    try w.print("data: {{\"id\":\"chatcmpl-zed\",\"object\":\"chat.completion.chunk\",\"model\":\"{s}\",\"choices\":[{{\"index\":0,\"delta\":{{\"tool_calls\":[{{\"index\":{d},\"function\":{{\"arguments\":", .{ model, cts.index });
    try std.json.Stringify.encodeJsonString(delta, .{}, w);
    try w.writeAll("}}]}}]}\n\n");
    try socket.send(client_stream, buf.written());
}

/// Chat clients: close the current tool call
fn endChatTool(cts: *ChatToolState, allocator: std.mem.Allocator) void {
    if (!cts.active) return;
    cts.active = false;
    cts.index += 1;
    if (cts.id.len > 0) {
        allocator.free(cts.id);
        cts.id = &.{};
    }
    if (cts.name.len > 0) {
        allocator.free(cts.name);
        cts.name = &.{};
    }
}
