const std = @import("std");
const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

const proto3 = @import("./proto3/proto3.zig");

const PostgresError = proto3.PostgresError;
const StartupMessage = proto3.StartupMessage;
const SASLInitialResponse = proto3.SASLInitialResponse;
const SASLResponse = proto3.SASLResponse;
const Query = proto3.Query;
const ErrorResponse = proto3.ErrorResponse;
const ErrorResponseRaw = proto3.ErrorResponseRaw;

const Reader = @import("./reader.zig").Reader;
const Writer = @import("./writer.zig").Writer;
const ScramClient = @import("./auth_scram.zig").ScramClient;
const Rows = @import("./rows.zig").Rows;

pub const Conn = struct {
    stream: Stream,
    allocator: Allocator,
    reader: Reader,
    writer: Writer,
    opts: Opts,
    scramclient: ?ScramClient = null,
    connectionerror: ?ErrorResponseRaw = null,

    const Self = @This();

    pub const Opts = struct {
        host: [4]u8,
        port: u16,
        database: []const u8,
        user: []const u8,
        password: []const u8,
        params: ?std.StringHashMap([]const u8) = null,
    };

    fn init(opts: Opts, allocator: Allocator) !Conn {
        const address = std.net.Address.initIp4(opts.host, opts.port);

        const stream = try std.net.tcpConnectToAddress(address);
        errdefer stream.close();

        var reader = try Reader.init(stream, allocator);
        errdefer reader.deinit();

        var writer = try Writer.init(stream, allocator);
        errdefer writer.deinit();

        return .{
            .stream = stream,
            .allocator = allocator,
            .reader = reader,
            .writer = writer,
            .opts = opts,
        };
    }

    pub fn connect(opts: Opts, allocator: Allocator) anyerror!Conn {
        var conn = try Conn.init(opts, allocator);

        try conn.sendStartupMessage();

        try conn.authenticate();

        while (true) {
            var msg = try conn.reader.read();
            switch (msg.msgtype()) {
                'Z' => return conn,
                'K' => {}, // Unimplemented
                'S' => {}, // Unimplemented
                else => {
                    std.debug.print("unknown message type: {c}", .{msg.msgtype()});
                    return error.UnexpectedMessage;
                },
            }
        }

        return conn;
    }

    pub fn disconnect(self: *Self) void {
        self.stream.close();
        self.reader.deinit();
        self.writer.deinit();
        if (self.scramclient) |*sc| {
            sc.deinit();
        }
        std.debug.print("Connection closed\n", .{});
    }

    fn authenticate(
        self: *Self,
    ) anyerror!void {
        while (true) {
            const msg = try self.reader.read();

            switch (msg.msgtype()) {
                'R' => {},
                'E' => {
                    self.connectionerror = proto3.ErrorResponseRaw{ .msg = msg };
                    return PostgresError.ConnectionError;
                },
                else => return PostgresError.ExpectedRequest,
            }

            const autentication = try proto3.Authentication.decode(msg);

            switch (autentication) {
                .Ok => {
                    return;
                },
                .SASL => |req| {
                    try self.authSasl(req.mechanism);
                },
                .SASLContinue => |req| {
                    try self.authSaslContinue(self.opts.password, req.data);
                },
                .SASLFinal => |req| {
                    try self.authSaslFinal(req.data);
                },
                .Unknown => {
                    return PostgresError.UnimplementedAuthRequest;
                },
            }
        }
    }

    pub fn query(self: *Self, sql: []const u8) !Rows {
        const querymsg = Query.init(sql);
        try self.writer.writeToStream(querymsg);

        return Rows.init(&self.reader, self.allocator);
    }

    fn sendStartupMessage(self: *Self) !void {
        const msg = StartupMessage.init(self.opts.user, self.opts.database);
        try self.writer.writeToStream(msg);
    }

    fn authSasl(self: *Self, mechanism: []const u8) anyerror!void {
        self.scramclient = try ScramClient.init(mechanism, self.allocator);
        errdefer self.scramclient.?.deinit();

        const clientfirst = try self.scramclient.?.createClientFirstMessage();

        const initialresponse = SASLInitialResponse.init(mechanism, clientfirst);

        try self.writer.writeToStream(initialresponse);
    }

    fn authSaslContinue(self: *Self, password: []const u8, data: []const u8) anyerror!void {
        try self.scramclient.?.handleServerFirstMessage(data);

        const clientfinal = try self.scramclient.?.createClientFinalMessage(password);

        const response = SASLResponse.init(clientfinal);

        try self.writer.writeToStream(response);
    }

    fn authSaslFinal(self: *Self, data: []const u8) anyerror!void {
        try self.scramclient.?.verifySeverFinalMessage(data);
    }

    pub fn errorAlloc(self: *Self, allocator: Allocator) !?ErrorResponse {
        if (self.connectionerror == null) return null;
        return ErrorResponse.decodeAlloc(self.connectionerror.msg, allocator);
    }
};
