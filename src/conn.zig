const std = @import("std");
const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

const Reader = @import("./reader.zig").Reader;
const Msg = @import("./reader.zig").Msg;
const Writer = @import("./writer.zig").Writer;
const StartupMessage = @import("./protocol/startup_message.zig").StartupMessage;
const ScramClient = @import("./auth_scram.zig").ScramClient;
const Authentication = @import("./auth_scram.zig").Authentication;
const Rows = @import("./rows.zig").Rows;

pub const Conn = struct {
    stream: Stream,
    allocator: Allocator,
    reader: Reader,
    writer: Writer,
    opts: Opts,
    scramclient: ?ScramClient = undefined,

    const Self = @This();

    pub const Opts = struct {
        host: [4]u8,
        port: u16,
        database: []const u8,
        user: []const u8,
        password: []const u8,
        params: ?std.StringHashMap([]const u8) = null,
    };

    pub fn init(opts: Opts, allocator: Allocator) !Conn {
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

    pub fn open(opts: Opts, allocator: Allocator) anyerror!Conn {
        var conn = try Conn.init(opts, allocator);

        try conn.send_startupmessage();

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

    pub fn query(self: *Self, sql: []const u8) !Rows {
        try self.writer.writeMsgStart('Q');
        try self.writer.writeString(sql);
        try self.writer.writeMsgEnd();
        try self.writer.flush();
        return Rows.init(&self.reader, self.allocator);
    }

    fn ready_forquery(self: *Self) !void {
        while (true) {
            var msg = try self.reader.read();
            switch (msg.msgtype()) {
                'Z' => return,
                'E' => return error.DatabaseError,
                else => return error.UnexpectedMessage,
            }
        }
    }

    fn authenticate(
        self: *Self,
    ) anyerror!void {
        while (true) {
            const msg = try self.reader.read();
            var reader = msg.reader();

            switch (try reader.readByte()) {
                'R' => {},
                'E' => return error.DBError,
                else => return,
            }
            // skips 4 bytes for message length;
            _ = try reader.readInt32();

            const req = try reader.readInt32();

            switch (req) {
                @intFromEnum(Authentication.Ok) => {
                    return;
                },
                @intFromEnum(Authentication.SASL) => {
                    var selectedmechanism: ?[]const u8 = null;
                    while (reader.readStringOptional()) |m| {
                        // we are not going to support channel-binding for now.
                        if (std.ascii.eqlIgnoreCase(m, "SCRAM-SHA-256")) {
                            selectedmechanism = m;
                        }
                    }
                    if (selectedmechanism == null) {
                        return error.NoSupportedScramMechanismFound;
                    }
                    try self.auth_sasl(selectedmechanism.?);
                },
                @intFromEnum(Authentication.SASLContinue) => {
                    const data = try reader.readAllRemaining();
                    try self.auth_saslcontinue(self.opts.password, data);
                },
                @intFromEnum(Authentication.SASLFinal) => {
                    const data = try reader.readAllRemaining();
                    try self.auth_saslfinal(data);
                },
                else => {
                    std.debug.print("unimplemented authreq: {d}", .{req});
                    return error.UnImplementedAuthReq;
                },
            }
        }
    }

    fn send_startupmessage(self: *Self) !void {
        try self.writer.writeMsgStart(0);
        try self.writer.writeInt(u32, 196608);
        try self.writer.write("user");
        try self.writer.writeByte(0);
        try self.writer.write(self.opts.user);
        try self.writer.writeByte(0);
        try self.writer.write("database");
        try self.writer.writeByte(0);
        try self.writer.write(self.opts.database);
        try self.writer.writeByte(0);
        try self.writer.writeByte(0);
        try self.writer.writeMsgEnd();
        try self.writer.flush();
    }

    fn auth_sasl(self: *Self, mechanism: []const u8) anyerror!void {
        self.scramclient = try ScramClient.init(mechanism, self.allocator);
        errdefer self.scramclient.?.deinit();

        const clientfirst = try self.scramclient.?.create_clientfirstmessage();

        try self.writer.writeMsgStart('p');
        try self.writer.writeString(mechanism);
        try self.writer.writeInt(i32, @intCast(clientfirst.len));
        try self.writer.write(clientfirst);
        try self.writer.writeMsgEnd();
        try self.writer.flush();
    }

    fn auth_saslcontinue(self: *Self, password: []const u8, data: []const u8) anyerror!void {
        try self.scramclient.?.handle_serverfirstmessage(data);

        const clientfinal = try self.scramclient.?.create_clientfinalmessage(password);

        try self.writer.writeMsgStart('p');
        try self.writer.write(clientfinal);
        try self.writer.writeMsgEnd();
        try self.writer.flush();
    }

    fn auth_saslfinal(self: *Self, data: []const u8) anyerror!void {
        try self.scramclient.?.verify_severfinalmessage(data);
    }

    pub fn close(self: *Self) void {
        self.stream.close();
        self.reader.deinit();
        self.writer.deinit();
        if (self.scramclient) |*sc| {
            sc.deinit();
        }
        std.debug.print("Connection closed\n", .{});
    }
};
