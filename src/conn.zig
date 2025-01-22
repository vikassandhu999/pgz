const std = @import("std");
const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

const Reader = @import("./reader.zig").Reader;
const Msg = @import("./reader.zig").Msg;
const Writer = @import("./writer.zig").Writer;
const StartupMessage = @import("./protocol/startup_message.zig").StartupMessage;
const ScramClient = @import("./auth_scram.zig").ScramClient;

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

        var startupmsg = StartupMessage.init(opts.user, opts.database);
        try startupmsg.write(&conn.writer);
        try conn.writer.flush();

        try conn.auth();

        _ = try conn.reader.read();

        return conn;
    }

    fn auth(
        self: *Self,
    ) anyerror!void {
        var msg = try self.reader.read();

        var reader = msg.reader();

        switch (try reader.readByte()) {
            'R' => {},
            'E' => return error.DBError,
            else => return error.InvalidType,
        }

        // skips 4 bytes for message length;
        _ = try reader.readInt32();

        const authreq = try reader.readInt32();

        switch (authreq) {
            10 => {
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
                return self.auth();
            },
            11 => {
                const data = try reader.readAllRemaining();
                try self.auth_sasl_continue(self.opts.password, data);
                return self.auth();
            },
            12 => {
                const res = try reader.readAllRemaining();
                try self.scramclient.?.handle_serverfinalmessage(res);
                return;
            },
            else => {
                std.debug.print("unimplemented authreq: {d}", .{authreq});
                return error.UnImplementedAuthReq;
            },
        }
    }

    fn auth_sasl_continue(self: *Self, password: []const u8, data: []const u8) anyerror!void {
        try self.scramclient.?.handle_serverfirstmessage(data);

        const clientfinalmessage = try self.scramclient.?.build_clientfinalmessage(password);

        try self.writer.writeMsgStart('p');
        try self.writer.write(clientfinalmessage);
        try self.writer.writeMsgEnd();
        try self.writer.flush();
    }

    fn auth_sasl(self: *Self, mechanism: []const u8) anyerror!void {
        self.scramclient = try ScramClient.init(mechanism, self.allocator);
        errdefer self.scramclient.?.deinit();

        {
            const clientfirstmessage = try self.scramclient.?.build_clientfirstmessage();

            try self.writer.writeMsgStart('p');
            try self.writer.writeString(mechanism);
            try self.writer.writeInt(i32, @intCast(clientfirstmessage.len));
            try self.writer.write(clientfirstmessage);
            try self.writer.writeMsgEnd();
            try self.writer.flush();
        }
    }

    pub fn close(self: *Self) void {
        self.stream.close();
        self.reader.deinit();
        self.writer.deinit();
        if (self.scramclient) |sc| {
            sc.deinit();
        }
        std.debug.print("Connection closed\n", .{});
    }
};
