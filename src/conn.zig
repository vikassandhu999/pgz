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

    scramclient: ?ScramClient = undefined,

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
        };
    }

    pub fn open(opts: Opts, allocator: Allocator) anyerror!Conn {
        var conn = try Conn.init(opts, allocator);

        var startupmsg = StartupMessage.init(opts.user, opts.database);
        try startupmsg.write(&conn.writer);
        try conn.writer.flush();

        try conn.auth();

        return conn;
    }

    fn auth(
        self: *Conn,
    ) anyerror!void {
        var msg = try self.reader.read();
        defer msg.deinit();

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
            0 => return,
            10 => {
                var selectedmechanism: ?[]const u8 = null;
                while (reader.readStringOptional()) |m| {
                    if (std.ascii.eqlIgnoreCase(m, "SCRAM-SHA-256-PLUS")) {
                        selectedmechanism = m;
                    } else if (std.ascii.eqlIgnoreCase(m, "SCRAM-SHA-256") and selectedmechanism == null) {
                        selectedmechanism = m;
                    }
                }
                return self.auth_sasl(selectedmechanism.?);
            },
            11 => {
                const res = try reader.readAllRemaining();
                try self.scramclient.?.handle_serverfirstmessage(res);
            },
            else => {
                std.debug.print("unimplemented authreq: {d}", .{authreq});
                return error.UnImplementedAuthReq;
            },
        }
    }

    fn auth_sasl(self: *Conn, mechanism: []const u8) anyerror!void {
        self.scramclient = try ScramClient.init(mechanism, self.allocator);
        errdefer self.scramclient.?.deinit();

        {
            const sc = self.scramclient.?;

            try self.writer.writeMsgStart('p');
            try self.writer.writeString(mechanism);
            try self.writer.writeInt(i32, @intCast(sc.clientfirstmessage.len));
            try self.writer.write(sc.clientfirstmessage);

            try self.writer.writeMsgEnd();
            try self.writer.flush();
        }

        try self.auth();
    }

    pub fn close(self: *Conn) void {
        self.stream.close();
        self.reader.deinit();
        self.writer.deinit();
        if (self.scramclient) |sc| {
            sc.deinit();
        }
        std.debug.print("Connection closed\n", .{});
    }
};
