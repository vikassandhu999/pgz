const std = @import("std");
const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

const Reader = @import("./reader.zig").Reader;
const Msg = @import("./reader.zig").Msg;
const Writer = @import("./writer.zig").Writer;
const StartupMessage = @import("./protocol/startup_message.zig").StartupMessage;

pub const Conn = struct {
    stream: Stream,
    allocator: Allocator,
    reader: Reader,
    writer: Writer,

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

    pub fn open(opts: Opts, allocator: Allocator) !Conn {
        var conn = try Conn.init(opts, allocator);
        try conn.auth(opts);
        return conn;
    }

    fn auth(
        self: *Conn,
        opts: Opts,
    ) !void {
        var startupmsg = StartupMessage.init(opts.user, opts.database);
        try startupmsg.write(&self.writer);
        try self.writer.flush();

        var msg = try self.reader.read();
        defer msg.deinit();

        switch (msg.msgtype()) {
            'R' => {},
            'E' => return error.DBError,
            else => return error.InvalidType,
        }
    }

    fn auth_sasl(self: *Conn, msg: Msg) !void {
        var varmsg = msg;
        var msgreader = varmsg.reader();
        const authtype = try msgreader.readInt32();
        std.debug.assert(authtype == 10);

        var scramsha256 = false;
        var scramsha256plus = false;
        while (msgreader.readStringOptional()) |mechanism| {
            if (std.ascii.eqlIgnoreCase(mechanism, "SCRAM-SHA-256")) {
                scramsha256 = true;
            }
            if (std.ascii.eqlIgnoreCase(mechanism, "SCRAM-SHA-256-PLUS")) {
                scramsha256plus = true;
            }
        }

        self.writer.writeMsgStart('p');
        self.writer.write("SCRAM-SHA-256");
    }

    pub fn close(self: *Conn) void {
        self.stream.close();
        self.reader.deinit();
        self.writer.deinit();
        std.debug.print("Connection closed\n", .{});
    }
};
