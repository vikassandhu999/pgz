const std = @import("std");
const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

const Reader = @import("./reader.zig").Reader;
const Writer = @import("./writer.zig").Writer;

pub const StartupMsg = struct {
    database: []const u8,
    user: []const u8,
    params: ?std.StringHashMap([]const u8) = null,

    pub fn write(self: *StartupMsg, writer: *Writer) !void {
        try writer.writeMsgStart(0);
        try writer.writeInt(u32, 196608);
        try writer.write(&"user".*);
        try writer.writeByte(0);
        try writer.write(self.user);
        try writer.writeByte(0);
        try writer.write(&"database".*);
        try writer.writeByte(0);
        try writer.write(self.database);
        try writer.writeByte(0);
        try writer.sendMsg();
    }
};

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

    pub const Errors = error{UnknownError};

    pub fn init(opts: Opts, allocator: Allocator) !Conn {
        var conn = try Conn.connect(opts, allocator);
        try conn.auth(opts);
        return conn;
    }

    pub fn deinit(self: *Conn) void {
        self.stream.close();
        self.reader.deinit();
        self.writer.deinit();
        std.debug.print("Connection closed\n", .{});
    }

    pub fn auth(self: *Conn, opts: Opts) !void {
        var startupMsg = StartupMsg{ .user = opts.user, .database = opts.database };
        try startupMsg.write(&self.writer);

        const msg = try self.reader.read();
        std.debug.print("message: {any}", .{msg});
    }

    fn connect(opts: Opts, allocator: Allocator) !Conn {
        const address = std.net.Address.initIp4(opts.host, opts.port);

        const stream = try std.net.tcpConnectToAddress(address);
        errdefer stream.close();

        const reader = try Reader.init(stream, allocator);
        errdefer reader.deinit();

        const writer = try Writer.init(stream, allocator);
        errdefer writer.deinit();

        return .{
            .stream = stream,
            .allocator = allocator,
            .reader = reader,
            .writer = writer,
        };
    }
};
