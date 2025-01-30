const std = @import("std");
const traits = @import("./traits.zig");

const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

pub const Writer = struct {
    stream: Stream,
    allocator: Allocator,
    buf: []u8,
    msgstart: usize,
    cursor: usize,

    const Self = @This();

    pub fn init(stream: Stream, allocator: Allocator) !Writer {
        return .{
            .buf = &.{},
            .stream = stream,
            .allocator = allocator,
            .msgstart = 0,
            .cursor = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.buf);
    }

    pub fn writeMsgStart(self: *Self, msgtype: u8) !void {
        self.cursor = 0;
        if (msgtype != 0) {
            try self.writeByte(msgtype);
        }
        self.msgstart = self.cursor;
        try self.writeInt(u32, 1);
    }

    pub fn writeMsgEnd(self: *Self) !void {
        std.debug.assert(self.cursor > 4);
        std.mem.writeInt(u32, self.buf[self.msgstart..][0..4], @intCast(self.cursor - self.msgstart), .big);
    }

    pub fn writeInt(self: *Self, comptime T: type, int: T) !void {
        const bytes = try self.advnaceComptime(@divExact(@typeInfo(T).Int.bits, 8));
        std.mem.writeInt(T, bytes, int, .big);
    }

    fn advnaceComptime(self: *Self, comptime n: usize) !*[n]u8 {
        try self.ensureCapacity(self.cursor + n);
        const res = self.buf[self.cursor..][0..n];
        self.cursor += n;
        return res;
    }

    pub fn writeByte(self: *Self, byte: u8) !void {
        try self.ensureCapacity(self.cursor + 1);
        self.buf[self.cursor] = byte;
        self.cursor += 1;
    }

    pub fn write(self: *Self, bytes: []const u8) !void {
        try self.ensureCapacity(self.cursor + bytes.len);
        @memcpy(self.buf[self.cursor .. self.cursor + bytes.len], bytes);
        self.cursor += bytes.len;
    }

    pub fn writeString(self: *Self, bytes: []const u8) !void {
        try self.ensureCapacity(self.cursor + bytes.len);
        @memcpy(self.buf[self.cursor .. self.cursor + bytes.len], bytes);
        self.cursor += bytes.len;
        try self.writeByte(0);
    }

    pub fn ensureCapacity(self: *Self, nreq: usize) !void {
        if (self.buf.len >= nreq) {
            return;
        }
        var newlen = self.buf.len;
        while (newlen > 0 and true) : (newlen *= 2) {
            if (newlen >= nreq) {
                break;
            }
        }
        if (newlen > 0) {
            self.buf = try self.allocator.realloc(self.buf, newlen);
            return;
        }

        newlen = self.buf.len;
        while (true) : (newlen += 8192) {
            if (newlen >= nreq) {
                break;
            }
        }
        self.buf = try self.allocator.realloc(self.buf, newlen);
        return;
    }

    pub fn writeToStream(self: *Self, msg: anytype) anyerror!void {
        const MsgType = @TypeOf(msg);
        const MsgTypeInfo = @typeInfo(MsgType);

        comptime {
            switch (MsgTypeInfo) {
                .Struct => |s| if (s.is_tuple) {
                    @compileError("Expected struct, enum or union, found tuple '" ++ @typeName(MsgType) ++ "'");
                },
                .Enum => {},
                .Union => {},
                else => @compileError("Expected struct, enum or union, found '" ++ @typeName(MsgType) ++ "'"),
            }
        }

        if (comptime traits.hasInternalEncoder(MsgType)) {
            try msg.encode(self);
            try self.stream.writeAll(self.buf[0..self.cursor]);
            return;
        }

        @compileError("Expected struct, enum or union with decode method.");
    }
};
