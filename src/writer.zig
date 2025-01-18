const std = @import("std");

const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;

const Msg = @import("./reader.zig");

pub const Writer = struct {
    stream: Stream,
    allocator: Allocator,
    buf: []u8,
    msgstart: usize,
    cursor: usize,

    pub fn init(stream: Stream, allocator: Allocator) !Writer {
        return .{
            .buf = &.{},
            .stream = stream,
            .allocator = allocator,
            .msgstart = 0,
            .cursor = 0,
        };
    }

    pub fn deinit(self: *Writer) void {
        self.allocator.free(self.buf);
    }

    pub fn writeMsgStart(self: *Writer, msgtype: u8) !void {
        self.cursor = 0;
        if (msgtype != 0) {
            try self.writeByte(msgtype);
        }
        self.msgstart = self.cursor;
        try self.writeInt(u32, 1);
    }

    pub fn sendMsg(self: *Writer) !void {
        std.debug.assert(self.cursor > 4);
        try self.writeByte(0);
        std.mem.writeInt(u32, self.buf[self.msgstart..][0..4], @intCast(self.cursor - self.msgstart), .big);
        try self.stream.writeAll(self.buf[0..self.cursor]);
        self.msgstart = self.cursor;
    }

    pub fn writeInt(self: *Writer, comptime T: type, int: T) !void {
        const bytes = try self.advnaceComptime(@divExact(@typeInfo(T).Int.bits, 8));
        std.mem.writeInt(T, bytes, int, .big);
    }

    fn advnaceComptime(self: *Writer, comptime n: usize) !*[n]u8 {
        try self.ensureCapacity(self.cursor + n);
        const res = self.buf[self.cursor..][0..n];
        self.cursor += n;
        return res;
    }

    pub fn writeByte(self: *Writer, byte: u8) !void {
        try self.ensureCapacity(self.cursor + 1);
        self.buf[self.cursor] = byte;
        self.cursor += 1;
    }

    pub fn write(self: *Writer, bytes: []const u8) !void {
        try self.ensureCapacity(self.cursor + bytes.len);
        @memcpy(self.buf[self.cursor .. self.cursor + bytes.len], bytes);
        self.cursor += bytes.len;
    }

    pub fn ensureCapacity(self: *Writer, nreq: usize) !void {
        if (self.buf.len >= nreq) {
            return;
        }
        //
        // // left justify data if we can.
        // const needed = nreq - self.start;
        // if (self.start < self.end) {
        //     if (self.start > 0) {
        //         @memcpy(self.buf[0 .. self.end - self.start], self.buf[self.start..self.end]);
        //         self.end -= self.start;
        //         self.cursor -= self.start;
        //         self.start = 0;
        //     }
        // } else {
        //     self.start = 0;
        //     self.end = 0;
        //     self.cursor = 0;
        // }
        // if (self.buf.len >= needed) {
        //     return;
        // }
        //
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
};
