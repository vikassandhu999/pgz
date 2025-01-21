const std = @import("std");

const Allocator = std.mem.Allocator;

pub const Message = struct {
    buf: []u8,
    allocator: Allocator,

    pub fn init(buf: []u8, allocator: Allocator) !Message {
        return .{ .buf = buf, .allocator = allocator };
    }

    pub fn deinit(self: *Message) void {
        self.allocator.free(self.buf);
    }

    pub fn reader(self: *const Message) MsgReader {
        return MsgReader.init(self.buf);
    }

    pub fn msgtype(self: *Message) u8 {
        std.debug.assert(self.buf.len >= 1);
        return self.buf[0];
    }
};

pub const MsgReader = struct {
    cursor: usize,
    buf: []u8,

    pub fn init(buf: []u8) MsgReader {
        return .{ .cursor = 0, .buf = buf };
    }

    pub fn readByte(self: *MsgReader) !u8 {
        if (self.cursor + 1 >= self.buf.len) return error.NoMoreData;
        const byte = self.buf[self.cursor];
        self.cursor += 1;
        return byte;
    }

    pub fn readInt16(self: *MsgReader) !u16 {
        if (self.cursor + 2 >= self.buf.len) return error.NoMoreData;
        const val = std.mem.readInt(u16, self.buf[self.cursor..][0..2], .big);
        self.cursor += 2;
        return val;
    }

    pub fn readInt32(self: *MsgReader) !u32 {
        if (self.cursor + 4 >= self.buf.len) return error.NoMoreData;
        const val = std.mem.readInt(u32, self.buf[self.cursor..][0..4], .big);
        self.cursor += 4;
        return val;
    }

    pub fn readStringOptional(self: *MsgReader) ?[]const u8 {
        const endIdx = std.mem.indexOfScalarPos(u8, self.buf, self.cursor, 0) orelse return null;
        const val = self.buf[self.cursor..endIdx];
        self.cursor = endIdx + 1;
        return val;
    }

    pub fn readString(self: *MsgReader) ![]const u8 {
        if (self.cursor + 1 >= self.buf.len) return error.NoMoreData;
        return self.readStringOptional() orelse return error.NotAString;
    }

    pub fn readAllRemaining(self: *MsgReader) ![]const u8 {
        if (self.cursor + 1 >= self.buf.len) return error.NoMoreData;
        const val = self.buf[self.cursor..];
        self.cursor = self.buf.len;
        return val;
    }

    pub fn ended(self: *MsgReader) bool {
        return self.cursor >= self.buf.len;
    }
};
