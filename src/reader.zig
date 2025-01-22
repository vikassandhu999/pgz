const std = @import("std");

const Stream = std.net.Stream;
const Allocator = std.mem.Allocator;
const Message = @import("./protocol/message.zig").Message;

pub const Reader = struct {
    stream: Stream,
    allocator: Allocator,
    buf: []u8,
    end: usize,
    start: usize,
    cursor: usize,

    pub fn init(stream: Stream, allocator: Allocator) !Reader {
        return .{
            .buf = &.{},
            .stream = stream,
            .allocator = allocator,
            .start = 0,
            .end = 0,
            .cursor = 0,
        };
    }

    pub fn deinit(self: *Reader) void {
        self.allocator.free(self.buf);
    }

    pub fn read(self: *Reader) !Message {
        std.debug.assert(self.start <= self.end);

        const min_toread = blk: {
            const start: i32 = @intCast(self.start);
            const end: i32 = @intCast(self.end);
            break :blk (5 - (end - start));
        };
        if (min_toread > 0) {
            const tsize: usize = @intCast(min_toread);
            try self.ensureCapacity(self.end + tsize);
            const nread = try self.stream.readAtLeast(self.buf[self.end..], tsize);
            if (nread == 0) {
                return error.UnknownError;
            }
            self.end += nread;
        }

        std.debug.assert(self.cursor + 5 <= self.end);

        const msgtype: u8 = std.mem.readInt(u8, self.buf[self.cursor..][0..1], .big);
        self.cursor += 1;
        const msglength: u32 = std.mem.readInt(u32, self.buf[self.cursor..][0..4], .big);
        self.cursor += 4;

        // excludes length byte itself.
        const buflength = msglength - 4;

        // msglength suggests stream has more bytes to be consumed.
        if (self.cursor + buflength > self.end) {
            try self.ensureCapacity(self.cursor + buflength);
            const nreamain = self.cursor + buflength - self.end;
            const nreadmore = try self.stream.readAtLeast(self.buf[self.end..], nreamain);
            if (nreadmore == 0) {
                return error.UnknownError;
            }
            self.end += nreadmore;
        }

        const msgbuf = try self.allocator.dupe(u8, self.buf[self.start .. self.cursor + buflength]);
        self.cursor += buflength;

        std.debug.assert(self.cursor <= self.end);

        self.start = self.cursor;

        std.debug.print("\nmsgtype: {c}\nmsglength: {d}\nmsgbuf: {s}\n", .{ msgtype, msglength, msgbuf });

        return Message.init(@ptrCast(msgbuf));
    }

    pub fn ensureCapacity(self: *Reader, nreq: usize) !void {
        if (self.buf.len >= nreq) {
            return;
        }

        // left justify data if we can.
        const needed = nreq - self.start;
        if (self.start < self.end) {
            if (self.start > 0) {
                @memcpy(self.buf[0 .. self.end - self.start], self.buf[self.start..self.end]);
                self.end -= self.start;
                self.cursor -= self.start;
                self.start = 0;
            }
        } else {
            self.start = 0;
            self.end = 0;
            self.cursor = 0;
        }
        if (self.buf.len >= needed) {
            return;
        }

        var newlen = self.buf.len;
        while (newlen > 0 and true) : (newlen *= 2) {
            if (newlen >= needed) {
                break;
            }
        }
        if (newlen > 0) {
            self.buf = try self.allocator.realloc(self.buf, newlen);
            return;
        }

        newlen = self.buf.len;
        while (true) : (newlen += 8192) {
            if (newlen >= needed) {
                break;
            }
        }
        self.buf = try self.allocator.realloc(self.buf, newlen);
        return;
    }
};
