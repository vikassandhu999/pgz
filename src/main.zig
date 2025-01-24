const std = @import("std");
const reader = @import("./reader.zig");

const Conn = @import("./conn.zig").Conn;

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const opts = Conn.Opts{
        .host = [4]u8{ 127, 0, 0, 1 },
        .port = 5432,
        .database = "xobizz_admin",
        .user = "admin",
        .password = "L2KjxOH9al",
    };

    var conn = try Conn.open(opts, allocator);

    try conn.exec();

    defer conn.close();
}
