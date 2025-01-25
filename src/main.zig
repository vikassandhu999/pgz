const std = @import("std");

const Allocator = std.mem.Allocator;

const Conn = @import("./conn.zig").Conn;

const Email = struct {
    email: []u8,

    const Self = @This();

    pub const PgType = struct {
        pub fn parse(allocator: Allocator, data: []const u8) !Self {
            var res: Email = undefined;
            res.email = try allocator.dupe(u8, data);
            return res;
        }
    };
};

const User = struct {
    id: []const u8 = undefined,
    email: Email,
};

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

    var rows = try conn.query("select id,email from users;");
    defer rows.deinit();

    var users: std.ArrayList(User) = std.ArrayList(User).init(allocator);
    while (try rows.hasnext()) {
        const user = try users.addOne();
        try rows.read(users.allocator, .{ &user.id, &user.email });
    }

    for (users.items, 0..) |user, i| {
        std.debug.print("\n\nuser {d} {d} {s}\n\n", .{ i, user.id, user.email.email });
    }

    defer conn.close();
}
