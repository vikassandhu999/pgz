const std = @import("std");

const Allocator = std.mem.Allocator;

const Conn = @import("./conn.zig").Conn;
const FieldDescription = @import("./proto3/proto3.zig").FieldDescription;

const Email = struct {
    email: []const u8,
    _a: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, email: []const u8) Email {
        return .{
            ._a = allocator,
            .email = email,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.email.len > 0) {
            self._a.free(self.email);
        }
    }

    pub const PgType = struct {
        pub fn decodeAlloc(allocator: Allocator, data: []const u8, _: FieldDescription) !Self {
            return Email.init(allocator, try allocator.dupe(u8, data));
        }

        pub fn encode() ![]const u8 {}
    };
};

const User = struct {
    id: []const u8 = undefined,
    email: Email,
    _a: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) !User {
        return .{
            ._a = allocator,
            .email = Email.init(allocator, undefined),
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.id.len > 0) {
            self._a.free(self.id);
        }
        self.email.deinit();
    }
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

    var conn = try Conn.connect(opts, allocator);

    var rows = try conn.query("select id,emaildd from users where 1=1;");
    defer rows.deinit();

    var users: std.ArrayList(User) = std.ArrayList(User).init(allocator);
    defer {
        for (users.items) |*item| {
            item.deinit();
        }
        users.deinit();
    }

    while (try rows.hasNext()) {
        const user = try users.addOne();
        try rows.readOne(users.allocator, .{ &user.id, &user.email });
    }

    for (users.items, 0..) |user, i| {
        std.debug.print("\n\nuser {d} {d} {s}\n\n", .{ i, user.id, user.email.email });
    }

    defer conn.disconnect();
}
