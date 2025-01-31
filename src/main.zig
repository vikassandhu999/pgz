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

pub const PostgresConfig = struct {
    name: []const u8,
    user: []const u8,
    password: []const u8,
    database: []const u8,
    port: u16,
    data_dir: ?[]const u8,
    image: []const u8 = "postgres:15",

    pub fn formatDockerArgs(self: *PostgresConfig, allocator: std.mem.Allocator) ![]const u8 {
        var args = std.ArrayList([]const u8).init(allocator);

        try args.append("-e");
        try args.append(try std.fmt.allocPrint(allocator, "POSTGRES_USER={s}", .{self.user}));
        try args.append("-e");
        try args.append(try std.fmt.allocPrint(allocator, "POSTGRES_PASSWORD={s}", .{self.password}));
        try args.append("-e");
        try args.append(try std.fmt.allocPrint(allocator, "POSTGRES_DB={s}", .{self.database}));
        try args.append("-e");
        try args.append(try std.fmt.allocPrint(allocator, "POSTGRES_LOGGING={s}", .{"debug"}));
        try args.append("-p");
        try args.append(try std.fmt.allocPrint(allocator, "{d}:5432", .{self.port}));

        if (self.data_dir) |dir| {
            try args.append("-v");
            try args.append(try std.fmt.allocPrint(allocator, "{s}:/var/lib/postgresql/data", .{dir}));
        }

        return std.mem.join(allocator, " ", args.items);
    }
};

pub const PostgresInstance = struct {
    config: PostgresConfig,
    allocator: std.mem.Allocator,

    pub fn start(self: *PostgresInstance) !void {
        const args = try self.config.formatDockerArgs(self.allocator);
        defer self.allocator.free(args);

        const cmd = try std.fmt.allocPrint(self.allocator, "docker run --rm -d {s} --name {s} {s}", .{ args, self.config.name, self.config.image });

        defer self.allocator.free(cmd);

        var proc = std.process.Child.init(&[_][]const u8{ "sh", "-c", cmd }, self.allocator);
        _ = try proc.spawnAndWait();
    }

    pub fn stop(self: *PostgresInstance) !void {
        const cmd = try std.fmt.allocPrint(self.allocator, "docker stop {s}", .{self.config.name});
        defer self.allocator.free(cmd);

        var proc = std.process.Child.init(&[_][]const u8{ "sh", "-c", cmd }, self.allocator);
        _ = try proc.spawnAndWait();
    }
};

test "connect" {
    const allocator = std.heap.page_allocator;

    const config = PostgresConfig{
        .name = "zig_pg_test",
        .user = "test",
        .password = "test",
        .database = "testdb",
        .port = 5433,
        .data_dir = "/tmp/zig_pg_data",
    };

    var instance = PostgresInstance{ .config = config, .allocator = allocator };

    try instance.start();
    std.debug.print("Started PostgreSQL at port {}\n", .{config.port});

    // Cleanup
    try instance.stop();
    std.debug.print("Stopped PostgreSQL instance\n", .{});
}
