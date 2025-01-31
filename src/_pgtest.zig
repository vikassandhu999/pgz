const std = @import("std");

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
