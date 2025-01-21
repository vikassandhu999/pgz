const std = @import("std");
const Writer = @import("../writer.zig").Writer;

pub const StartupMessage = struct {
    database: []const u8,
    user: []const u8,
    params: ?std.StringHashMap([]const u8) = null,

    pub fn init(user: []const u8, database: []const u8) StartupMessage {
        return .{ .user = user, .database = database };
    }

    pub fn write(self: *StartupMessage, writer: *Writer) !void {
        try writer.writeMsgStart(0);
        try writer.writeInt(u32, 196608);
        try writer.write("user");
        try writer.writeByte(0);
        try writer.write(self.user);
        try writer.writeByte(0);
        try writer.write("database");
        try writer.writeByte(0);
        try writer.write(self.database);
        try writer.writeByte(0);
        try writer.writeByte(0);
        try writer.writeMsgEnd();
    }
};
