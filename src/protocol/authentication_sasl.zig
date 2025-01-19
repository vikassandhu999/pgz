const std = @import("std");
const MessageReader = @import("./message.zig").MessageReader;

pub const AuthenticationSASL = struct {
    auth_mechanisms: std.ArrayList(Mechanism),

    const Mechanism = enum {
        SCRAM_SHA256,
        SCRAM_SHA256_PLUS,
    };

    pub fn init(reader: *MessageReader) !AuthenticationSASL {
        const val = AuthenticationSASL{ .auth_mechanisms = .{} };
        while (reader.readStringOptional()) |mechanism| {
            if (std.ascii.eqlIgnoreCase(mechanism, "SCRAM-SHA256")) {}
        }
        return val;
    }
};
