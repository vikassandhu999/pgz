const std = @import("std");

const Allocator = std.mem.Allocator;

pub const ScramClient = struct {
    selectedmechanism: []const u8,
    clientnonce: []u8,
    clientfirstmessage: []u8,
    serverfirstmessage: []u8 = undefined,
    clientandservernonce: []u8 = undefined,
    salt: u8 = undefined,
    iterations: u32 = 0,
    saltedpassword: []u8 = undefined,
    authmessage: []u8 = undefined,

    _a: Allocator,

    const Self = @This();

    const Base64Encoder = std.base64.standard.Encoder;

    pub fn init(selectedmechanism: []const u8, allocator: Allocator) !ScramClient {
        const clientnonce = try allocator.alloc(u8, 18);
        std.crypto.random.bytes(clientnonce);
        const encodinglength = Base64Encoder.calcSize(clientnonce.len);
        var clientfirstmessage = try allocator.alloc(u8, 8 + encodinglength);
        std.mem.copyForwards(u8, clientfirstmessage[0..8], "n,,n=,r=");
        _ = Base64Encoder.encode(clientfirstmessage[8..], clientnonce);

        return .{
            .selectedmechanism = selectedmechanism,
            ._a = allocator,
            .clientnonce = clientnonce,
            .clientfirstmessage = clientfirstmessage,
        };
    }

    pub fn deinit(self: Self) void {
        if (self.clientfirstmessage) |m| {
            self._a.free(m);
        }
        if (self.clientnonce) |n| {
            self._a.free(n);
        }
    }

    pub fn handle_serverfirstmessage(self: Self, msg: []const u8) !void {
        self.serverfirstmessage = @ptrCast(msg);
    }
};
