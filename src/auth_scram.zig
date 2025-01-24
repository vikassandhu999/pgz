const std = @import("std");

const Allocator = std.mem.Allocator;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;
const pbkdf2 = std.crypto.pwhash.pbkdf2;

pub const Authentication = enum(u32) {
    Ok = 0,
    SASL = 10,
    SASLContinue = 11,
    SASLFinal = 12,
};

pub const ScramClient = struct {
    mechanism: []const u8,
    clientnonce: []u8 = undefined,
    clientfirst: []u8 = undefined,
    clientfinal: []u8 = undefined,
    serverfirst: []u8 = undefined,
    servernonce: []u8 = undefined,
    salt: []u8 = undefined,
    iterations: u32 = 0,
    saltedpassword: []u8 = undefined,
    authmessage: []u8 = undefined,

    _a: Allocator,

    const Self = @This();

    const Base64Encoder = std.base64.standard.Encoder;
    const Base64Decoder = std.base64.standard.Decoder;

    pub fn init(mechanism: []const u8, allocator: Allocator) !ScramClient {
        return .{
            .mechanism = mechanism,
            ._a = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.clientfirst.len > 0) {
            self._a.free(self.clientfirst);
        }
        if (self.clientnonce.len > 0) {
            self._a.free(self.clientnonce);
        }
        if (self.salt.len > 0) {
            self._a.free(self.salt);
        }
        if (self.serverfirst.len > 0) {
            self._a.free(self.serverfirst);
        }
        if (self.clientfinal.len > 0) {
            self._a.free(self.clientfinal);
        }
    }

    pub fn create_clientfirstmessage(self: *Self) ![]const u8 {
        self.clientnonce = try self._a.alloc(u8, 18);
        std.crypto.random.bytes(self.clientnonce);

        const encodinglength = Base64Encoder.calcSize(self.clientnonce.len);
        self.clientfirst = try self._a.alloc(u8, 8 + encodinglength);

        std.mem.copyForwards(u8, self.clientfirst[0..8], "n,,n=,r=");
        _ = Base64Encoder.encode(self.clientfirst[8..], self.clientnonce);

        return self.clientfirst;
    }

    pub fn handle_serverfirstmessage(self: *Self, data: []const u8) !void {
        self.serverfirst = try self._a.dupe(u8, data);

        var parts = std.mem.splitSequence(u8, data, ",");

        {
            var nonce_part = parts.next() orelse return error.MissingNonce;
            if (!std.mem.startsWith(u8, nonce_part, "r=")) {
                return error.InvalidNonceStart;
            }
            self.servernonce = try self._a.dupe(u8, nonce_part[2..]);
        }

        {
            var salt_part = parts.next() orelse return error.MissingSalt;
            if (!std.mem.startsWith(u8, salt_part, "s=")) {
                return error.InvalidSaltStart;
            }
            const encoded_salt = salt_part[2..];
            self.salt = try self._a.alloc(u8, try Base64Decoder.calcSizeForSlice(encoded_salt));
            try Base64Decoder.decode(self.salt, encoded_salt);
        }

        {
            const iterations_part = parts.next() orelse return error.MissingIterations;
            if (!std.mem.startsWith(u8, iterations_part, "i=")) {
                return error.InvalidIterationsStart;
            }
            self.iterations = try std.fmt.parseInt(u32, iterations_part[2..], 10);
        }
    }

    // reference https://datatracker.ietf.org/doc/html/rfc5802#section-3
    pub fn create_clientfinalmessage(self: *Self, password: []const u8) ![]u8 {
        self.saltedpassword = try self._a.alloc(u8, 32);
        try pbkdf2(self.saltedpassword, password, self.salt, self.iterations, HmacSha256);

        const withoutproof = try std.fmt.allocPrint(self._a, "c=biws,r={s}", .{self.servernonce});
        defer self._a.free(withoutproof);

        self.authmessage = try std.fmt.allocPrint(self._a, "{s},{s},{s}", .{ self.clientfirst[3..], self.serverfirst, withoutproof });

        var clientkey: [32]u8 = undefined;
        HmacSha256.create(&clientkey, "Client Key", self.saltedpassword);

        var storedkey: [32]u8 = undefined;
        Sha256.hash(&clientkey, &storedkey, .{});

        var clientsignature: [32]u8 = undefined;
        HmacSha256.create(&clientsignature, self.authmessage, &storedkey);

        var proof: [32]u8 = undefined;
        for (clientkey, clientsignature, 0..) |k, s, i| {
            proof[i] = k ^ s;
        }

        var encodedproof: [44]u8 = undefined;
        _ = Base64Encoder.encode(&encodedproof, &proof);

        self.clientfinal = try std.fmt.allocPrint(self._a, "{s},p={s}", .{ withoutproof, encodedproof });
        return @ptrCast(self.clientfinal);
    }

    pub fn verify_severfinalmessage(self: *Self, data: []const u8) !void {
        if (!std.mem.startsWith(u8, data, "v=")) {
            return error.InvalidServerFirstMessage;
        }
        const serversignature = try self._a.alloc(u8, try Base64Decoder.calcSizeForSlice(data[2..]));
        _ = try Base64Decoder.decode(serversignature, data[2..]);

        var serverkey: [32]u8 = undefined;
        HmacSha256.create(&serverkey, "Server Key", self.saltedpassword);

        var computedsignature: [32]u8 = undefined;
        HmacSha256.create(&computedsignature, self.authmessage, &serverkey);

        if (!std.mem.eql(u8, serversignature, &computedsignature)) {
            return error.InvalidServerSignature;
        }
    }
};
