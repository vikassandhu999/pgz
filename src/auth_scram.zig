const std = @import("std");

const Allocator = std.mem.Allocator;

pub const ScramClient = struct {
    mechanism: []const u8,
    clientnonce: []u8 = undefined,
    clientfirstmessage: []u8 = undefined,
    clientfinalmessage: []u8 = undefined,
    serverfirstmessage: []u8 = undefined,
    clientandservernonce: []u8 = undefined,
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

    pub fn deinit(_: Self) void {
        // if (self.clientfirstmessage) |m| {
        //     self._a.free(m);
        // }
        // if (self.clientnonce) |n| {
        //     self._a.free(n);
        // }
        // if (self.salt) |s| {
        //     self._a.free(s);
        // }
        // if (self.serverfirstmessage) |m| {
        //     self._a.free(m);
        // }
        // if (self.clientfinalmessage) |m| {
        //     self._a.free(m);
        // }
    }

    pub fn build_clientfirstmessage(self: *Self) ![]const u8 {
        self.clientnonce = try self._a.alloc(u8, 18);
        std.crypto.random.bytes(self.clientnonce);
        const encodinglength = Base64Encoder.calcSize(self.clientnonce.len);
        self.clientfirstmessage = try self._a.alloc(u8, 8 + encodinglength);
        std.mem.copyForwards(u8, self.clientfirstmessage[0..8], "n,,n=,r=");
        _ = Base64Encoder.encode(self.clientfirstmessage[8..], self.clientnonce);
        return self.clientfirstmessage;
    }

    pub fn handle_serverfirstmessage(self: *Self, msg: []const u8) !void {
        self.serverfirstmessage = try self._a.dupe(u8, msg);

        var buf = self.serverfirstmessage;
        if (!std.mem.startsWith(u8, buf, "r=")) {
            return error.MissingNonce;
        }
        buf = buf[2..];

        const noncesep_idx = std.mem.indexOfScalar(u8, buf, ',') orelse return error.InvalidServerFirstMessagee;
        self.clientandservernonce = buf[0..noncesep_idx];
        buf = buf[noncesep_idx + 1 ..];

        if (!std.mem.startsWith(u8, buf, "s=")) {
            return error.MissingSalt;
        }
        buf = buf[2..];

        const saltsep_idx = std.mem.indexOfScalar(u8, buf, ',') orelse return error.InvalidServerFirstMessagee;
        const encodedsalt = buf[0..saltsep_idx];
        buf = buf[saltsep_idx + 1 ..];
        self.salt = try self._a.alloc(u8, try Base64Decoder.calcSizeForSlice(encodedsalt));
        try Base64Decoder.decode(self.salt, encodedsalt);

        if (!std.mem.startsWith(u8, buf, "i=")) {
            return error.MissingIterations;
        }
        buf = buf[2..];
        self.iterations = try std.fmt.parseInt(u32, buf, 10);
    }

    // reference https://datatracker.ietf.org/doc/html/rfc5802#section-3
    pub fn build_clientfinalmessage(self: *Self, password: []const u8) ![]u8 {
        self.saltedpassword = try self._a.alloc(u8, 32);
        try std.crypto.pwhash.pbkdf2(self.saltedpassword, password, self.salt, self.iterations, std.crypto.auth.hmac.sha2.HmacSha256);

        const withoutproof = try std.fmt.allocPrint(self._a, "c=biws,r={s}", .{self.clientandservernonce});
        defer self._a.free(withoutproof);

        self.authmessage = try std.fmt.allocPrint(self._a, "{s},{s},{s}", .{ self.clientfirstmessage[3..], self.serverfirstmessage, withoutproof });

        var clientkey: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&clientkey, "Client Key", self.saltedpassword);

        var storedkey: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(&clientkey, &storedkey, .{});

        var clientsignature: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&clientsignature, self.authmessage, &storedkey);

        var proof: [32]u8 = undefined;
        for (clientkey, clientsignature, 0..) |k, s, i| {
            proof[i] = k ^ s;
        }

        var encodedproof: [44]u8 = undefined;
        _ = Base64Encoder.encode(&encodedproof, &proof);

        self.clientfinalmessage = try std.fmt.allocPrint(self._a, "{s},p={s}", .{ withoutproof, encodedproof });
        std.debug.print("\n\nfinal message: {s}\n\n", .{self.clientfinalmessage});
        return @ptrCast(self.clientfinalmessage);
    }

    pub fn handle_serverfinalmessage(self: *Self, msg: []const u8) !void {
        if (!std.mem.startsWith(u8, msg, "v=")) {
            return error.InvalidServerFirstMessage;
        }
        const got_serversignature = try self._a.alloc(u8, try Base64Decoder.calcSizeForSlice(msg[2..]));
        _ = try Base64Decoder.decode(got_serversignature, msg[2..]);

        var serverkey: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&serverkey, "Server Key", self.saltedpassword);

        var want_serversignature: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&want_serversignature, self.authmessage, &serverkey);

        if (!std.mem.eql(u8, got_serversignature, &want_serversignature)) {
            return error.InvalidServerSignature;
        }
    }
};
