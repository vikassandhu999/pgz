const std = @import("std");

const Writer = @import("../writer.zig").Writer;

const Allocator = std.mem.Allocator;

pub const PostgresError = error{
    ExpectedRequest,
    UnimplementedAuthRequest,
    SCRAMMethodNotSupported,
    ConnectionError,
};

pub const StartupMessage = struct {
    database: []const u8,
    user: []const u8,

    const Self = @This();

    pub fn init(user: []const u8, database: []const u8) Self {
        return .{ .user = user, .database = database };
    }

    pub fn encode(self: Self, writer: *Writer) !void {
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

pub const Authentication = union(enum) {
    Ok: void,
    SASL: struct {
        mechanism: []const u8,
    },
    SASLContinue: struct {
        data: []const u8,
    },
    SASLFinal: struct {
        data: []const u8,
    },
    Unknown: void,

    const Self = @This();

    pub fn decode(msg: Message) !Authentication {
        var reader = msg.reader();

        const msgtype = try reader.readByte();
        std.debug.assert(msgtype == 'R');

        _ = try reader.readInt32();

        switch (try reader.readInt32()) {
            0 => return .{ .Ok = {} },
            10 => {
                var selectedmech: ?[]const u8 = null;
                while (reader.readStringOptional()) |mechanism| {
                    // we are not going to support channel-binding for now.
                    if (std.ascii.eqlIgnoreCase(mechanism, "SCRAM-SHA-256")) {
                        selectedmech = mechanism;
                    }
                }
                if (selectedmech == null) {
                    return PostgresError.SCRAMMethodNotSupported;
                }
                return .{ .SASL = .{ .mechanism = selectedmech.? } };
            },
            11 => return .{ .SASLContinue = .{ .data = try reader.readAllRemaining() } },
            12 => return .{ .SASLFinal = .{ .data = try reader.readAllRemaining() } },
            else => return .{ .Unknown = {} },
        }
    }
};

pub const SASLInitialResponse = struct {
    mechanism: []const u8,
    clientfirstmessage: []const u8,

    const Self = @This();

    pub fn init(mechanism: []const u8, clientfirstmessage: []const u8) Self {
        return .{ .mechanism = mechanism, .clientfirstmessage = clientfirstmessage };
    }

    pub fn encode(self: Self, writer: *Writer) !void {
        try writer.writeMsgStart('p');
        try writer.writeString(self.mechanism);
        try writer.writeInt(i32, @intCast(self.clientfirstmessage.len));
        try writer.write(self.clientfirstmessage);
        try writer.writeMsgEnd();
    }
};

pub const SASLResponse = struct {
    clientfinalmessage: []const u8,

    const Self = @This();

    pub fn init(clientfinalmessage: []const u8) Self {
        return .{ .clientfinalmessage = clientfinalmessage };
    }

    pub fn encode(self: Self, writer: *Writer) !void {
        try writer.writeMsgStart('p');
        try writer.write(self.clientfinalmessage);
        try writer.writeMsgEnd();
    }
};

pub const Query = struct {
    sql: []const u8,

    const Self = @This();

    pub fn init(sql: []const u8) Self {
        return .{ .sql = sql };
    }

    pub fn encode(self: Self, writer: *Writer) !void {
        try writer.writeMsgStart('Q');
        try writer.writeString(self.sql);
        try writer.writeMsgEnd();
    }
};

pub const ErrorResponseRaw = struct {
    msg: Message,
};

pub const ErrorResponse = struct {
    severity: []const u8 = undefined,
    severity_unlocalized: []const u8 = undefined,
    code: []const u8 = undefined,
    message: []const u8 = undefined,
    detail: []const u8 = undefined,
    hint: []const u8 = undefined,
    position: i32 = undefined,
    internal_position: i32 = undefined,
    internal_query: []const u8 = undefined,
    where: []const u8 = undefined,
    schema_name: []const u8 = undefined,
    table_name: []const u8 = undefined,
    column_name: []const u8 = undefined,
    data_type_name: []const u8 = undefined,
    constraint_name: []const u8 = undefined,
    file: []const u8 = undefined,
    line: i32 = undefined,
    routine: []const u8 = undefined,

    _a: Allocator,

    pub fn deinit(self: *@This()) void {
        const strings = [_][]u8{
            self.severity,
            self.severity_unlocalized,
            self.code,
            self.message,
            self.detail,
            self.hint,
            self.internal_query,
            self.where,
            self.schema_name,
            self.table_name,
            self.column_name,
            self.data_type_name,
            self.constraint_name,
            self.file,
            self.line,
            self.routine,
        };

        for (strings) |string| {
            if (string.len > 0) {
                self._a.free(string);
            }
        }
    }

    pub fn decodeAlloc(msg: Message, allocator: Allocator) !@This() {
        var reader = msg.reader();

        const msgtype = try reader.readByte();
        std.debug.assert(msgtype == 'E');

        _ = try reader.readInt32();

        var res = ErrorResponse{ ._a = allocator };

        while (true) {
            const k = try reader.readByte();
            if (k == 0) {
                return res;
            }
            const str = try reader.readString();
            switch (k) {
                'S' => {
                    res.severity = try res._a.dupe(str);
                },
                'V' => {
                    res.severity_unlocalized = try res._a.dupe(str);
                },
                'C' => {
                    res.code = try res._a.dupe(str);
                },
                'M' => {
                    res.message = try res._a.dupe(str);
                },
                'D' => {
                    res.detail = try res._a.dupe(str);
                },
                'H' => {
                    res.hint = try res._a.dupe(str);
                },
                'P' => {
                    res.position = try std.fmt.parseInt(i32, str, 10);
                },
                'p' => {
                    res.internal_position = try std.fmt.parseInt(i32, str, 10);
                },
                'q' => {
                    res.internal_query = try res._a.dupe(str);
                },
                'W' => {
                    res.where = try res._a.dupe(str);
                },
                's' => {
                    res.schema_name = try res._a.dupe(str);
                },
                't' => {
                    res.table_name = try res._a.dupe(str);
                },
                'c' => {
                    res.column_name = try res._a.dupe(str);
                },
                'd' => {
                    res.data_type_name = try res._a.dupe(str);
                },
                'n' => {
                    res.constraint_name = try res._a.dupe(str);
                },
                'F' => {
                    res.file = try res._a.dupe(str);
                },
                'R' => {
                    res.routine = try res._a.dupe(str);
                },
                else => {},
            }
        }
    }
};

pub const FieldDescription = struct {
    name: []const u8,
    tableoid: i32,
    tableattributenumber: i16,
    datatypeoid: i32,
    datatypesize: i16,
    typemodifier: i32,
    format: i16 = 0,
};

pub const RowDescription = struct {
    fields: []FieldDescription = undefined,
    _a: Allocator,

    fn init(allocator: Allocator) !RowDescription {
        return .{ ._a = allocator };
    }

    pub fn deinit(self: *@This()) void {
        if (self.fields.len == 0) return;

        for (self.fields) |field| {
            if (field.name.len > 0) {
                self._a.free(field.name);
            }
        }
        self._a.free(self.fields);
    }

    pub fn fieldsCount(self: *@This()) usize {
        return self.fields.len;
    }

    pub fn fieldAt(self: *@This(), column: usize) !FieldDescription {
        std.debug.assert(self.fieldsCount() > column);
        return self.fields[column];
    }

    pub fn decodeAlloc(msg: Message, allocator: Allocator) !RowDescription {
        var reader = msg.reader();

        const msgtype = try reader.readByte();
        std.debug.assert(msgtype == 'T');

        _ = try reader.readInt32();

        const fieldscount: usize = @intCast(try reader.readInt16());

        var rd = try RowDescription.init(allocator);

        if (fieldscount != 0) {
            rd.fields = try allocator.alloc(FieldDescription, fieldscount);
        }

        for (0..fieldscount) |i| {
            rd.fields[i].name = try rd._a.dupe(u8, try reader.readString());
            rd.fields[i].tableoid = try reader.readInt32();
            rd.fields[i].tableattributenumber = try reader.readInt16();
            rd.fields[i].datatypeoid = try reader.readInt32();
            rd.fields[i].datatypesize = try reader.readInt16();
            rd.fields[i].typemodifier = try reader.readInt32();
            rd.fields[i].format = try reader.readInt16();
        }

        return rd;
    }
};

pub const Message = struct {
    buf: []const u8,

    pub fn init(buf: []const u8) !Message {
        return .{ .buf = buf };
    }

    pub fn reader(self: Message) MessageReader {
        return MessageReader.init(self.buf);
    }

    pub fn msgtype(self: Message) u8 {
        std.debug.assert(self.buf.len >= 1);
        return self.buf[0];
    }
};

pub const MessageReader = struct {
    cursor: usize,
    buf: []const u8,

    pub fn init(buf: []const u8) MessageReader {
        return .{ .cursor = 0, .buf = buf };
    }

    pub fn readByte(self: *MessageReader) !u8 {
        if (self.cursor + 1 > self.buf.len) return error.NoMoreData;
        const byte = self.buf[self.cursor];
        self.cursor += 1;
        return byte;
    }

    pub fn readBytes(self: *MessageReader, n: usize) ![]const u8 {
        if (self.cursor + n > self.buf.len) return error.NoMoreData;
        const bytes = self.buf[self.cursor .. self.cursor + n];
        self.cursor += n;
        return bytes;
    }

    pub fn readInt16(self: *MessageReader) !i16 {
        if (self.cursor + 2 > self.buf.len) return error.NoMoreData;
        const val = std.mem.readInt(i16, self.buf[self.cursor..][0..2], .big);
        self.cursor += 2;
        return val;
    }

    pub fn readInt32(self: *MessageReader) !i32 {
        if (self.cursor + 4 > self.buf.len) return error.NoMoreData;
        const val = std.mem.readInt(i32, self.buf[self.cursor..][0..4], .big);
        self.cursor += 4;
        return val;
    }

    pub fn readStringOptional(self: *MessageReader) ?[]const u8 {
        const endIdx = std.mem.indexOfScalarPos(u8, self.buf, self.cursor, 0) orelse return null;
        const val = self.buf[self.cursor..endIdx];
        self.cursor = endIdx + 1;
        return val;
    }

    pub fn readString(self: *MessageReader) ![]const u8 {
        if (self.cursor + 1 > self.buf.len) return error.NoMoreData;
        return self.readStringOptional() orelse return error.NotAString;
    }

    pub fn readAllRemaining(self: *MessageReader) ![]const u8 {
        if (self.cursor + 1 > self.buf.len) return error.NoMoreData;
        const val = self.buf[self.cursor..];
        self.cursor = self.buf.len;
        return val;
    }

    pub fn ended(self: *MessageReader) bool {
        return self.cursor >= self.buf.len;
    }
};
