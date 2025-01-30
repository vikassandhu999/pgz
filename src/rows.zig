const std = @import("std");

const Allocator = std.mem.Allocator;

const proto3 = @import("./proto3/proto3.zig");
const ErrorResponse = proto3.ErrorResponse;
const Message = proto3.Message;

const Reader = @import("./reader.zig").Reader;
const traits = @import("./traits.zig");

pub const FieldDescription = struct {
    name: []const u8,
    tableoid: i32,
    tableattributenumber: i16,
    datatypeoid: i32,
    datatypesize: i16,
    typemodifier: i32,
    format: i16 = 0,
};

pub const Rows = struct {
    fields: []FieldDescription = undefined,
    reader: *Reader,
    _a: Allocator,
    _state: State = .ReadingRowDescription,
    _reading: ?Message = null,
    queryerror: ?ErrorResponse = null,

    const State = enum {
        ReadingRowDescription,
        ReadingRows,
        Completed,
        Errored,
    };

    const Self = @This();

    pub fn init(reader: *Reader, allocator: Allocator) !Rows {
        return .{
            ._a = allocator,
            .reader = reader,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.fields.len > 0) {
            self._a.free(self.fields);
        }
    }

    pub fn hasnext(self: *Self) !bool {
        return switch (self._state) {
            .ReadingRowDescription => {
                try self.read_fielddescription();
                self._state = .ReadingRows;
                return self.hasnext();
            },
            .ReadingRows => {
                if (self._reading != null) {
                    return true;
                }
                const rowmsg = try self.reader.read();
                //TODO: handle all cases for query commands
                switch (rowmsg.msgtype()) {
                    'D' => {
                        self._reading = rowmsg;
                        self._state = .ReadingRows;
                        return true;
                    },
                    'C' => {
                        self._state = .Completed;
                        return false;
                    },
                    'E' => {
                        self.queryerror = try ErrorResponse.decode(rowmsg);
                        std.debug.print("query error: {any}", .{self.queryerror});
                        self._state = .Errored;
                        return false;
                    },
                    else => {
                        self.deinit();
                        return error.UnexpectedDBMessage;
                    },
                }
            },
            .Completed => return false,
            .Errored => return false,
        };
    }

    pub fn read(self: *Self, allocator: Allocator, args: anytype) !void {
        const ArgsType = @TypeOf(args);
        const ArgsT = @typeInfo(ArgsType);

        comptime {
            switch (ArgsT) {
                .Struct => |s| if (!s.is_tuple) {
                    @compileError("Expected tuple type, found struct '" ++ @typeName(ArgsType) ++ "'");
                },
                else => @compileError("Expected tuple type, found '" ++ @typeName(ArgsType) ++ "'"),
            }

            for (@typeInfo(ArgsType).Struct.fields, 0..) |field, idx| {
                const FieldType = field.type;
                const field_info = @typeInfo(FieldType);

                if (field_info != .Pointer) {
                    @compileError("Tuple element " ++ std.fmt.comptimePrint("{}", .{idx}) ++
                        " must be pointer type, found '" ++ @typeName(FieldType) ++ "'");
                }
            }
        }

        if (!try self.hasnext()) {
            return error.NoMorRows;
        }
        const msg = self._reading orelse unreachable;

        var rowreader = msg.reader();

        const msgtype = try rowreader.readByte();
        // hasnext should only return true if self._reading is a DataRow message.
        std.debug.assert(msgtype == 'D');

        // skip length.
        _ = try rowreader.readInt32();

        const colscount: usize = @intCast(try rowreader.readInt16());

        // fields description count should be same as column count in any given row for this query.
        std.debug.assert(colscount != self.fields.len);

        const fields = comptime ArgsT.Struct.fields;
        const argsCount = comptime fields.len;

        //TODO: decide if we want to return error for lesser or more number of args than fields.
        inline for (0..argsCount) |i| {
            if (i >= colscount) break;

            const field = @field(args, fields[i].name);
            const Field = @typeInfo(fields[i].type).Pointer.child;
            const T = @typeInfo(Field);

            const colsize = try rowreader.readInt32();
            switch (colsize) {
                -1 => {},
                0 => { // not data we can safely ignore.
                },
                else => {
                    const bytes = try rowreader.readBytes(@intCast(colsize));
                    // TODO: add runtime strict type matching using field description.
                    switch (T) {
                        .Int, .ComptimeInt => {
                            field.* = try std.mem.readInt(Field, bytes, .big);
                        },
                        .Float, .ComptimeFloat => {
                            field.* = try std.fmt.parseFloat(Field, bytes);
                        },
                        .Struct => {
                            if (comptime traits.isPgTypeDecoder(Field)) {
                                const x: Field = try Field.PgType.decodeAlloc(allocator, bytes, self.fields[i]);
                                field.* = x;
                            } else {
                                @compileError("Struct does not implement PgType.decodeAlloc");
                            }
                        },
                        .Bool => {
                            field.* = bytes[0] == 1;
                        },
                        .Pointer => |ptr| {
                            if (comptime ptr.child != u8) {
                                @compileError("Slice only can be a u8 slice");
                            }
                            field.* = bytes;
                        },
                        else => {
                            @compileError("Unsupported type " ++ @typeName(T));
                        },
                    }
                },
            }
        }
        self._reading = null;
    }

    fn read_fielddescription(self: *Self) !void {
        const msg = try self.reader.read();

        var msgreader = msg.reader();

        const msgtype = try msgreader.readByte();

        std.debug.assert(msgtype == 'T');
        // skip length.
        _ = try msgreader.readInt32();

        const fieldscount: usize = @intCast(try msgreader.readInt16());

        if (fieldscount != 0) {
            self.fields = try self._a.alloc(FieldDescription, fieldscount);
        }

        for (0..fieldscount) |i| {
            self.fields[i].name = try self._a.dupe(u8, try msgreader.readString());
            self.fields[i].tableoid = try msgreader.readInt32();
            self.fields[i].tableattributenumber = try msgreader.readInt16();
            self.fields[i].datatypeoid = try msgreader.readInt32();
            self.fields[i].datatypesize = try msgreader.readInt16();
            self.fields[i].typemodifier = try msgreader.readInt32();
            self.fields[i].format = try msgreader.readInt16();
        }
    }
};
