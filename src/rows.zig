const std = @import("std");

const Allocator = std.mem.Allocator;

const proto3 = @import("./proto3/proto3.zig");
const ErrorResponse = proto3.ErrorResponse;
const ErrorResponseRaw = proto3.ErrorResponseRaw;
const Message = proto3.Message;
const RowDescription = proto3.RowDescription;

const Reader = @import("./reader.zig").Reader;
const traits = @import("./traits.zig");

pub const Rows = struct {
    description: RowDescription = undefined,
    reader: *Reader,
    _a: Allocator,
    _state: State = .ReadingRowDescription,
    _reading: ?Message = null,
    queryerror: ?ErrorResponseRaw = null,

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
        self.description.deinit();
    }

    pub fn hasNext(self: *Self) !bool {
        return switch (self._state) {
            .ReadingRowDescription => {
                const msg = try self.reader.read();
                return switch (msg.msgtype()) {
                    'T' => {
                        self.description = try RowDescription.decodeAlloc(msg, self._a);
                        self._state = .ReadingRows;
                        return self.hasNext();
                    },
                    'E' => {
                        self.queryerror = ErrorResponseRaw{ .msg = msg };
                        self._state = .Errored;
                        return false;
                    },
                    else => {
                        self.deinit();
                        return error.UnexpectedDBMessage;
                    },
                };
            },
            .ReadingRows => {
                if (self._reading != null) {
                    return true;
                }
                const msg = try self.reader.read();
                //TODO: handle all cases for query commands
                switch (msg.msgtype()) {
                    'D' => {
                        self._reading = msg;
                        self._state = .ReadingRows;
                        return true;
                    },
                    'C' => {
                        self._state = .Completed;
                        return false;
                    },
                    'E' => {
                        self.queryerror = ErrorResponseRaw{ .msg = msg };
                        self._state = .Errored;
                        return false;
                    },
                    else => {
                        return error.UnexpectedDBMessage;
                    },
                }
            },
            .Completed => return false,
            .Errored => return false,
        };
    }

    pub fn readOne(self: *Self, allocator: Allocator, args: anytype) !void {
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

        if (!try self.hasNext()) {
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
        std.debug.assert(colscount != self.description.fieldsCount());

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
                                const x: Field = try Field.PgType.decodeAlloc(allocator, bytes, try self.description.fieldAt(i));
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

    pub fn errorAlloc(self: *Self, allocator: Allocator) !?ErrorResponse {
        if (self.queryerror == null) return null;
        return ErrorResponse.decodeAlloc(self.queryerror.msg, allocator);
    }
};
