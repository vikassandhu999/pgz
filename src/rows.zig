const std = @import("std");

const Allocator = std.mem.Allocator;
const Message = @import("./protocol/message.zig").Message;

const FieldDescription = struct {
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
    _a: Allocator,

    const Self = @This();

    pub fn init(msg: Message, allocator: Allocator) !Rows {
        var rows = Rows{
            ._a = allocator,
        };
        var varmsg = msg;
        var reader = varmsg.reader();

        _ = try reader.readByte();
        _ = try reader.readInt32();

        const fieldscount: usize = @intCast(try reader.readInt16());

        if (fieldscount != 0) {
            rows.fields = try allocator.alloc(FieldDescription, fieldscount);
        }

        for (0..fieldscount) |i| {
            rows.fields[i].name = try reader.readString();
            rows.fields[i].tableoid = try reader.readInt32();
            rows.fields[i].tableattributenumber = try reader.readInt16();
            rows.fields[i].datatypeoid = try reader.readInt32();
            rows.fields[i].datatypesize = try reader.readInt16();
            rows.fields[i].typemodifier = try reader.readInt32();
            rows.fields[i].format = try reader.readInt16();
        }

        return rows;
    }

    pub fn deinit(self: *Self) void {
        if (self.fields.len > 0) {
            self._a.free(self.fields);
        }
    }

    pub fn print(self: *Self) !void {
        const writer = std.debug;

        writer.print("Row Description ({d} columns):\n", .{self.fields.len});

        if (self.fields.len == 0) {
            writer.print("No columns defined\n", .{});
            return;
        }

        // Table header
        writer.print(
            \\┌──────────────┬───────────┬────────────┬────────────┬──────────────┬──────────────┬────────┐
            \\│ Name         │ Table OID │ Column #   │ Type ID    │ Type Length  │ Attr Modifier│ Format │
            \\├──────────────┼───────────┼────────────┼────────────┼──────────────┼──────────────┼────────┤
        , .{});

        // Table rows
        for (self.fields) |col| {
            writer.print(
                \\
                \\│ {s:<12} │ {d:>9} │ {d:>10} │ {d:>10} │ {d:>12} │ {d:>12} │ {d:>6} │
                \\├──────────────┼───────────┼────────────┼────────────┼──────────────┼──────────────┼────────┤
            , .{
                col.name,
                col.tableoid,
                col.tableattributenumber,
                col.datatypeoid,
                col.datatypesize,
                col.typemodifier,
                col.format,
            });
        }

        // Replace last divider with footer
        writer.print(
            \\
            \\└──────────────┴───────────┴────────────┴────────────┴──────────────┴──────────────┴────────┘
            \\
        , .{});
    }
};
