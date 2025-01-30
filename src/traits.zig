pub fn isPgTypeDecoder(comptime T: type) bool {
    const tid = @typeInfo(T);
    return (tid == .Struct or tid == .Enum or tid == .Union) and @hasDecl(T, "PgType") and @hasDecl(T.PgType, "decodeAlloc");
}

pub fn hasInternalEncoder(comptime T: type) bool {
    const tid = @typeInfo(T);
    return (tid == .Struct or tid == .Enum or tid == .Union) and @hasDecl(T, "encode");
}
