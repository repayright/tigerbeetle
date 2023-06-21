const std = @import("std");

const stdx = @import("../stdx.zig");

const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;

/// A CacheMap is a hybrid between our SetAssociativeCache and a HashMap. The SetAssociativeCache
/// sits on top and absorbs the majority of read / write requests. Below that, lives a HashMap.
/// Should an insert() cause an eviction (which can happen either because the Key is the same,
/// or because our Way is full), the evicted value is caught and put in the HashMap.
///
/// Cache invalidation for the HashMap is then handled out of band (TODO!)
pub fn CacheMap(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime hash: fn (Key) callconv(.Inline) u64,
    comptime equal: fn (Key, Key) callconv(.Inline) bool,
    comptime HashMapContextValue: type,
    comptime name: [:0]const u8,
) type {
    const Cache = SetAssociativeCache(
        Key,
        Value,
        key_from_value,
        hash,
        equal,
        .{},
        name,
    );

    const load_factor = 50;
    // TODO: Should be stdx when merged
    const Map = std.HashMapUnmanaged(
        Value,
        void,
        HashMapContextValue,
        load_factor,
    );

    return struct {
        const Self = @This();

        pub const Cache = Cache;
        pub const Map = Map;

        cache: *Cache,
        map: Map,

        // TODO: Make these params a struct
        pub fn init(allocator: std.mem.Allocator, cache_value_count_max: u32, map_value_count_max: u32) !Self {
            var cache = try allocator.create(Cache);
            errdefer allocator.destroy(cache);

            cache.* = try Cache.init(
                allocator,
                cache_value_count_max,
            );
            errdefer cache.deinit(allocator);

            var map: Map = .{};
            try map.ensureTotalCapacity(allocator, map_value_count_max);
            errdefer map.deinit(allocator);

            return Self{
                .cache = cache,
                .map = map,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.map.deinit(allocator);
            self.cache.deinit(allocator);
            allocator.destroy(self.cache);
        }

        pub fn has(self: *Self, key: Key) bool {
            _ = key;
            _ = self;
        }

        pub fn get(self: *Self, key: Key) ?*Value {
            return self.cache.get(key);
        }

        pub fn insert(self: *Self, value: *const Value) void {
            _ = self;
            _ = value;
        }

        pub fn remove(self: *Self, key: Key) void {
            _ = self;
            _ = key;
        }
    };
}

// const _ObjectsCache = SetAssociativeCache(
//     PrimaryKey,
//     Object,
//     struct {
//         inline fn key_from_value(value: *const Object) PrimaryKey {
//             if (has_id) {
//                 return value.id;
//             } else {
//                 return value.timestamp;
//             }
//         }
//     }.key_from_value,
//     struct {
//         inline fn hash(key: PrimaryKey) u64 {
//             return stdx.hash_inline(key);
//         }
//     }.hash,
//     struct {
//         inline fn equal(a: PrimaryKey, b: PrimaryKey) bool {
//             return a == b;
//         }
//     }.equal,
//     .{},
//     @typeName(Object),
// );
