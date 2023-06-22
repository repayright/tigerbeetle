const std = @import("std");

const stdx = @import("../stdx.zig");
const assert = std.debug.assert;

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
    comptime tombstone_from_key: fn (Key) callconv(.Inline) Value,
    comptime tombstone: fn (*const Value) callconv(.Inline) bool,
    comptime name: [:0]const u8,
) type {
    _ = tombstone;
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

        scope_map: Map,
        scope_is_active: bool = false,

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

            // TODO: This def doesn't need to be map_value_count_max
            var scope_map: Map = .{};
            try scope_map.ensureTotalCapacity(allocator, map_value_count_max);
            errdefer scope_map.deinit(allocator);

            return Self{
                .cache = cache,
                .map = map,
                .scope_map = scope_map,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.map.deinit(allocator);
            self.cache.deinit(allocator);
            allocator.destroy(self.cache);
            self.scope_map.deinit(allocator);
        }

        // TODO: Profile me
        pub fn has(self: *Self, key: Key) bool {
            return self.cache.get_index(key) != null or self.map.getKeyPtr(tombstone_from_key(key)) != null;
        }

        // TODO: Profile me
        pub fn get(self: *Self, key: Key) ?*Value {
            return self.cache.get(key) orelse self.map.getKeyPtr(tombstone_from_key(key));
        }

        // TODO: Profile me
        pub fn insert(self: *Self, value: *const Value) void {
            const insert_result = self.cache.insert_index(value, true);

            // When inserting into the cache, we have a few options, depending on if we evicted
            // something, and if we're running in a scope or not:
            // 1. We have evicted an item that was an exact match. This means we're doing an update.
            //    If we have an active scope, store this item in our scope_map, otherwise, we don't
            //    need to put it in our map.
            // 2. We have evicted an item that was not an exact match. This means we're doing an
            //    insert of a new value, but two keys have the same tags. If we have an active scope
            //    store this item in our scope_map, otherwise, store it in our map.
            // 3. No eviction. The value didn't exist in the cache, and it mapped to an empty way.
            //    If we have an active scope, store a tombstone in our scope_map. If we don't have
            //    an active scope, we don't need to do anything.
            if (insert_result.evicted) |evicted| {
                if (insert_result.was_in_cache) {
                    // Case 1
                    if (self.scope_is_active) {
                        _ = self.scope_map.getOrPutAssumeCapacity(evicted);
                    }
                } else {
                    // Case 2
                    if (self.scope_is_active) {
                        _ = self.scope_map.getOrPutAssumeCapacity(evicted);
                    } else {
                        _ = self.map.getOrPutAssumeCapacity(evicted);
                    }
                }
            } else if (self.scope_is_active) {
                // Case 3
                _ = self.scope_map.getOrPutAssumeCapacity(tombstone_from_key(key_from_value(value)));
            }
        }

        // TODO: Profile me
        pub fn remove(self: *Self, key: Key) void {
            // TODO: Do we want to assert we actually removed something?
            const maybe_removed = self.cache.remove(key);

            if (maybe_removed) |removed| {
                if (self.scope_is_active) {
                    _ = self.scope_map.getOrPutAssumeCapacity(removed);
                }
            } else {
                if (self.scope_is_active) {
                    var maybe_map_removed = self.map.getKey(tombstone_from_key(key));
                    if (maybe_map_removed) |map_removed| {
                        _ = self.scope_map.getOrPutAssumeCapacity(map_removed);
                    }
                }

                _ = self.map.remove(tombstone_from_key(key));
            }
        }

        /// Start a new scope. Within a scope, changes can be commited
        /// or rolled back. Only one scope can be active at a time.
        pub fn scope_start(self: *Self) void {
            assert(!self.scope_is_active);
            self.scope_is_active = true;
        }

        pub fn scope_commit(self: *Self) void {
            // We don't need to do anything to commit a scope; we can just drop it
            // and clear the underlying scope map
            assert(self.scope_is_active);
            self.scope_is_active = false;
            self.scope_map.clearRetainingCapacity();
        }

        pub fn scope_rollback(self: *Self) void {
            assert(self.scope_is_active);

            // NB To deactivate the scope before iterating and calling insert again
            // TODO: Check the interaction of this with our other map and evictions too....
            self.scope_is_active = false;

            var iterator = self.scope_map.keyIterator();
            while (iterator.next()) |value| {
                // The value in scope_map is what the value in our object cache was originally.
                if (tombstone(value)) {
                    // Reverting an insert consists of a .remove call. The value in here will be a tombstone (indicating)
                    // the original value didn't exist.
                    self.remove(key_from_value(value));
                } else {
                    // Reverting an update or delete consist of an insert to the original value
                    self.insert(value);
                }
            }

            self.scope_map.clearRetainingCapacity();
        }
    };
}

const TestTable = struct {
    const Key = u32;
    const Value = struct {
        key: Key,
        value: u32,
        tombstone: bool,
        padding: [7]u8 = undefined,
    };
    const value_count_max = 8;

    inline fn key_from_value(v: *const Value) u32 {
        return v.key;
    }

    inline fn compare_keys(a: Key, b: Key) std.math.Order {
        return std.math.order(a, b);
    }

    inline fn tombstone_from_key(a: Key) Value {
        return Value{ .key = a, .value = 0, .tombstone = true };
    }

    pub const HashMapContextValue = struct {
        pub inline fn eql(_: HashMapContextValue, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .eq;
        }

        pub inline fn hash(_: HashMapContextValue, value: Value) u64 {
            return stdx.hash_inline(key_from_value(&value));
        }
    };
};

test "cache_map: unit" {
    const testing = std.testing;
    const PrimaryKey = TestTable.Key;
    const Object = TestTable.Value;

    const TestCacheMap = CacheMap(
        PrimaryKey,
        Object,
        struct {
            inline fn key_from_value(value: *const Object) PrimaryKey {
                return value.key;
            }
        }.key_from_value,
        struct {
            inline fn hash(key: PrimaryKey) u64 {
                return stdx.hash_inline(key);
            }
        }.hash,
        struct {
            inline fn equal(a: PrimaryKey, b: PrimaryKey) bool {
                return a == b;
            }
        }.equal,
        TestTable.HashMapContextValue,
        struct {
            // NEed to make this more efficient!
            inline fn tombstone_from_key(a: PrimaryKey) Object {
                var obj: Object = undefined;
                obj.key = a;
                obj.tombstone = true;

                return obj;
            }
        }.tombstone_from_key,
        struct {
            // NEed to make this more efficient!
            inline fn tombstone(a: *const Object) bool {
                return a.tombstone;
            }
        }.tombstone,

        @typeName(Object),
    );

    const allocator = testing.allocator;

    var cache_map = try TestCacheMap.init(allocator, 2048, 32);
    defer cache_map.deinit(allocator);

    cache_map.insert(&.{ .key = 1, .value = 1, .tombstone = false });
    assert(std.meta.eql(cache_map.get(1).?.*, .{ .key = 1, .value = 1, .tombstone = false }));

    // Test scope commiting
    cache_map.scope_start();
    cache_map.insert(&.{ .key = 2, .value = 2, .tombstone = false });
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2, .tombstone = false }));
    cache_map.scope_commit();
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2, .tombstone = false }));

    // Test scope rollback on updates
    cache_map.scope_start();
    cache_map.insert(&.{ .key = 2, .value = 22, .tombstone = false });
    cache_map.insert(&.{ .key = 2, .value = 222, .tombstone = false });
    cache_map.insert(&.{ .key = 2, .value = 2222, .tombstone = false });
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2222, .tombstone = false }));
    cache_map.scope_rollback();
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2, .tombstone = false }));

    // Test scope rollback on inserts
    cache_map.scope_start();
    cache_map.insert(&.{ .key = 3, .value = 3, .tombstone = false });
    assert(std.meta.eql(cache_map.get(3).?.*, .{ .key = 3, .value = 3, .tombstone = false }));
    cache_map.insert(&.{ .key = 3, .value = 33, .tombstone = false });
    assert(std.meta.eql(cache_map.get(3).?.*, .{ .key = 3, .value = 33, .tombstone = false }));
    cache_map.scope_rollback();
    assert(!cache_map.has(3));
    assert(cache_map.get(3) == null);

    // Test scope rollback on removes
    cache_map.scope_start();
    cache_map.remove(2);
    assert(!cache_map.has(2));
    assert(cache_map.get(2) == null);
    cache_map.scope_rollback();
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2, .tombstone = false }));
}
