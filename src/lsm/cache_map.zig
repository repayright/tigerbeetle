const std = @import("std");
const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const assert = std.debug.assert;

const SetAssociativeCache = @import("set_associative_cache.zig").SetAssociativeCache;
const ScopeCloseMode = @import("tree.zig").ScopeCloseMode;

/// The CacheMap needs to stay consistent until data has left the immutable table and
/// been persisted to level 0.
const max_ops = 3;

/// A CacheMap is a hybrid between our SetAssociativeCache and a HashMap. The SetAssociativeCache
/// sits on top and absorbs the majority of read / write requests. Below that, lives a HashMap.
/// Should an insert() cause an eviction (which can happen either because the Key is the same,
/// or because our Way is full), the evicted value is caught and put in the HashMap.
///
/// Cache invalidation for the HashMap is then handled by `compact`.
pub fn CacheMap(
    comptime Key: type,
    comptime Value: type,
    comptime key_from_value: fn (*const Value) callconv(.Inline) Key,
    comptime hash: fn (Key) callconv(.Inline) u64,
    comptime equal: fn (Key, Key) callconv(.Inline) bool,
    comptime HashMapContextValue: type,
    comptime tombstone_from_key: fn (Key) callconv(.Inline) Value,
    comptime tombstone: fn (*const Value) callconv(.Inline) bool,
) type {
    const Cache = SetAssociativeCache(
        Key,
        Value,
        key_from_value,
        hash,
        equal,
        .{},
    );

    const load_factor = 50;
    // TODO: Should be stdx when merged
    const Map = stdx.HashMapUnmanaged(
        Value,
        void,
        HashMapContextValue,
        load_factor,
    );

    return struct {
        const Self = @This();

        pub const Cache = Cache;
        pub const Map = Map;

        pub const CacheMapOptions = struct {
            cache_value_count_max: u32,
            map_value_count_max: u32,
            name: []const u8,
        };

        cache: Cache,
        map: Map,
        swap_map: Map,

        scope_is_active: bool = false,
        scope_map: Map,

        op: u64 = 0,
        ops_keys: [max_ops][]Key,
        op_keys_count: [max_ops]u64,

        last_upsert_caused_eviction: bool = undefined,

        pub fn init(allocator: std.mem.Allocator, options: CacheMapOptions) !Self {
            var cache: Cache = try Cache.init(
                allocator,
                options.cache_value_count_max,
                .{ .name = options.name },
            );
            errdefer cache.deinit(allocator);

            var map: Map = .{};
            try map.ensureTotalCapacity(allocator, options.map_value_count_max);
            errdefer map.deinit(allocator);

            var swap_map: Map = .{};
            try swap_map.ensureTotalCapacity(allocator, options.map_value_count_max);
            errdefer swap_map.deinit(allocator);

            // Scopes are limited to a single beat, so the maximum number of entries in our
            // scope_map is options.map_value_count_max divided by lsm_batch_multiple.
            // TODO: Don't like pulling in constants and making this non-pure
            std.log.info("Hmmm: {}", .{options.map_value_count_max});
            const scope_map_capacity = 20000; //@divExact(
            // options.map_value_count_max,
            // constants.lsm_batch_multiple,
            // );
            var scope_map: Map = .{};
            try scope_map.ensureTotalCapacity(allocator, scope_map_capacity);
            errdefer scope_map.deinit(allocator);

            var ops_keys: [max_ops][]Key = undefined;
            var op_keys_count: [max_ops]u64 = undefined;

            for (ops_keys) |*op_keys, i| {
                op_keys.* = try allocator.alloc(Key, options.map_value_count_max);
                op_keys_count[i] = 0;
            }

            return Self{
                .cache = cache,
                .map = map,
                .swap_map = swap_map,
                .scope_map = scope_map,
                .ops_keys = ops_keys,
                .op_keys_count = op_keys_count,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.map.deinit(allocator);
            self.scope_map.deinit(allocator);
            self.cache.deinit(allocator);

            for (self.ops_keys) |op_keys| {
                allocator.free(op_keys);
            }
        }

        // TODO: Profile me
        pub fn has(self: *Self, key: Key) bool {
            return self.cache.get_index(key) != null or self.map.getKeyPtr(tombstone_from_key(key)) != null;
        }

        // TODO: Profile me
        pub inline fn get(self: *Self, key: Key) ?*Value {
            return self.cache.get(key) orelse self.map.getKeyPtr(tombstone_from_key(key));
        }

        // TODO: Profile me
        pub fn upsert(self: *Self, value: *const Value) void {
            self.last_upsert_caused_eviction = false;
            _ = self.cache.upsert_index(value, on_eviction);

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

            if (self.scope_is_active and !self.last_upsert_caused_eviction) {
                _ = self.scope_map.putAssumeCapacity(tombstone_from_key(key_from_value(value)), {});
            }
        }

        fn on_eviction(cache: *Cache, value: *const Value, updated: bool) void {
            var self = @fieldParentPtr(Self, "cache", cache);
            if (updated) {
                // Case 1
                if (self.scope_is_active) {
                    _ = self.scope_map.putAssumeCapacity(value.*, {});
                }
            } else {
                // Case 2
                if (self.scope_is_active) {
                    _ = self.scope_map.putAssumeCapacity(value.*, {});
                } else {
                    _ = self.map.putAssumeCapacity(value.*, {});
                    var op_keys = self.ops_keys[self.op % self.ops_keys.len];
                    var op_keys_count = self.op_keys_count[self.op % self.ops_keys.len];

                    const key = key_from_value(value);
                    op_keys[op_keys_count] = key;
                    self.op_keys_count[self.op % self.ops_keys.len] += 1;
                }
            }
        }

        // TODO: Profile me
        pub fn remove(self: *Self, key: Key) void {
            // TODO: Do we want to assert we actually removed something?
            const maybe_removed = self.cache.remove(key);

            if (maybe_removed) |removed| {
                if (self.scope_is_active) {
                    _ = self.scope_map.putAssumeCapacity(removed, {});
                }
            } else {
                if (self.scope_is_active) {
                    var maybe_map_removed = self.map.getKey(tombstone_from_key(key));
                    if (maybe_map_removed) |map_removed| {
                        _ = self.scope_map.putAssumeCapacity(map_removed, {});
                    }
                }

                _ = self.map.remove(tombstone_from_key(key));
            }
        }

        /// Start a new scope. Within a scope, changes can be commited
        /// or rolled back. Only one scope can be active at a time.
        pub fn scope_open(self: *Self) void {
            assert(!self.scope_is_active);
            self.scope_is_active = true;
        }

        pub fn scope_close(self: *Self, data: ScopeCloseMode) void {
            assert(self.scope_is_active);

            // We don't need to do anything to persist a scope; we can just drop it
            // and clear the underlying scope map
            if (data == .persist) {
                self.scope_is_active = false;
                self.scope_map.clearRetainingCapacity();
                return;
            }

            // NB To deactivate the scope before iterating and calling insert again
            // TODO: Check the interaction of this with our other map and evictions too....
            self.scope_is_active = false;
            var iterator = self.scope_map.keyIterator();

            while (iterator.next()) |value| {
                // The value in scope_map is what the value in our object cache was originally.
                if (tombstone(value)) {
                    // Reverting an insert consists of a .remove call. The value in here will be a tombstone indicating
                    // the original value didn't exist.
                    self.remove(key_from_value(value));
                } else {
                    // Reverting an update or delete consist of an insert to the original value
                    self.upsert(value);
                }
            }

            self.scope_map.clearRetainingCapacity();
        }

        /// Remove any entries in our map that are older than `op`
        pub fn compact(self: *Self, op: u64) void {
            assert(!self.scope_is_active);

            var op_keys = self.ops_keys[op % self.ops_keys.len];
            var op_keys_count = self.op_keys_count[self.op % self.ops_keys.len];

            for (op_keys[0..op_keys_count]) |key| {
                _ = self.map.remove(tombstone_from_key(key));
            }

            if (self.op % self.ops_keys.len == 0) {
                self.swap_map.clearRetainingCapacity();
                var it = self.map.keyIterator();
                while (it.next()) |key| {
                    self.swap_map.putAssumeCapacity(key.*, {});
                }
                const old_map = self.map;
                self.map = self.swap_map;
                self.swap_map = old_map;
            }

            self.op_keys_count[self.op % self.ops_keys.len] = 0;
        }

        pub fn reset(self: *Self) void {
            self.cache.reset();
            self.map.clearRetainingCapacity();

            for (self.op_keys_count) |*op_key_count| {
                op_key_count.* = 0;
            }
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
    );

    const allocator = testing.allocator;

    var cache_map = try TestCacheMap.init(allocator, 2048, 32);
    defer cache_map.deinit(allocator);

    cache_map.upsert(&.{ .key = 1, .value = 1, .tombstone = false });
    assert(std.meta.eql(cache_map.get(1).?.*, .{ .key = 1, .value = 1, .tombstone = false }));

    // Test scope persisting
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 2, .value = 2, .tombstone = false });
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2, .tombstone = false }));
    cache_map.scope_close(.persist);
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2, .tombstone = false }));

    // Test scope discard on updates
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 2, .value = 22, .tombstone = false });
    cache_map.upsert(&.{ .key = 2, .value = 222, .tombstone = false });
    cache_map.upsert(&.{ .key = 2, .value = 2222, .tombstone = false });
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2222, .tombstone = false }));
    cache_map.scope_close(.discard);
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2, .tombstone = false }));

    // Test scope discard on inserts
    cache_map.scope_open();
    cache_map.upsert(&.{ .key = 3, .value = 3, .tombstone = false });
    assert(std.meta.eql(cache_map.get(3).?.*, .{ .key = 3, .value = 3, .tombstone = false }));
    cache_map.upsert(&.{ .key = 3, .value = 33, .tombstone = false });
    assert(std.meta.eql(cache_map.get(3).?.*, .{ .key = 3, .value = 33, .tombstone = false }));
    cache_map.scope_close(.discard);
    assert(!cache_map.has(3));
    assert(cache_map.get(3) == null);

    // Test scope discard on removes
    cache_map.scope_open();
    cache_map.remove(2);
    assert(!cache_map.has(2));
    assert(cache_map.get(2) == null);
    cache_map.scope_close(.discard);
    assert(std.meta.eql(cache_map.get(2).?.*, .{ .key = 2, .value = 2, .tombstone = false }));
}
