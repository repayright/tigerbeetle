const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const KWayMergeIterator = @import("k_way_merge.zig").KWayMergeIterator;

/// Range queries are not supported on the TableMutable, it must first be made immutable.
pub fn TableMutableType(comptime Table: type, comptime tree_name: [:0]const u8, comptime ObjectsCache: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    // const tombstone_from_key = Table.tombstone_from_key;
    // const tombstone = Table.tombstone;
    const key_count_max = Table.value_count_max;
    // const usage = Table.usage;
    _ = tree_name;

    // TODO: should be real value of key_index and deal with padding etc
    const k_sort_interval = @divExact(key_count_max, constants.lsm_batch_multiple);

    return struct {
        const TableMutable = @This();

        keys: []Key,
        key_index: u32,

        objects_cache: *ObjectsCache,

        // Used when k-way-merging.
        streams: [constants.lsm_batch_multiple][]Key = undefined,

        inline fn key_is_value(value: *const Key) Key {
            return value.*;
        }

        const KWay = KWayMergeIterator(
            TableMutable,
            Key,
            Key,
            key_is_value,
            Table.compare_keys,
            constants.lsm_batch_multiple,
            stream_peek,
            stream_pop,
            stream_precedence,
        );

        pub fn init(
            allocator: mem.Allocator,
            objects_cache: *ObjectsCache,
        ) !TableMutable {
            var keys = try allocator.alloc(Key, key_count_max);
            errdefer allocator.free(table.keys);

            return TableMutable{
                .keys = keys,
                .key_index = 0,
                .objects_cache = objects_cache,
            };
        }

        fn stream_peek(context: *const TableMutable, stream_index: u32) error{ Empty, Drained }!Key {
            const stream = context.streams[stream_index];
            if (stream.len == 0) {
                return error.Empty;
            }
            return stream[0];
        }

        fn stream_pop(context: *TableMutable, stream_index: u32) Key {
            const stream = context.streams[stream_index];
            context.streams[stream_index] = stream[1..];
            return stream[0];
        }

        fn stream_precedence(context: *const TableMutable, a: u32, b: u32) bool {
            _ = context;

            // Higher streams have higher precedence
            return a > b;
        }

        pub fn deinit(table: *TableMutable, allocator: mem.Allocator) void {
            allocator.free(table.keys);
        }

        pub fn put(table: *TableMutable, value: *const Value) void {
            table.keys[table.key_index] = key_from_value(value);

            // If we're doing a put that's a multiple of key_count_max / lsm_batch_multiple
            // Sort the range. This will get k-way-merged later by sort_into_values_and_clear

            // Todo if this goes below
            table.key_index += 1;

            // TODO Faster way of doing this check?
            if (table.key_index % k_sort_interval == 0) { // and table.key_index > 0
                const start_index = table.key_index - k_sort_interval;
                const end_index = table.key_index;

                var chunk = table.keys[start_index..end_index];
                std.sort.sort(Key, chunk, {}, sort_keys_in_ascending_order);

                // std.log.info("key_index is: {} sorting from {} to {}", .{table.key_index, start_index, end_index});
                // std.log.info("key_index is: {}", .{table.key_index});
                // std.log.info("Sorted is: {} {} {}", .{chunk[0], chunk[1], chunk[2]});
            }

        }

        pub fn remove(table: *TableMutable, value: *const Value) void {
            _ = table;
            _ = value;
            // table.key_count += 1;
            // switch (usage) {
            //     .secondary_index => {
            //         const existing = table.values.fetchRemove(value.*);
            //         if (existing) |kv| {
            //             // The previous operation on this key then it must have been a put.
            //             // The put and remove cancel out.
            //             assert(!tombstone(&kv.key));
            //         } else {
            //             // If the put is already on-disk, then we need to follow it with a tombstone.
            //             // The put and the tombstone may cancel each other out later during compaction.
            //             table.values.putAssumeCapacityNoClobber(tombstone_from_key(key_from_value(value)), {});
            //         }
            //     },
            //     .general => {
            //         // If the key is already present in the hash map, the old key will not be overwritten
            //         // by the new one if using e.g. putAssumeCapacity(). Instead we must use the lower
            //         // level getOrPut() API and manually overwrite the old key.
            //         const upsert = table.values.getOrPutAssumeCapacity(value.*);
            //         upsert.key_ptr.* = tombstone_from_key(key_from_value(value));
            //     },
            // }

            // assert(table.values.count() <= key_count_max);
        }

        pub fn clear(table: *TableMutable) void {
            table.key_index = 0;
        }

        pub fn count(table: *const TableMutable) u32 {
            return table.key_index;
        }

        /// The returned slice is invalidated whenever this is called for any tree.
        pub fn sort_into_values_and_clear(
            table: *TableMutable,
            values_max: []Value,
        ) []const Value {
            assert(table.count() > 0);
            assert(table.count() <= key_count_max);
            assert(table.count() <= values_max.len);
            assert(values_max.len == key_count_max);

            // Set up our streams
            var i: u32 = 0;
            while (i < constants.lsm_batch_multiple) {
                const start_idx = k_sort_interval*i;
                const end_idx = std.math.min(k_sort_interval*(i+1), table.key_index);
                // std.log.info("Stream {} runs from {} to {}", .{i, start_idx, end_idx});
                table.streams[i] = table.keys[start_idx..end_idx];

                defer i += 1;

                if (end_idx == table.key_index) {
                    // Sort the last stream - it wouldn't have been done in the put
                    std.sort.sort(Key, table.streams[i], {}, sort_keys_in_ascending_order);

                    break;
                }
            }

            var kway = KWay.init(table, i, .ascending);
            var popped: u32 = 0;
            while (kway.pop()) |key| {
                values_max[popped] = table.objects_cache.get(key).?;
                popped += 1;
                // try actual.append(value);
            }

            table.clear();
            assert(table.count() == 0);

            return values_max[0..popped];
            // std.log.info("Just popped: {}, should be popping {}", .{popped, table.key_index});

            // var i: usize = 0;
            // var it = table.values.keyIterator();
            // while (it.next()) |value| : (i += 1) {
            //     values_max[i] = value.*;
            // }

            // const values = values_max[0..i];
            // assert(values.len == table.count());
            // std.sort.sort(Value, values, {}, sort_values_by_key_in_ascending_order);

        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }

        fn sort_keys_in_ascending_order(_: void, a: Key, b: Key) bool {
            return compare_keys(a, b) == .lt;
        }
    };
}
