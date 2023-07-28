const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const div_ceil = @import("../stdx.zig").div_ceil;
const binary_search = @import("binary_search.zig");
const snapshot_latest = @import("tree.zig").snapshot_latest;
const KWayMergeIteratorType = @import("k_way_merge.zig").KWayMergeIteratorType;

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const value_count_max = Table.value_count_max;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableMemory = @This();

        pub const ValueContext = struct {
            count: usize = 0,
            count_last: u64 = 0,

            key_min: ?Key = null,
            key_max: ?Key = null,

            // TODO: Document this
            buffers: [constants.lsm_batch_multiple][]Value = undefined,
            buffers_count: u64 = 0,
        };

        const Mutability = union(enum) {
            mutable,
            immutable: struct {
                snapshot_min: u64 = undefined,

                /// An empty table has nothing to flush
                flushed: bool = true,
            },
        };

        fn stream_peek(context: *const TableMemory, stream_index: u32) error{ Empty, Drained }!Key {
            const stream = context.streams[stream_index];
            if (stream.len == 0) return error.Empty;
            return key_from_value(&stream[0]);
        }

        fn stream_pop(context: *TableMemory, stream_index: u32) Value {
            const stream = context.streams[stream_index];
            context.streams[stream_index] = stream[1..];
            return stream[0];
        }

        fn stream_precedence(context: *const TableMemory, a: u32, b: u32) bool {
            _ = context;

            // Higher streams have higher precedence. This is critical for correct
            // behavior since we're just an array and not a Map anymore. Each stream
            // represents a batch, and we need to ensure the last value is used (
            // which is taken care of by fill_immutable_values() in compaction.zig).
            // However, the TigerBeetle state machine currently doesn't rely on or
            // need this.
            return a > b;
        }

        pub const Iterator = KWayMergeIteratorType(
            TableMemory,
            Key,
            Value,
            key_from_value,
            Table.compare_keys,
            constants.lsm_batch_multiple,
            stream_peek,
            stream_pop,
            stream_precedence,
        );

        values: []Value,
        value_context: ValueContext,
        mutability: Mutability,

        // Used when k-way-merging.
        streams: [constants.lsm_batch_multiple][]Value = undefined,

        pub fn init(allocator: mem.Allocator, mutability: Mutability) !TableMemory {
            const values = try allocator.alloc(Value, value_count_max);
            errdefer allocator.free(values);

            return TableMemory{
                .values = values,
                .value_context = .{},
                .mutability = mutability,
            };
        }

        pub fn count(table: *TableMemory) usize {
            return table.value_context.count;
        }

        fn verify_sort(sorted_values: []Value, equality: enum { allow_equals, lt_only }) void {
            var i: usize = 1;
            while (i < sorted_values.len) : (i += 1) {
                assert(i > 0);
                const left_key = key_from_value(&sorted_values[i - 1]);
                const right_key = key_from_value(&sorted_values[i]);
                if (equality == .allow_equals) {
                    assert(compare_keys(left_key, right_key) != .gt);
                } else {
                    assert(compare_keys(left_key, right_key) == .lt);
                }
            }
        }

        /// Synchronous compact hook. Sorts the previous beat's worth of values.
        pub fn compact(table: *TableMemory) void {
            assert(table.mutability == .mutable);

            table.value_context.buffers[table.value_context.buffers_count] = table.values[table.value_context.count_last..table.value_context.count];
            std.sort.sort(Value, table.value_context.buffers[table.value_context.buffers_count], {}, sort_values_by_key_in_ascending_order);
            std.log.warn("Values after sort: {any}", .{table.value_context.buffers[table.value_context.buffers_count]});
            table.value_context.buffers_count += 1;

            // TODO: Get rid of count_last
            table.value_context.count_last = table.value_context.count;
        }

        pub fn put(table: *TableMemory, value: *const Value) void {
            assert(table.mutability == .mutable);
            assert(table.values.len == value_count_max); // Sanity check
            std.log.info("HACKME: Putting {} at index {}", .{ value, table.value_context.count });

            table.values[table.value_context.count] = value.*;
            table.value_context.count += 1;

            // Hmmm - less branchy way of doing this?
            // Also, need to confirm sorted setting logic
            // ALSO we actually care about sorted runs...
            const key = key_from_value(value);
            if (table.value_context.key_min) |_key_min| {
                if (compare_keys(_key_min, key) == .gt) {
                    table.value_context.key_min = key;

                    // We've got a new key_min, so we lose our sorted propery
                    // table.value_context.sorted = false;
                }
            } else {
                table.value_context.key_min = key;
            }

            if (table.value_context.key_max) |_key_max| {
                if (compare_keys(_key_max, key) == .lt) {
                    table.value_context.key_max = key;
                    // If this is a new key_max, we're good and still sorted
                }
            } else {
                table.value_context.key_max = key;
            }
        }

        pub fn iterator(table: *TableMemory) Iterator {
            // For now only; nothing really stops us, but we just need to be careful since the KWayMerge
            // iterator state will likely be invalid if a put comes in. We'd also need to ensure we
            // sort the last buffer.
            assert(table.mutability == .immutable);
            // assert(table.value_context.sorted);

            var i: u32 = 0;
            while (i < table.value_context.buffers_count) : (i += 1) {
                table.streams[i] = table.value_context.buffers[i];
            }

            return Iterator.init(table, i, .ascending);
        }

        pub fn make_immutable(table: *TableMemory, snapshot_min: u64) void {
            assert(table.mutability == .mutable);
            // assert(table.value_context.sorted);

            table.values = table.values[0..table.value_context.count];

            if (constants.verify) {
                // At this stage, we might have duplicate values in our values stream. This is _ok_
                // it's only when iterating the final output that we need to ensure the latest
                // update 'wins'
                // TODO: Fix this
                // verify_sort(table.values, .allow_equals);
            }

            // If we have no values, then we can consider ourselves flushed right away.
            table.mutability = .{ .immutable = .{ .flushed = table.value_context.count == 0, .snapshot_min = snapshot_min } };
        }

        pub fn make_mutable(table: *TableMemory) void {
            assert(table.mutability == .immutable);
            assert(table.mutability.immutable.flushed == true);

            var values_max = table.values.ptr[0..value_count_max];
            assert(values_max.len == value_count_max);

            table.* = .{
                .values = values_max,
                .value_context = .{},
                .mutability = .mutable,
            };
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }

        pub inline fn key_min(table: *const TableMemory) Key {
            assert(table.values.len > 0);
            return table.value_context.key_min.?;
        }

        pub inline fn key_max(table: *const TableMemory) Key {
            assert(table.values.len > 0);
            return table.value_context.key_max.?;
        }

        pub fn deinit(table: *TableMemory, allocator: mem.Allocator) void {
            table.values = table.values.ptr[0..value_count_max];
            allocator.free(table.values);
        }

        pub fn reset(table: *TableMemory) void {
            var values_max = table.values.ptr[0..value_count_max];
            var mutability: Mutability = if (table.mutability == .immutable) .{ .immutable = .{ .flushed = true, .snapshot_min = undefined } } else .mutable;

            table.* = .{
                .values = values_max,
                .value_context = .{},
                .mutability = mutability,
            };
        }
    };
}

const TestTable = struct {
    const Key = u32;
    const Value = struct { key: Key, value: u32, tombstone: bool };
    const value_count_max = 8;

    inline fn key_from_value(v: *const Value) u32 {
        return v.key;
    }

    inline fn compare_keys(a: Key, b: Key) math.Order {
        return math.order(a, b);
    }

    inline fn tombstone_from_key(a: Key) Value {
        return Value{ .key = a, .value = 0, .tombstone = true };
    }
};

test "table_memory: unit" {
    const testing = std.testing;
    const TableMemory = TableMemoryType(TestTable);

    const allocator = testing.allocator;
    var table_memory = try TableMemory.init(allocator, .mutable);
    defer table_memory.deinit(allocator);

    table_memory.put(&.{ .key = 1, .value = 1, .tombstone = false });
    table_memory.put(&.{ .key = 3, .value = 3, .tombstone = false });
    table_memory.put(&.{ .key = 5, .value = 5, .tombstone = false });

    assert(table_memory.count() == 3 and table_memory.value_context.count == 3);
    assert(table_memory.value_context.key_min.? == 1);
    assert(table_memory.value_context.key_max.? == 5);
    // assert(table_memory.value_context.sorted);

    table_memory.put(&.{ .key = 0, .value = 0, .tombstone = false });

    assert(table_memory.count() == 4 and table_memory.value_context.count == 4);
    assert(table_memory.value_context.key_min.? == 0);
    assert(table_memory.value_context.key_max.? == 5);
    // assert(!table_memory.value_context.sorted);

    // Our iterator will return the latest .put, even though internally we store them all.
    table_memory.put(&.{ .key = 1, .value = 11, .tombstone = false });
    table_memory.put(&.{ .key = 3, .value = 33, .tombstone = false });
    table_memory.put(&.{ .key = 3, .value = 333, .tombstone = false });
    table_memory.put(&.{ .key = 3, .value = 3333, .tombstone = false });

    table_memory.compact();
    table_memory.make_immutable(0);
    var iterator = table_memory.iterator();

    const a1 = iterator.pop_latest();
    const a2 = iterator.pop_latest();
    const a3 = iterator.pop_latest();
    const a4 = iterator.pop_latest();
    std.log.warn("Got: {}", .{a1});
    std.log.warn("Got: {}", .{a2});
    std.log.warn("Got: {}", .{a3});
    std.log.warn("Got: {}", .{a4});
    assert(std.meta.eql(a1, .{ .key = 0, .value = 0, .tombstone = false }));
    assert(std.meta.eql(a2, .{ .key = 1, .value = 11, .tombstone = false }));
    assert(std.meta.eql(a3, .{ .key = 3, .value = 3333, .tombstone = false }));
    assert(std.meta.eql(a4, .{ .key = 5, .value = 5, .tombstone = false }));

    // "Flush" and make mutable again
    table_memory.mutability.immutable.flushed = true;

    table_memory.make_mutable();
    assert(table_memory.count() == 0 and table_memory.value_context.count == 0);
    assert(table_memory.value_context.key_min == null);
    assert(table_memory.value_context.key_max == null);
    // assert(table_memory.value_context.sorted);
    assert(table_memory.mutability == .mutable);

    // TODO: Assert key_min / max tracking too.
}
