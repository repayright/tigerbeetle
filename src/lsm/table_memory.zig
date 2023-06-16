const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const div_ceil = @import("../stdx.zig").div_ceil;
const binary_search = @import("binary_search.zig");
const snapshot_latest = @import("tree.zig").snapshot_latest;

pub fn TableMemoryType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const value_count_max = Table.value_count_max;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;
    const tombstone_from_key = Table.tombstone_from_key;
    const k_sort_interval = @divExact(value_count_max, constants.lsm_batch_multiple);
    _ = k_sort_interval;

    return struct {
        const TableMemory = @This();
        const OrderRequirement = enum { ordered_puts, unordered_puts };
        const Mutability = enum { mutable, immutable };

        values: []Value,
        values_scratch: []Value,
        value_index: usize,
        snapshot_min: u64,
        mutability: Mutability,
        ordering: OrderRequirement,
        flushed: bool,

        pub fn init(allocator: mem.Allocator, ordering: OrderRequirement, mutability: Mutability) !TableMemory {
            const values = try allocator.alloc(Value, value_count_max);
            errdefer allocator.free(values);

            const values_scratch = try allocator.alloc(Value, value_count_max);
            errdefer allocator.free(values_scratch);

            return TableMemory{
                .snapshot_min = undefined,
                .values = values,
                .values_scratch = values_scratch,
                .value_index = 0,
                .mutability = mutability,
                .ordering = ordering,

                // An empty table has nothing to flush
                .flushed = true,
            };
        }

        pub fn count(table: *TableMemory) usize {
            return table.value_index;
        }

        fn verify_sort(values: []Value) void {
            // if (constants.verify) {
            //     var i: usize = 1;
            //     while (i < sorted_values.len) : (i += 1) {
            //         assert(i > 0);
            //         const left_key = key_from_value(&sorted_values[i - 1]);
            //         const right_key = key_from_value(&sorted_values[i]);
            //         assert(compare_keys(left_key, right_key) == .lt);
            //     }
            // }

            var good = true;
            for (values) |value, i| {
                if (i + 1 == values.len) {
                    break;
                }
                const comp = compare_keys(key_from_value(&value), key_from_value(&values[i + 1]));
                if (comp == .gt or comp == .eq) {
                    good = false;
                    std.log.info("WTF: Comp was gt / eq!! index {} and +1: {} vs {}", .{ i, key_from_value(&value), key_from_value(&values[i + 1]) });
                    // assert(false);
                }
            }
        }

        fn merge(table: *TableMemory, start_index_1: usize, end_index_1: usize, start_index_2: usize, end_index_2: usize) void {
            var slice_1 = table.values[start_index_1..end_index_1];
            var slice_2 = table.values[start_index_2..end_index_2];

            // std.log.info("Merge kicking off with lens {} and {}", .{slice_1.len, slice_2.len});

            var i: usize = 0;
            var values_scratch = table.values_scratch;
            while (slice_1.len > 0 and slice_2.len > 0) {
                const a = slice_1[0];
                const b = slice_2[0];
                const result = compare_keys(key_from_value(&a), key_from_value(&b));
                if (result == .lt) {
                    values_scratch[i] = a;
                    slice_1 = slice_1[1..]; // Pop the first item off
                } else if (result == .eq) {
                    // If two keys are equal, the newest (slice_2) one wins, and remove the other one
                    values_scratch[i] = b;
                    slice_1 = slice_1[1..]; // Pop the first item off
                    slice_2 = slice_2[1..]; // Pop the second item off
                } else {
                    values_scratch[i] = b;
                    slice_2 = slice_2[1..]; // Pop the first item off
                }
                // TODO - check the previous value too...
                i += 1;
            }

            // Handle residue... Just need to copy to the end
            // Could optimize and do this straight to values
            if (slice_1.len > 0) {
                // std.log.info("Residue on 1", .{});
                std.mem.copy(Value, values_scratch[i .. i + slice_1.len], slice_1[0..]);
                i += slice_1.len;
            } else if (slice_2.len > 0) {
                // std.log.info("Residue on 2", .{});
                std.mem.copy(Value, values_scratch[i .. i + slice_2.len], slice_2[0..]);
                i += slice_2.len;
            }

            if (start_index_1 == 0 and end_index_2 == table.value_index) {
                // Whole copy - we can swap ptrs
                std.mem.swap([]Value, &table.values, &table.values_scratch);
            } else {
                // Worth having k real buckets and swapping ptrs vs memcpy for the smaller cases?
                std.mem.copy(Value, table.values[start_index_1..end_index_2], table.values_scratch[0..i]);
            }
        }

        /// Synchronous compact hook. Used to sort up to k_sort_interval values each compaction beat.
        pub fn compact(table: *TableMemory) void {
            // Todo make this comptime
            if (table.ordering == .ordered_puts) {
                std.log.info("We are ordered putting, not compacting", .{});
                return;
            }
        }

        // ALSO - Make put handle duplicates...
        pub fn put(table: *TableMemory, value: *const Value) void {
            assert(table.mutability == .mutable);
            assert(table.values.len == value_count_max); // Sanity check
            // std.log.info("{*}: Putting {} at index {}", .{ table, value, table.value_index });

            table.values[table.value_index] = value.*;

            // If we're doing a put that's a multiple of key_count_max / lsm_batch_multiple
            // Sort the range. This will get k-way-merged later by sort_into_values_and_clear

            // Todo if this goes below
            table.value_index += 1;

            // TODO: Move all into fn compact
            // // TODO Faster way of doing this check?
            // if (table.value_index % k_sort_interval == 0) { // and table.value_index > 0
            //     const start_index = table.value_index - k_sort_interval;
            //     const end_index = table.value_index;

            //     var chunk = table.values[start_index..end_index];
            //     std.sort.sort(Value, chunk, {}, sort_values_by_key_in_ascending_order);

            //     std.log.info("value_index is: {} sorting 1st from {} to {}, sort is good: {}", .{ table.value_index, start_index, end_index, verify_sort(chunk) });
            //     // std.log.info("value_ind ex is: {}", .{table.value_index});
            //     // std.log.info("Sorted is: {} {} {}", .{chunk[0], chunk[1], chunk[2]});
            // }

            // var hack: u32 = 2;
            // while (hack <= constants.lsm_batch_multiple) {
            //     const k_sort_interval_multiple = k_sort_interval * hack;
            //     if (table.value_index % (k_sort_interval * hack) == 0) {
            //         const start_index_1 = table.value_index - (k_sort_interval_multiple);
            //         const end_index_1 = table.value_index - @divExact(k_sort_interval_multiple, 2);

            //         const start_index_2 = end_index_1;
            //         const end_index_2 = table.value_index;

            //         table.merge(start_index_1, end_index_1, start_index_2, end_index_2);
            //         var chunk = table.values[start_index_1..end_index_2];
            //         std.log.info("value_index is: {} {}ith layer from {} to {}, {} to {}, sort is good: {}", .{ table.value_index, hack, start_index_1, end_index_1, start_index_2, end_index_2, verify_sort(chunk) });
            //     }

            //     hack *= 2;
            // }
        }

        pub fn remove(table: *TableMemory, value: *const Value) void {
            // TODO - untested, and not sure how it'll interact with everything
            // else, as well as value_count_max...
            // TODO this and put need asserts for being mutable...
            const tombstone = tombstone_from_key(key_from_value(value));
            // std.log.info("{*}: Removing {} with {}", .{ table, value, tombstone });
            table.put(&tombstone);
        }

        pub fn make_immutable(table: *TableMemory, snapshot_min: u64) void {
            // if (table.values.len > 0) {
            //     for (table.values[0..table.value_index]) |*value| {
            //         std.log.info("{*} VALUES BEFORE MAKING IMMUTABLE: {}", .{ table, value });
            //     }
            // } else {
            //     std.log.info("{*} VALUES BEFORE MAKING IMMUTABLE EMPTY", .{table});
            // }

            assert(table.mutability == .mutable);
            // assert(table.flushed == false);
            std.log.info("{*} Making immutable...", .{table});
            table.values = table.values[0..table.value_index];

            // TODO: if table.value_index not a multiple, we need to sort the last bucket...
            std.sort.sort(Value, table.values, {}, sort_values_by_key_in_ascending_order);

            // Big hack, fix this up, it should just do an incomplete merge if any
            var i: usize = 0;
            for (table.values) |*value| {
                if (value.id == 327) {
                    std.log.info("MAKE IMMUTABLE HITTING: {}", .{value});
                }

                if (i > 0 and compare_keys(key_from_value(&table.values_scratch[i - 1]), key_from_value(value)) == .eq) {
                    i -= 1;
                }

                table.values_scratch[i] = value.*;
                i += 1;
            }
            table.values_scratch = table.values_scratch[0..i];
            table.value_index = i;
            std.mem.swap([]Value, &table.values, &table.values_scratch);

            // if (table.values.len > 0) {
            //     for (table.values[0..table.value_index]) |*value| {
            //         std.log.info("{*} VALUES AFTER MAKING IMMUTABLE: {}", .{ table, value });
            //     }
            // } else {
            //     std.log.info("{*} VALUES AFTER MAKING IMMUTABLE EMPTY", .{table});
            // }

            // Gate this behind config.verify
            verify_sort(table.values);
            table.mutability = .immutable;

            // If we have no values, then we can consider ourselves flushed right away.
            table.flushed = table.value_index == 0;
            table.snapshot_min = snapshot_min;
            std.log.info("{*} Done Making immutable...", .{table});
        }

        pub fn make_mutable(table: *TableMemory) void {
            table.values = table.values.ptr[0..value_count_max];
            assert(table.values.len == value_count_max);
            std.log.info("{*}: Making mutable...", .{table});

            assert(table.mutability == .immutable);
            assert(table.flushed == true);
            table.value_index = 0;
            table.values = table.values.ptr[0..value_count_max];
            table.values_scratch = table.values_scratch.ptr[0..value_count_max];
            table.mutability = .mutable;
            table.snapshot_min = undefined;
            std.log.info("{*}: Done making mutable...", .{table});
        }

        fn sort_values_by_key_in_ascending_order(_: void, a: Value, b: Value) bool {
            return compare_keys(key_from_value(&a), key_from_value(&b)) == .lt;
        }

        pub inline fn key_min(table: *const TableMemory) Key {
            assert(table.mutability == .immutable);
            assert(table.values.len > 0);
            return key_from_value(&table.values[0]);
        }

        pub inline fn key_max(table: *const TableMemory) Key {
            assert(table.mutability == .immutable);
            assert(table.values.len > 0);
            return key_from_value(&table.values[table.values.len - 1]);
        }

        pub fn deinit(table: *TableMemory, allocator: mem.Allocator) void {
            // Reset the slices before deinit
            table.values = table.values.ptr[0..value_count_max];
            table.values_scratch = table.values_scratch.ptr[0..value_count_max];

            allocator.free(table.values);
            allocator.free(table.values_scratch);
        }

        // TODO(ifreund) This would be great to unit test.
        pub fn get(table: *TableMemory, key: Key) ?*const Value {
            // This code path should normally _never_ be taken; any lookups for table_mutable or table_immutable
            // will be caught at a Groove level by the object cache. However, it is exercised by the fuzzer,
            // and that's useful for testing our puts and all the permutations there are working.
            assert(constants.verify);

            // Since this is a binary search, we need to be sure we're operating on a fully sorted values
            // array.
            if (table.value_index > 0) {
                std.sort.sort(Value, table.values[0..table.value_index], {}, sort_values_by_key_in_ascending_order);

                // Same equality fixup hack as in make_immutable
                var i: usize = 0;
                for (table.values[0..table.value_index]) |*value| {
                    if (i > 0 and compare_keys(key_from_value(&table.values_scratch[i - 1]), key_from_value(value)) == .eq) {
                        i -= 1;
                    }

                    table.values_scratch[i] = value.*;
                    i += 1;
                }

                table.value_index = i;
                std.mem.swap([]Value, &table.values, &table.values_scratch);
            }

            if (key.id == 327 and table.mutability == .mutable) {
                if (table.values.len > 0) {
                    for (table.values[0..table.value_index]) |*value| {
                        std.log.info("{*} VALUES IN GET: {}", .{ table, value });
                    }
                } else {
                    std.log.info("{*} VALUES IN GET EMPTY", .{table});
                }
            }

            const result = binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                compare_keys,
                table.values[0..table.value_index], // TODO for immutable??
                key,
                .{},
            );
            // std.log.info("")

            if (result.exact) {
                const value = &table.values[result.index];
                if (constants.verify) assert(compare_keys(key, key_from_value(value)) == .eq);
                return value;
            }

            return null;
        }
    };
}
