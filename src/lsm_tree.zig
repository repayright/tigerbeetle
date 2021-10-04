const std = @import("std");
const mem = std.mem;
const vsr = @import("vsr.zig");

// StateMachine:
//
// /// state machine will pass this on to all object stores
// /// Read I/O only
// pub fn read(batch, callback) void
//
// /// write the ops in batch to the memtable/objcache, previously called commit()
// pub fn write(batch) void
//
// /// Flush in memory state to disk, preform merges, etc
// /// Only function that triggers Write I/O in LSMs, as well as some Read
// /// Make as incremental as possible, don't block the main thread, avoid high latency/spikes
// pub fn flush(callback) void
//
// /// Write manifest info for all object stores into buffer
// pub fn encode_superblock(buffer) void
//
// /// Restore all in-memory state from the superblock data
// pub fn decode_superblock(buffer) void
//

pub const CompositeKey = extern struct {
    const tombstone_bit = 1 << 63;

    pub const Value = extern struct {
        secondary: u128,
        /// The most significant bit indicates if the value is a tombstone
        timestamp: u64,

        comptime {
            assert(@sizeOf(Value) == 24);
            assert(@alignOf(Value) == 16);
        }
    };

    secondary: u128,
    /// The most significant bit must be unset as it is used to indicate a tombstone
    timestamp: u64,

    comptime {
        assert(@sizeOf(CompositeKey) == 24);
        assert(@alignOf(CompositeKey) == 16);
    }

    pub fn compare_keys(a: CompositeKey, b: CompositeKey) std.math.Order {
        if (a.secondary < b.secondary) {
            return .lt;
        } else if (a.secondary > b.secondary) {
            return .gt;
        } else if (a.timestamp < b.timestamp) {
            return .lt;
        } else if (a.timestamp > b.timestamp) {
            return .gt;
        } else {
            return .eq;
        }
    }

    pub fn key_from_value(value: Value) CompositeKey {
        return .{
            .secondary = value.secondary,
            .timestamp = @truncate(u63, key.timestamp),
        };
    }

    pub fn tombstone(value: Value) bool {
        return value.timestamp & tombstone_bit != 0;
    }

    pub fn tombstone_from_key(key: CompositeKey) Value {
        return .{
            .secondary = key.secondary,
            .timestamp = key.timestamp | tombstone_bit,
        };
    }
};

pub const TableFreeSet = struct {
    /// Bits set indicate free tables
    free: std.bit_set.DynamicBitSetUnmanaged,

    pub fn init(allocator: *std.mem.Allocator, count: usize) !TableFreeSet {
        return TableFreeSet{
            .free = try std.bit_set.DynamicBitSetUnmanaged.initFull(count, allocator),
        };
    }

    pub fn deinit(set: *TableFreeSet, allocator: *std.mem.Allocator) void {
        set.free.deinit(allocator);
    }

    pub fn acquire(set: *TableFreeSet) ?u32 {
        const table = set.free.findFirstSet() orelse return null;
        set.free.unset(table);
        return @intCast(u32, table);
    }

    pub fn release(set: *TableFreeSet, table: u32) void {
        assert(!set.free.isSet(table));
        set.free.set(table);
    }
};

// vsr.zig
pub const SuperBlock = packed struct {
    // IDEA: to reduce the size of the superblock we could make the manifest use
    // half disk sectors instead.
    pub const Manifest = packed struct {
        /// Hash chained checksum of manifest sectors stored outside the superblock.
        /// On startup, all sectors of the manifest are read in from disk and the checksum
        /// of each is calculated and chained together to produce this value. On writing a
        /// new manifest sector, we calculate the checksum of that sector and combine it
        /// with the current value of this checksum to obtain the new value.
        parent_checksum: u128,
        offset: u64,
        sectors: u32,
        /// This is stored in the superblock so that we can
        /// append new table metadata to the same sector without
        /// copy on write.
        tail: [config.sector_size]u8,
    };

    checksum: u128,
    vsr_committed_log_offset: u64,
    /// Consider replacing with a timestamp
    parent: u128,
    client_table: [config.clients_max]ClientTableEntry,

    manifests: [config.lsm_trees]Manifest,
    snapshot_manifests: [config.lsm_trees]Manifest,
    /// Timestamp of 0 indicates that the snapshot slot is free
    snapshot_timestamps: [config.lsm_snapshots_max]u64,
    snapshot_last_used: [config.lsm_snapshots_max]u64,
};

// vsr.zig
pub const ClientTableEntry = packed struct {
    message_checksum: u128,
    message_offset: u64,
    session: u64,
};

/// Limited to message_size_max in size
///
/// (128 bytes data * 8 bits per byte + 9.04 bits per key + 1) / 8 bits per byte =
/// 129.13 bytes data + filter per key/value pair
///
/// The size of the index is determined only by the number of pages in the
/// table, not by the number of objects per page.
///
/// At the start of each page there is a tombstone_count value, which is
/// the count of removed/dead/tombstone keys in that page. When reading the page,
/// the implementation should first read N - tombstone_count values from the page
/// where N is the total number of values that fit in the page. Then if the key
/// is not found, tombstone_count keys should be read from the end of the page.
pub const Table = packed struct {
    // Contains the maximum size of the table including the header
    // Use one of the header fields for the table id
    header: vr.Header,
    // filter (fixed size)
    // index (fixed size)
    // data (append up to maximum size)
};

pub const Forest = struct {
    transfers_lsm: TransfersLsm,
    transfers_indexes_lsm: TransfersIndexesLsm,
    // ...
};

const LsmTreeOptions = struct {
    tables_max: u32,
};

// const TransfersLsm = LsmTree(u64, Transfer, compare, key_from_value, storage);
// const TransfersIndexesLsm = LsmTree(TransferCompositeKey, TransferCompositeKey, compare, key_from_value, storage);

pub fn LsmTree(
    comptime Key: type,
    comptime Value: type,
    comptime compare_keys: fn (Key, Key) std.math.Order,
    comptime key_from_value: fn (Value) Key,
    comptime tombstone: fn (Value) bool,
    comptime tombstone_from_key: fn (Key) Value,
    comptime Storage: type,
) type {
    return struct {
        const Self = @This();

        // To obtain the checksums for a manifest, divide size by message_size_max
        // using ceiling division to determine the number of checksums.
        // The checksum slices for all manifests are stored in order in manifest_checksums.
        pub const Manifest = struct {
            /// 4MiB table
            ///
            /// 32_768 bytes transfers
            /// 65_536 bytes bloom filter
            /// 16_384 bytes index
            ///
            /// First 128 bytes of the table are a VSR protocol header for a repair message.
            /// This data is filled in on writing the table so that we don't
            /// need to do any extra work before sending the message on repair.
            pub const Table = packed struct {
                checksum: u128,
                id: u32,
                /// Size in disk sectors
                sectors: u32,
                timestamp: u64,
                key_min: Key,
                key_max: Key,

                comptime {
                    assert(@sizeOf(Table) == 32 + @sizeOf(Key) * 2);
                    assert(@alignOf(Table) == 16);
                }
            };

            superblock: SuperBlock.Manifest,
            tables: []Table,
        };

        // Point queries go through the object cache instead of directly accessing this table.
        // Range queries are not supported on MemTables, they must instead be made immutable.
        pub const MutableTable = struct {
            const ValuesContext = struct {
                pub fn eql(a: Value, b: Value) bool {
                    return compare_keys(key_from_value(a), key_from_value(b)) == .eq;
                }
                pub fn hash(value: Value) u64 {
                    const key = key_from_value(value);
                    return std.hash_map.getAutoHashFn(Key, ValuesContext)(key);
                }
            };
            const Values = std.HashMapUnmanaged(Value, void, ValuesContext, 50);

            values: Values = .{},

            pub fn init(allocator: *std.mem.Allocator, size: usize) !MutableTable {
                var table: MutableTable = .{};
                try table.values.ensureTotalCapacity(size);

                return table;
            }

            /// Add the given value to the table
            pub fn put(table: *MutableTable, value: Value) void {
                table.values.putAssumeCapacity(value, {});
            }

            pub fn remove(table: *MutableTable, key: Key) void {
                table.values.putAssumeCapacity(tombstone_from_key(key), {});
            }

            pub fn get(table: *MutableTable, key: Key) ?Value {
                return table.values.get(tombstone_from_key(key));
            }
        };

        pub const ImmutableTable = struct {
            /// header + index + filter size
            const pages_offset = config.sector_size;

            // The actual data to be written to disk, all other fields are slices into this buffer
            buffer: []align(config.sector_size) const u8,

            // contains checksum, id, count and timestamp
            header: *const vsr.Header,
            key_min: Key,
            key_max: Key,

            index: []const Key,
            //filter: *BinaryFuseFilter(u8),
            values: []const Value, // sorted

            pub fn create(
                mutable: *const MutableTable,
                buffer: []align(config.sector_size) u8,
            ) ImmutableTable {
                const values_size = mutable.values.count() * @sizeOf(Value);
                const values = mem.bytesAsSlice(Value, buffer[pages_offset..][0..values_size]);
                var i: usize = 0;
                var it = mutable.values.keyIterator();
                while (it.next()) |value| : (i += 1) {
                    values[i] = value.*;
                }

                const less_than = struct {
                    pub fn less_than(_: void, a: Value, b: Value) bool {
                        return std.math.order(key_from_value(a), key_from_value(b)) == .lt;
                    }
                }.less_than;
                std.sort.insertionSort(Value, values, {}, less_than);

                // page size = 7
                // value size = 3
                //
                // steps:
                // XXX XXX X|XX XXX XX|X XXX XXX|
                // XXX XXX X|XX XXX XX|X XXX ___|XXX
                // XXX XXX X|XX XXX __|XXX XXX _|XXX
                // XXX XXX _|XXX XXX _|XXX XXX _|XXX
                // XXX XXX _|XXX XXX _|XXX XXX _|XXX ___ _|
                // XXX XXX X|XX XXX XX|X XXX XXX|

                const values_per_page = config.lsm_table_page_size / @sizeOf(Value);
                const full_pages = values.len / values_per_page;
                const last_page_values = values.len - full_pages * values_per_page;
            }

            pub fn get(table: *ImmutableTable, key: Key) ?Value {}
        };

        table_free_set: *TableFreeSet,
        storage: *Storage,
        options: LsmTreeOptions,

        write_transaction: WriteTransaction,

        manifest: []Manifest,

        pub fn init(
            allocator: *std.mem.Allocator,
            table_free_set: *TableFreeSet,
            storage: *Storage,
            options: LsmTreeOptions,
        ) !Self {}

        pub const Error = error{
            IO,
        };

        pub fn put(tree: *Self, value: Value) void {}

        pub fn flush(
            tree: *Self,
            callback: fn (result: Error!void) void,
        ) void {}

        // ~Special case of put()
        pub fn remove(tree: *Self, value: Value) void {}

        pub fn get(
            tree: *Self,
            /// The snapshot timestamp, if any
            snapshot: ?u64,
            key: Key,
            callback: fn (result: Error!?Value) void,
        ) void {}

        pub const RangeQuery = union(enum) {
            bounded: struct {
                start: Key,
                end: Key,
            },
            open: struct {
                start: Key,
                order: enum {
                    ascending,
                    descending,
                },
            },
        };

        pub const RangeQueryIterator = struct {
            tree: *Self,
            snapshot: ?u64,
            query: RangeQuery,

            pub fn next(callback: fn (result: Error!?Value) void) void {}
        };

        pub fn range_query(
            tree: *Self,
            /// The snapshot timestamp, if any
            snapshot: ?u64,
            query: RangeQuery,
        ) RangeQueryIterator {}
    };
}
