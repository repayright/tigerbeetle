const std = @import("std");
const assert = std.debug.assert;

const MessagePool = @import("../../message_pool.zig").MessagePool;
const Message = MessagePool.Message;
const Header = @import("../../vsr.zig").Header;
const ProcessType = @import("../../vsr.zig").ProcessType;

const Network = @import("network.zig").Network;

const log = std.log.scoped(.message_bus);

pub const Process = union(ProcessType) {
    replica: u8,
    client: u128,
};

pub const MessageBus = struct {
    network: *Network,
    pool: *MessagePool,

    cluster: u32,
    process: Process,
    view: u32 = 0,

    /// The callback to be called when a message is received.
    on_message_callback: fn (message_bus: *MessageBus, message: *Message) void,

    //check_send_to_replica: []const Options.CheckSendToReplica,

    pub const Options = struct {
        network: *Network,
        check_send_to_replica: []const CheckSendToReplica = &.{},

        const CheckSendToReplica = fn (replica: u8, message: *const Message) void;
    };

    pub fn init(
        _: std.mem.Allocator,
        cluster: u32,
        process: Process,
        message_pool: *MessagePool,
        on_message_callback: fn (message_bus: *MessageBus, message: *Message) void,
        options: Options,
    ) !MessageBus {
        return MessageBus{
            .network = options.network,
            .pool = message_pool,
            .cluster = cluster,
            .process = process,
            .on_message_callback = on_message_callback,
        };
    }

    /// TODO
    pub fn deinit(_: *MessageBus, _: std.mem.Allocator) void {}

    pub fn tick(_: *MessageBus) void {}

    pub fn get_message(bus: *MessageBus) *Message {
        return bus.pool.get_message();
    }

    pub fn unref(bus: *MessageBus, message: *Message) void {
        bus.pool.unref(message);
    }

    pub fn send_message_to_replica(bus: *MessageBus, replica: u8, message: *Message) void {
        if (bus.process == .replica) {
            // Messages sent by a process to itself should never be passed to the message bus.
            assert(replica != bus.process.replica);

            // Verify that the replica's advertised view never backtracks.
            if (message.header.command != .request and
                message.header.command != .prepare and
                message.header.command != .request_start_view)
            {
                bus.view = std.math.max(bus.view, message.header.view);
                assert(bus.view == message.header.view);
            }
        }

        bus.network.send_message(message, .{
            .source = bus.process,
            .target = .{ .replica = replica },
        });
    }

    /// Try to send the message to the client with the given id.
    /// If the client is not currently connected, the message is silently dropped.
    pub fn send_message_to_client(bus: *MessageBus, client_id: u128, message: *Message) void {
        assert(bus.process == .replica);

        bus.network.send_message(message, .{
            .source = bus.process,
            .target = .{ .client = client_id },
        });
    }
};
