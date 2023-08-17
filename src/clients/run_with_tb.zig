//
// This script spins up a TigerBeetle server on the first available
// TCP port and references a brand new data file. Then it runs the
// command passed to it and shuts down the server. It cleans up all
// resources unless told not to. It fails if the command passed to it
// fails. It could have been a bash script except for that it works on
// Windows as well.
//
// Example: (run from the repo root)
//   ./zig/zig build run_with_tb -- node myscript.js
//

const std = @import("std");
const builtin = @import("builtin");
const os = std.os;

const run = @import("./shutil.zig").run;
const run_with_env = @import("./shutil.zig").run_with_env;
const TmpDir = @import("./shutil.zig").TmpDir;
const git_root = @import("./shutil.zig").git_root;
const path_exists = @import("./shutil.zig").path_exists;
const script_filename = @import("./shutil.zig").script_filename;
const binary_filename = @import("./shutil.zig").binary_filename;
const file_or_directory_exists = @import("./shutil.zig").file_or_directory_exists;

fn free_port() !u16 {
    const address = try std.net.Address.parseIp4("127.0.0.1", 0);

    const server = try os.socket(address.any.family, os.SOCK.STREAM, os.IPPROTO.TCP);
    defer os.closeSocket(server);

    try os.bind(server, &address.any, address.getOsSockLen());

    var client_address = std.net.Address.initIp4(undefined, undefined);
    var client_address_len = client_address.getOsSockLen();
    try os.getsockname(server, &client_address.any, &client_address_len);

    return client_address.getPort();
}

pub fn run_with_tb(
    arena: *std.heap.ArenaAllocator,
    commands: []const []const u8,
    cwd: []const u8,
) !void {
    try run_many_with_tb(arena, &[_][]const []const u8{commands}, cwd);
}

pub fn run_many_with_tb(
    arena: *std.heap.ArenaAllocator,
    commands: []const []const []const u8,
    cwd: []const u8,
) !void {
    const root = try git_root(arena);

    std.debug.print("Moved to git root: {s}\n", .{root});
    try std.os.chdir(root);
    var tb_binary = try binary_filename(arena, &[_][]const u8{"tigerbeetle"});

    // Build TigerBeetle
    if (!file_or_directory_exists(tb_binary)) {
        std.debug.print("Building TigerBeetle server\n", .{});
        try run(arena, &[_][]const u8{
            try script_filename(arena, &[_][]const u8{ "scripts", "install" }),
        });
    }

    std.debug.assert(file_or_directory_exists(tb_binary));

    var tmpdir = try TmpDir.init(arena);
    defer tmpdir.deinit();
    const wrk_dir = tmpdir.path;

    const data_file = try std.fmt.allocPrint(
        arena.allocator(),
        "{s}/0_0.tigerbeetle",
        .{wrk_dir},
    );

    std.debug.print("Formatting data file: {s}\n", .{data_file});
    _ = try run(arena, &[_][]const u8{
        tb_binary,
        "format",
        "--cluster=0",
        "--replica=0",
        "--replica-count=1",
        data_file,
    });

    const port = try free_port();

    const start_args = &[_][]const u8{
        tb_binary,
        "start",
        try std.fmt.allocPrint(arena.allocator(), "--addresses={}", .{port}),
        "--cache-grid=128MB",
        data_file,
    };
    std.debug.print("Starting TigerBeetle server: {s}\n", .{start_args});
    var cp = std.ChildProcess.init(
        start_args,
        arena.allocator(),
    );
    defer _ = cp.kill() catch {};
    try cp.spawn();

    try std.os.chdir(cwd);

    for (commands) |command_argv| {
        try run_with_env(arena, command_argv, &[_][]const u8{
            "TB_ADDRESS",
            try std.fmt.allocPrint(arena.allocator(), "{}", .{port}),
        });
    }
}

fn error_main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const cwd = std.process.getEnvVarOwned(allocator, "R_CWD") catch ".";

    var collected_args = std.ArrayList([]const u8).init(allocator);
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip first arg, this process's name
    std.debug.assert(args.skip());
    while (args.next()) |arg| {
        try collected_args.append(arg);
    }

    try run_with_tb(&arena, collected_args.items, cwd);
}

// Returning errors in main produces useless traces, at least for some
// known errors. But using errors allows defers to run. So wrap the
// main but don't pass the error back to main. Just exit(1) on
// failure.
pub fn main() !void {
    if (error_main()) {
        // fine
    } else |err| switch (err) {
        error.RunCommandFailed => std.os.exit(1),
        else => return err,
    }
}
