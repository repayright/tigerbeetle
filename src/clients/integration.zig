//
// This script wraps ./run_with_tb.zig with setup necessary for the
// sample code in src/clients/$lang/samples/$sample. It builds and
// runs one sample, failing if the sample fails. It could have been a
// bash script except for that it works on Windows as well.
//
// Example: (run from the repo root)
//   ./zig/zig build client_integration -- --language java --sample basic
//

const std = @import("std");
const builtin = @import("builtin");

const java_docs = @import("./java/docs.zig").JavaDocs;
const dotnet_docs = @import("./dotnet/docs.zig").DotnetDocs;
const go_docs = @import("./go/docs.zig").GoDocs;
const node_docs = @import("./node/docs.zig").NodeDocs;
const Docs = @import("./docs_types.zig").Docs;
const TmpDir = @import("./shutil.zig").TmpDir;
const git_root = @import("./shutil.zig").git_root;
const run_shell = @import("./shutil.zig").run_shell;
const prepare_directory = @import("./docs_generate.zig").prepare_directory;
const integrate = @import("./docs_generate.zig").integrate;

fn error_main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    const root = try git_root(&arena);

    std.debug.print("Moved to git root: {s}\n", .{root});
    try std.os.chdir(root);

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip the first argument of the exe.
    std.debug.assert(args.skip());

    var language: ?Docs = null;
    var sample: []const u8 = "";
    var keep_tmp = false;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--language")) {
            const next = args.next() orelse "";

            if (std.mem.eql(u8, next, "java")) {
                language = java_docs;
            } else if (std.mem.eql(u8, next, "node")) {
                language = node_docs;
            } else if (std.mem.eql(u8, next, "go")) {
                language = go_docs;
            } else if (std.mem.eql(u8, next, "dotnet")) {
                language = dotnet_docs;
            } else {
                std.debug.print("Unknown language: {s}.\n", .{next});
                return error.UnknownLanguage;
            }
        }

        if (std.mem.eql(u8, arg, "--keep-tmp")) {
            keep_tmp = true;
        }

        if (std.mem.eql(u8, arg, "--sample")) {
            const next = args.next() orelse "";

            sample = next;
        }
    }

    if (sample.len == 0) {
        std.debug.print("--sample not set.\n", .{});
        return error.SampleNotSet;
    }

    if (language == null) {
        std.debug.print("--language not set.\n", .{});
        return error.LanguageNotSet;
    }

    var tmp_copy = try TmpDir.init(&arena);

    // Copy the sample into a temporary directory.
    try run_shell(
        &arena,
        try std.fmt.allocPrint(
            arena.allocator(),
            // Works on Windows as well.
            "cp -r {s}/* {s}/",
            .{
                // Full path of sample directory.
                try std.fmt.allocPrint(allocator, "{s}/src/clients/{s}/samples/{s}", .{
                    root,
                    language.?.directory,
                    sample,
                }),
                tmp_copy.path,
            },
        ),
    );

    try prepare_directory(&arena, language.?, tmp_copy.path);

    try integrate(&arena, language.?, tmp_copy.path, true);

    if (!keep_tmp) {
        tmp_copy.deinit();
    }
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
