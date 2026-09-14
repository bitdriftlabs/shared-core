load("@rules_shell//shell:sh_binary.bzl", "sh_binary")
load("//tools/lint:linters.bzl", "rust_binary")

def fuzz_binary(name, fuzz_runtime_data, **kwargs):
    data = kwargs.pop("data", [])
    rust_binary(
        name = name + "_binary",
        data = data + fuzz_runtime_data,
        **kwargs
    )
    sh_binary(
        name = name,
        srcs = ["scripts/run_bazel_fuzzer.sh"],
        data = [":" + name + "_binary"] + fuzz_runtime_data,
        tags = ["manual"],
        deps = ["@bazel_tools//tools/bash/runfiles"],
    )
