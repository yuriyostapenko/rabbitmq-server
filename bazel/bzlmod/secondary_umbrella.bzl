load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

ADD_PLUGINS_DIR_BUILD_FILE = """set -euo pipefail

cat << EOF > plugins/BUILD.bazel
load("@rules_pkg//:pkg.bzl", "pkg_zip")

pkg_zip(
    name = "inet_tcp_proxy_dist_ez",
    testonly = True,
    package_dir = "inet_tcp_proxy_dist/ebin",
    srcs = [
        "@erlang_packages//inet_tcp_proxy_dist",
    ],
    package_file_name = "inet_tcp_proxy_dist-0.1.0.ez",
    visibility = ["//visibility:public"],
)

filegroup(
    name = "standard_plugins",
    srcs = glob(["**/*"]),
    visibility = ["//visibility:public"],
)
EOF
"""

def secondary_umbrella():
    http_archive(
        name = "rabbitmq-server-generic-unix-3.12",
        build_file = "@//:BUILD.package_generic_unix",
        patch_cmds = [ADD_PLUGINS_DIR_BUILD_FILE],
        strip_prefix = "rabbitmq_server-3.12.6",
        # This file is produced just in time by the test-mixed-versions.yaml GitHub Actions workflow.
        urls = [
            "https://rabbitmq-github-actions.s3.eu-west-1.amazonaws.com/secondary-umbrellas/rbe-25_3/package-generic-unix-for-mixed-version-testing-v3.12.6.tar.xz",
        ],
    )
