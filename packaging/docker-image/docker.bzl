load("@rules_oci//oci:defs.bzl", "oci_image", "oci_tarball")
load("@rules_erlang//:util.bzl", "path_join")

def docker_run_and_commit_layer(
        name = None,
        commands = None,
        exec_properties = None,
        image = None,
        tags = [],
    ):

    oci_tarball_name = "%s_image_tarball" % name

    oci_tarball(
        name = oci_tarball_name,
        image = image,
        repo_tags = [
            "%s:latest" % name,
        ],
        tags = tags,
    )

    run_and_commit_name = "%s_run_and_commit" % name

    native.genrule(
        name = run_and_commit_name,
        srcs = [":%s" % oci_tarball_name],
        outs = ["%s.tar" % name],
        cmd = """set -euo pipefail

DOCKER=$${{DOCKER:-docker}}

trap 'catch $$?' EXIT
catch() {{
    $$DOCKER stop {name}
    $$DOCKER rm {name}
    $$DOCKER rmi {name}:latest
}}

set -x

$$DOCKER load --input $(locations :{oci_tarball_name})
$$DOCKER create --name {name} {name}:latest /bin/bash -c "{commands}"
$$DOCKER start -a {name}
$$DOCKER save --output $(location :{name}.tar) {name}
""".format(
    oci_tarball_name = oci_tarball_name,
    name = name,
    commands = "; ".join(commands),
),
        exec_properties = exec_properties,
        tags = tags,
    )

    oci_image(
        name = name,
        base = image,
        tars = [
            ":%s" % run_and_commit_name
        ],
        tags = tags,
    )

def docker_buildx_build(
        name = None,
        platforms = ["linux/amd64"],
        base = None,
        context = {},
        build_args = {},
        exec_properties = None,
        tags = []):

    oci_tarball_name = "%s_base_tarball" % name

    base_tag = "%s-base:latest" % name

    oci_tarball(
        name = oci_tarball_name,
        image = base,
        repo_tags = [base_tag],
        tags = tags,
    )

    buildx_build_name = "%s_buildx_build" % name

    _docker_buildx_build(
        name = buildx_build_name,
        base_tag = base_tag,
        base_tarball = oci_tarball_name,
        platforms = platforms,
        context = context,
        build_args = build_args,
        out = "%s.tar" % name,
    )

    # setup_context_commands = [
    #     'cp $(location {}) "$CONTEXT/{}"'.format(k, v)
    #     for k, v in context.items()
    # ]

#     full_name = Label(name).package + "/" + name

#     native.genrule(
#         name = buildx_build_name,
#         srcs = [":%s" % oci_tarball_name] + context.keys(),
#         outs = ["%s.tar" % name],
#         cmd = """set -euo pipefail

# CONTEXT="$(mktemp -d)"

# {setup_context_commands}

# FROM=$(docker load --input $(locations :{oci_tarball_name}))

# docker buildx build \\
#     --platforms={platforms} \\
#     --tag {full_name}:latest \\
#     --build-arg FROM=$FROM {extra_build_arg_args} \\
#     "$CONTEXT"

# docker save --output $(location :{out}) {full_name}:latest
# """.format(
#     setup_context_commands = "\n".join(setup_context_commands),
#     oci_tarball_name = oci_tarball_name,
#     platforms = ",".join(platforms),
#     full_name = full_name,
#     out = "%s.tar" % name,
#     extra_build_arg_args = [
#         "\\\n    --build-arg {}={}".format(k, v)
#         for k, v in build_args.items()
#     ],
# ),
#         exec_properties = exec_properties,
#         tags = tags,
#     )

    oci_image(
        name = name,
        base = base,
        tars = [
            ":%s" % buildx_build_name
        ],
        tags = tags,
    )

def _docker_buildx_build_impl(ctx):
    context_dir = ctx.actions.declare_directory("%s_context" % ctx.attr.name)

    setup_context_commands = [
        "set -euxo pipefail",
    ]

    for k, v in ctx.attr.context.items():
        for f in k[DefaultInfo].files.to_list():
            setup_context_commands.append(
                "cp {} {}".format(f.path, path_join(context_dir.path, v))
            )

    ctx.actions.run_shell(
        inputs = ctx.files.context,
        outputs = [context_dir],
        command = "\n".join(setup_context_commands)
    )

    full_name = path_join(ctx.label.package, ctx.label.name)

    ctx.actions.run_shell(
        inputs = [ctx.file.base_tarball, context_dir],
        outputs = [ctx.outputs.out],
        command = """set -euxo pipefail

docker load --input "{base_tarball}"
docker images

docker buildx create --name {builder_name} --use
docker buildx build \\
    --platform={platforms} \\
    --tag {full_name}:latest \\
    --build-arg FROM={base_tag} {extra_build_arg_args} \\
    {context_dir}

docker save --output $(location :{out}) {full_name}:latest

exit 1
""".format(
            base_tarball = ctx.file.base_tarball.path,
            builder_name = full_name.replace("/", "_"),
            platforms = ",".join(ctx.attr.platforms),
            full_name = full_name,
            base_tag = ctx.attr.base_tag,
            extra_build_arg_args = "".join([
                "\\\n    --build-arg {}={}".format(k, v)
                for k, v in ctx.attr.build_args.items()
            ]),
            context_dir = context_dir.path,
            out = ctx.outputs.out.path,
        )
    )

_docker_buildx_build = rule(
    implementation = _docker_buildx_build_impl,
    attrs = {
        "base_tag": attr.string(
            mandatory = True),
        "base_tarball": attr.label(
            mandatory = True,
            allow_single_file = True,
        ),
        "platforms": attr.string_list(
            mandatory = True),
        "context": attr.label_keyed_string_dict(
            allow_files = True,
        ),
        "build_args": attr.string_dict(),
        "out": attr.output(),
    },
)
