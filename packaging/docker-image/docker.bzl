load("@rules_oci//oci:defs.bzl", "oci_image", "oci_tarball")

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
