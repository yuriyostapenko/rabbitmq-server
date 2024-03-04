load("@bazel_skylib//lib:shell.bzl", "shell")
load(
    "@rules_erlang//:erlang_app_info.bzl",
    "ErlangAppInfo",
)
load(
    "@rules_erlang//:util.bzl",
    "path_join",
    "windows_path",
)
load(
    "@rules_erlang//private:util.bzl",
    "additional_file_dest_relative_path",
)
load(
    "//bazel/elixir:elixir_toolchain.bzl",
    "elixir_dirs",
    "erlang_dirs",
    "maybe_install_erlang",
)
load(
    ":rabbitmqctl.bzl",
    "deps_dir_contents",
)

def _impl(ctx):
    (erlang_home, _, erlang_runfiles) = erlang_dirs(ctx)
    (elixir_home, elixir_runfiles) = elixir_dirs(ctx, short_path = True)

    deps_dir = ctx.label.name + "_deps"

    deps_dir_files = deps_dir_contents(
        ctx,
        ctx.attr.deps,
        deps_dir,
    )

    for dep, app_name in ctx.attr.source_deps.items():
        for src in dep.files.to_list():
            if not src.is_directory:
                rp = additional_file_dest_relative_path(dep.label, src)
                f = ctx.actions.declare_file(path_join(
                    deps_dir,
                    app_name,
                    rp,
                ))
                ctx.actions.symlink(
                    output = f,
                    target_file = src,
                )
                deps_dir_files.append(f)

    package_dir = path_join(
        ctx.label.workspace_root,
        ctx.label.package,
    )

    precompiled_deps = " ".join([
        dep[ErlangAppInfo].app_name
        for dep in ctx.attr.deps
    ])

    if not ctx.attr.is_windows:
        output = ctx.actions.declare_file(ctx.label.name)
        script = """set -euo pipefail

{maybe_install_erlang}

if [[ "{elixir_home}" == /* ]]; then
    ABS_ELIXIR_HOME="{elixir_home}"
else
    ABS_ELIXIR_HOME=$PWD/{elixir_home}
fi

export PATH="$ABS_ELIXIR_HOME"/bin:"{erlang_home}"/bin:${{PATH}}

export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

INITIAL_DIR="$(pwd)"

if [ ! -f ${{INITIAL_DIR}}/{package_dir}/test/test_helper.exs ]; then
    echo "test_helper.exs cannot be found. 'bazel clean' might fix this."
    exit 1
fi

cp -r ${{INITIAL_DIR}}/{package_dir}/config ${{TEST_UNDECLARED_OUTPUTS_DIR}}
cp -r ${{INITIAL_DIR}}/{package_dir}/lib ${{TEST_UNDECLARED_OUTPUTS_DIR}}
cp -r ${{INITIAL_DIR}}/{package_dir}/test ${{TEST_UNDECLARED_OUTPUTS_DIR}}
cp    ${{INITIAL_DIR}}/{package_dir}/mix.exs ${{TEST_UNDECLARED_OUTPUTS_DIR}}
cp    ${{INITIAL_DIR}}/{package_dir}/.formatter.exs ${{TEST_UNDECLARED_OUTPUTS_DIR}}

cd ${{TEST_UNDECLARED_OUTPUTS_DIR}}

export IS_BAZEL=true
export HOME=${{PWD}}
export DEPS_DIR=$TEST_SRCDIR/$TEST_WORKSPACE/{package_dir}/{deps_dir}
export MIX_ENV=test
export ERL_COMPILER_OPTIONS=deterministic
for archive in {archives}; do
    "${{ABS_ELIXIR_HOME}}"/bin/mix archive.install --force $INITIAL_DIR/$archive
done
set -x
"${{ABS_ELIXIR_HOME}}"/bin/mix deps.compile
"${{ABS_ELIXIR_HOME}}"/bin/mix compile --warnings-as-errors
""".format(
            maybe_install_erlang = maybe_install_erlang(ctx, short_path = True),
            erlang_home = erlang_home,
            elixir_home = elixir_home,
            package_dir = package_dir,
            deps_dir = deps_dir,
            archives = " ".join([shell.quote(a.short_path) for a in ctx.files.archives]),
            precompiled_deps = precompiled_deps,
        )
    else:
        output = ctx.actions.declare_file(ctx.label.name + ".bat")
        script = """@echo off
:: set LANG="en_US.UTF-8"
:: set LC_ALL="en_US.UTF-8"

set PATH="{elixir_home}\\bin";"{erlang_home}\\bin";%PATH%

set OUTPUTS_DIR=%TEST_UNDECLARED_OUTPUTS_DIR:/=\\%

:: robocopy exits non-zero when files are copied successfully
:: https://social.msdn.microsoft.com/Forums/en-US/d599833c-dcea-46f5-85e9-b1f028a0fefe/robocopy-exits-with-error-code-1?forum=tfsbuild
robocopy {package_dir}\\config %OUTPUTS_DIR%\\config /E /NFL /NDL /NJH /NJS /nc /ns /np
robocopy {package_dir}\\lib %OUTPUTS_DIR%\\lib /E /NFL /NDL /NJH /NJS /nc /ns /np
robocopy {package_dir}\\test %OUTPUTS_DIR%\\test /E /NFL /NDL /NJH /NJS /nc /ns /np
copy {package_dir}\\mix.exs %OUTPUTS_DIR%\\mix.exs || goto :error
copy {package_dir}\\.formatter.exs %OUTPUTS_DIR%\\.formatter.exs || goto :error

cd %OUTPUTS_DIR% || goto :error

set DEPS_DIR=%TEST_SRCDIR%/%TEST_WORKSPACE%/{package_dir}/{deps_dir}
set DEPS_DIR=%DEPS_DIR:/=\\%
set ERL_COMPILER_OPTIONS=deterministic
set MIX_ENV=test
for %%a in ({archives}) do (
    set ARCH=%TEST_SRCDIR%/%TEST_WORKSPACE%/%%a
    set ARCH=%ARCH:/=\\%
    "{elixir_home}\\bin\\mix" archive.install --force %ARCH% || goto :error
)
"{elixir_home}\\bin\\mix" deps.compile || goto :error
"{elixir_home}\\bin\\mix" compile --warnings-as-errors || goto :error
goto :EOF
:error
exit /b 1
""".format(
            erlang_home = windows_path(erlang_home),
            elixir_home = windows_path(elixir_home),
            package_dir = windows_path(ctx.label.package),
            deps_dir = deps_dir,
            archives = " ".join([shell.quote(a.short_path) for a in ctx.files.archives]),
            precompiled_deps = precompiled_deps,
        )

    ctx.actions.write(
        output = output,
        content = script,
    )

    runfiles = ctx.runfiles(
        files = ctx.files.srcs + ctx.files.data + ctx.files.archives,
        transitive_files = depset(deps_dir_files),
    ).merge_all([
        erlang_runfiles,
        elixir_runfiles,
    ])

    return [DefaultInfo(
        runfiles = runfiles,
        executable = output,
    )]

rabbitmqctl_compile_warnings_as_errors_private_test = rule(
    implementation = _impl,
    attrs = {
        "is_windows": attr.bool(mandatory = True),
        "srcs": attr.label_list(allow_files = [".ex", ".exs"]),
        "data": attr.label_list(allow_files = True),
        "deps": attr.label_list(providers = [ErlangAppInfo]),
        "archives": attr.label_list(
            allow_files = [".ez"],
        ),
        "source_deps": attr.label_keyed_string_dict(),
    },
    toolchains = [
        "//bazel/elixir:toolchain_type",
    ],
    test = True,
)

def rabbitmqctl_compile_warnings_as_errors_test(**kwargs):
    rabbitmqctl_compile_warnings_as_errors_private_test(
        is_windows = select({
            "@bazel_tools//src/conditions:host_windows": True,
            "//conditions:default": False,
        }),
        **kwargs
    )
