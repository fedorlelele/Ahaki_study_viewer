"""Build ASV.app with its offline questions and embedded Sparkle framework."""

import argparse
import fcntl
import os
import plistlib
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from package_data import package


MAC = Path(__file__).resolve().parents[1]
INFO = MAC / "Packaging" / "Info.plist"
SCRATCH = Path(os.environ.get("ASV_SWIFT_SCRATCH_PATH", MAC / ".build")).resolve()


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def sparkle_root():
    roots = list((SCRATCH / "artifacts").glob("*/Sparkle/Sparkle.xcframework"))
    if len(roots) != 1:
        raise RuntimeError("SwiftPMが展開したSparkleパッケージを特定できません。")
    return roots[0].parent


def sign(app, identity):
    if identity:
        codesign = ["codesign", "--force", "--timestamp", "--options", "runtime", "--sign", identity]
        framework = app / "Contents" / "Frameworks" / "Sparkle.framework"
        helpers = framework / "Versions" / "B"
        run(*codesign, helpers / "XPCServices/Installer.xpc")
        run(*codesign, "--preserve-metadata=entitlements", helpers / "XPCServices/Downloader.xpc")
        run(*codesign, helpers / "Autoupdate")
        run(*codesign, helpers / "Updater.app")
        run(*codesign, framework)
        run(*codesign, app)
    else:
        run("codesign", "--force", "--sign", "-", app)
    run("codesign", "--verify", "--deep", "--strict", app)


def build(version=None, universal=False, identity=None, destination=None):
    info = plistlib.loads(INFO.read_bytes())
    if version is not None:
        if not re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", version):
            raise ValueError("バージョンには 0.1.1 のような数字3組を指定してください。")
        info.update(CFBundleVersion=version, CFBundleShortVersionString=version)

    products = []
    # Intel-only Python installations on Apple Silicon can launch Swift under Rosetta.
    has_arm64 = subprocess.run(["/usr/bin/arch", "-arm64", "/usr/bin/true"],
                               capture_output=True).returncode == 0
    host = "arm64" if has_arm64 else "x86_64"
    for arch in (["arm64", "x86_64"] if universal else [host]):
        command = ["/usr/bin/arch", f"-{host}", "/usr/bin/xcrun", "swift", "build",
                   "--package-path", MAC, "--scratch-path", SCRATCH,
                   "-c", "release", "--arch", arch]
        if os.environ.get("ASV_SWIFT_CACHE_PATH"):
            command.extend(["--cache-path", os.environ["ASV_SWIFT_CACHE_PATH"]])
        run(*command, "--product", "ASVMac")
        directory = Path(run(*command, "--show-bin-path", capture_output=True, text=True).stdout.strip())
        products.append(directory / "ASVMac")

    output = destination or MAC / "build"
    output.mkdir(parents=True, exist_ok=True)
    result = output / "ASV.app"
    with tempfile.TemporaryDirectory(dir=output, prefix=".asv-build-") as temp:
        staging = Path(temp)
        app = staging / "ASV.app"
        contents = app / "Contents"
        resources = contents / "Resources"
        executable = contents / "MacOS" / "ASVMac"
        executable.parent.mkdir(parents=True)
        resources.mkdir()
        (contents / "Info.plist").write_bytes(plistlib.dumps(info))
        if universal:
            run("lipo", "-create", *products, "-output", executable)
        else:
            shutil.copy(products[0], executable)
        run("install_name_tool", "-add_rpath", "@executable_path/../Frameworks", executable)

        sparkle = sparkle_root()
        framework = sparkle / "Sparkle.xcframework/macos-arm64_x86_64/Sparkle.framework"
        shutil.copytree(framework, contents / "Frameworks/Sparkle.framework", symlinks=True,
                        copy_function=shutil.copy)
        shutil.copy(sparkle / "LICENSE", resources / "Sparkle-LICENSE.txt")
        shutil.copy(MAC / "Packaging/AppIcon.icns", resources / "AppIcon.icns")
        package(resources)
        sign(app, identity)

        with (output / ".asv-build.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            previous = staging / "previous.app"
            if result.exists():
                os.rename(result, previous)
            try:
                os.rename(app, result)
            except OSError:
                if previous.exists():
                    os.rename(previous, result)
                raise
    print(result, flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", help="CFBundleVersion and display version, e.g. 0.1.1")
    parser.add_argument("--universal", action="store_true", help="include Apple Silicon and Intel")
    parser.add_argument("--sign-identity", help="Developer ID Application identity in your Keychain")
    args = parser.parse_args()
    build(args.version, args.universal, args.sign_identity)
