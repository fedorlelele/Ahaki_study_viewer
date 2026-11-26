"""Produce a signed ASV release archive and its signed Sparkle appcast."""

import argparse
import plistlib
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

from build_app import MAC, build, run, sparkle_root


ACCOUNT = "ASV-Mac-Updates"
REPOSITORY = "https://github.com/fedorlelele/Ahaki_study_viewer"


def prepare(version, identity, notary):
    if notary and not identity:
        raise ValueError("公証には --sign-identity も指定してください。")
    output = MAC / "dist"
    output.mkdir(parents=True, exist_ok=True)
    release = output / f"mac-v{version}"
    if release.exists():
        raise FileExistsError(f"同じ版の配布ファイルがすでにあります。 {release}")

    with tempfile.TemporaryDirectory(dir=output, prefix=".asv-release-") as temporary:
        staging = Path(temporary)
        app = build(version, universal=True, identity=identity, destination=staging)
        info = plistlib.loads((app / "Contents/Info.plist").read_bytes())
        tools = sparkle_root() / "bin"
        public_key = run(tools / "generate_keys", "--account", ACCOUNT, "-p",
                         capture_output=True, text=True).stdout.strip()
        if public_key != info["SUPublicEDKey"]:
            raise ValueError("このMacの更新署名鍵と、アプリに組み込んだ公開鍵が一致しません。")

        if notary:
            submission = staging / "notarize.zip"
            run("ditto", "-c", "-k", "--keepParent", app, submission)
            run("xcrun", "notarytool", "submit", submission, "--keychain-profile", notary, "--wait")
            run("xcrun", "stapler", "staple", app)
            run("xcrun", "stapler", "validate", app)
            run("spctl", "--assess", "--type", "execute", app)

        assets = staging / "assets"
        assets.mkdir()
        archive = assets / f"ASV-{version}.zip"
        run("ditto", "-c", "-k", "--keepParent", app, archive)
        url_prefix = f"{REPOSITORY}/releases/download/mac-v{version}/"
        run(tools / "generate_appcast", "--account", ACCOUNT, "--maximum-deltas", "0",
            "--download-url-prefix", url_prefix, assets)
        feed = assets / "appcast.xml"
        enclosure = ET.parse(feed).find("./channel/item/enclosure")
        if enclosure is None or enclosure.get("url") != url_prefix + archive.name:
            raise ValueError("生成した更新情報の配布URLが一致しません。")
        signature = enclosure.attrib["{http://www.andymatuschak.org/xml-namespaces/sparkle}edSignature"]
        run(tools / "sign_update", "--account", ACCOUNT, "--verify", feed)
        run(tools / "sign_update", "--account", ACCOUNT, "--verify", archive, signature)
        assets.rename(release)
    print(f"配布用のZIPと署名済みappcast.xmlを作成しました。 {release}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="release version, e.g. 0.1.1")
    parser.add_argument("--sign-identity", help="Developer ID Application identity")
    parser.add_argument("--notary-profile", help="notarytool Keychain profile")
    args = parser.parse_args()
    prepare(args.version, args.sign_identity, args.notary_profile)
