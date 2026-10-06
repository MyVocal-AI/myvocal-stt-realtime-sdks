#!/usr/bin/env python3
"""Build a Maven Central Portal deployment bundle from already-built release files.

Input directory must contain <artifactId>-<version>.jar, -sources.jar, -javadoc.jar and .pom
(the exact files attached to the GitHub Release). The script never rebuilds them. It writes
.md5/.sha1/.sha256/.sha512 checksums and, unless --no-sign, ASCII-armoured detached GPG
signatures for each file, laid out under groupId/artifactId/version, and zips the result.

Signing uses the gpg on PATH and its default (or --key) secret key. A passphrase, if any, is
read from the GPG_PASSPHRASE environment variable and fed to gpg on stdin; it is never placed
on a command line or written to disk.
"""
import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile

NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def coords(pom_path):
    root = ET.parse(pom_path).getroot()
    def text(tag):
        el = root.find("m:" + tag, NS)
        return el.text.strip() if el is not None and el.text else None
    return text("groupId"), text("artifactId"), text("version")


def sign(path, key):
    cmd = ["gpg", "--batch", "--yes", "--armor", "--detach-sign", "--output", path + ".asc"]
    if key:
        cmd += ["--local-user", key]
    passphrase = os.environ.get("GPG_PASSPHRASE")
    if passphrase:
        cmd += ["--pinentry-mode", "loopback", "--passphrase-fd", "0"]
    cmd.append(path)
    subprocess.run(cmd, input=(passphrase or "").encode(), check=True)
    subprocess.run(["gpg", "--batch", "--verify", path + ".asc", path], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--in", dest="src", required=True, help="directory with the release files")
    ap.add_argument("--out", required=True, help="bundle zip to write")
    ap.add_argument("--key", help="GPG key id/fingerprint (default: gpg default key)")
    ap.add_argument("--no-sign", action="store_true", help="checksums only (local dry run)")
    a = ap.parse_args()

    poms = [f for f in os.listdir(a.src) if f.endswith(".pom")]
    if len(poms) != 1:
        sys.exit("expected exactly one .pom in %s, found %d" % (a.src, len(poms)))
    group, artifact, version = coords(os.path.join(a.src, poms[0]))
    if not (group and artifact and version):
        sys.exit("POM must declare groupId, artifactId and version directly")
    base = "%s-%s" % (artifact, version)
    names = [base + ".pom", base + ".jar", base + "-sources.jar", base + "-javadoc.jar"]
    missing = [n for n in names if not os.path.isfile(os.path.join(a.src, n))]
    if missing:
        sys.exit("missing release files: " + ", ".join(missing))

    with tempfile.TemporaryDirectory() as tmp:
        rel = os.path.join(*group.split("."), artifact, version)
        dest = os.path.join(tmp, rel)
        os.makedirs(dest)
        for n in names:
            p = os.path.join(dest, n)
            shutil.copyfile(os.path.join(a.src, n), p)
            data = open(p, "rb").read()
            for algo in ("md5", "sha1", "sha256", "sha512"):
                with open(p + "." + algo, "w", encoding="ascii") as fh:
                    fh.write(hashlib.new(algo, data).hexdigest())
            if not a.no_sign:
                sign(p, a.key)
        with zipfile.ZipFile(a.out, "w", zipfile.ZIP_DEFLATED) as z:
            for f in sorted(os.listdir(dest)):
                z.write(os.path.join(dest, f), os.path.join(rel, f).replace(os.sep, "/"))
        print("bundle %s: %s:%s:%s, %d entries" % (a.out, group, artifact, version,
                                                   len(os.listdir(dest))))


if __name__ == "__main__":
    main()
