#!/usr/bin/env python3
"""A13 static scan of the public surface: built packages and public docs/examples.

Fails when a package or public file contains upstream provider brands/hosts, credentials,
private keys, internal-only material, or local absolute paths. Usage:

    python tools/scan_public.py [--terms <private rules file>] <file-or-dir> [...]

Archives (.jar/.whl/.zip/.tar.gz) are scanned member by member. Exit code 1 on findings.
"""
import io
import os
import re
import sys
import tarfile
import zipfile

RULES = [
    ("private-key", re.compile(r"-----BEGIN (RSA |EC |ENCRYPTED )?PRIVATE KEY-----")),
    ("fixture-secret", re.compile(r"fixture-key-SECRET")),
    ("bearer-or-key-literal", re.compile(r"(?i)(accessKey|api[_-]?key|authorization)\s*[:=]\s*[\"'](?!\.\.\.|<|\$|your)[A-Za-z0-9_\-]{16,}[\"']")),
    ("signed-url", re.compile(r"(?i)[?&](X-Amz-Signature|Signature|sig|token|ticket)=[A-Za-z0-9%_\-]{12,}")),
    ("local-path", re.compile(r"(?i)\b[A-Z]:\\(Users|Tools|Program Files)\\|\b[A-Z]:\\[^\s\\]*[^\x00-\x7f]|/home/[a-z]+/")),
]
# Upstream provider names, hosts, auth headers and internal workspace names are deliberately NOT written in this public file.
# Supply them from a file kept outside the repository: --terms <file> or MYVOCAL_SCAN_TERMS=<file>,
# one "rule<TAB>regex" per line (case-insensitive; lines starting with # are ignored).


def load_terms(path):
    rules = []
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.rstrip("\r\n")
            if not raw.strip() or raw.lstrip().startswith("#"):
                continue
            rule, rx = raw.split("\t", 1)
            rules.append((rule.strip(), re.compile(rx, re.I)))
    return rules
TEXT_EXT = (".java", ".go", ".py", ".pyi", ".md", ".mdx", ".txt", ".json", ".xml", ".pom", ".toml", ".cfg", ".mod",
            ".sum", ".properties", ".mf", ".in", ".yaml", ".yml", ".ps1", ".sh", ".typed", "")


# The fake key planted by the protocol fixture (used to prove that SDKs never print a key) may
# appear only in the fixture tooling itself, never in packages, examples or docs.
FIXTURE_SECRET_HOME = ("tools/scan_public.py", "tools/fixture/server.py", "fixtures/scenarios.json")


def scan_text(name, data, findings):
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return
    norm = name.replace("\\", "/")
    for rule, rx in RULES:
        if rule == "fixture-secret" and norm.endswith(FIXTURE_SECRET_HOME) and "!" not in norm:
            continue
        for m in rx.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            findings.append("%s:%d: %s: %r" % (name, line, rule, m.group(0)[:60]))


def is_text(name):
    base = name.rsplit("/", 1)[-1]
    ext = os.path.splitext(base)[1].lower()
    return ext in TEXT_EXT or base in ("LICENSE", "NOTICE", "PKG-INFO", "METADATA", "RECORD", "WHEEL")


def scan_archive(path, findings):
    if path.endswith((".jar", ".whl", ".zip")):
        with zipfile.ZipFile(path) as z:
            for n in z.namelist():
                if n.endswith((".jar", ".zip")):
                    scan_archive_bytes(path + "!" + n, z.read(n), findings)
                elif is_text(n):
                    scan_text(path + "!" + n, z.read(n), findings)
                elif n.endswith(".class"):
                    scan_text(path + "!" + n, z.read(n).decode("latin-1").encode("utf-8"), findings)
    else:
        with tarfile.open(path) as t:
            for m in t.getmembers():
                if m.isfile() and is_text(m.name):
                    scan_text(path + "!" + m.name, t.extractfile(m).read(), findings)


def scan_archive_bytes(name, data, findings):
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for n in z.namelist():
            if is_text(n):
                scan_text(name + "!" + n, z.read(n), findings)


def main(args):
    findings, scanned = [], 0
    terms = os.environ.get("MYVOCAL_SCAN_TERMS")
    paths = []
    it = iter(args)
    for a in it:
        if a == "--terms":
            terms = next(it)
        else:
            paths.append(a)
    if terms:
        RULES.extend(load_terms(terms))
        print("private term rules loaded: %d" % (len(RULES) - 5))
    else:
        print("WARNING: no private term file (--terms / MYVOCAL_SCAN_TERMS); upstream-name rules not applied")
    for p in paths:
        if os.path.isdir(p):
            for root, dirs, files in os.walk(p):
                dirs[:] = [d for d in dirs if d not in (".git", "target", "dist", "build", "__pycache__", "node_modules")]
                for f in files:
                    fp = os.path.join(root, f)
                    if is_text(f):
                        with open(fp, "rb") as fh:
                            scan_text(fp, fh.read(), findings)
                        scanned += 1
        elif p.endswith((".jar", ".whl", ".zip", ".tar.gz", ".tgz")):
            scan_archive(p, findings)
            scanned += 1
        else:
            with open(p, "rb") as fh:
                scan_text(p, fh.read(), findings)
            scanned += 1
    for f in findings:
        print(f)
    print("scanned %d inputs, %d findings" % (scanned, len(findings)))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
