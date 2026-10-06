# Releasing

One version number covers all three SDKs. A release is made from a commit on `main` whose
local matrix passes (`tools/matrix/run-matrix.ps1`) and whose public files pass the publication scan.

1. Build the artifacts once from the release commit:
   - Java (JDK 8+): `cd java && mvn -B verify` produces the JAR, sources JAR, Javadoc JAR and
     the POM (`pom.xml`, attached as `myvocal-stt-realtime-<version>.pom`). Builds are
     reproducible (`project.build.outputTimestamp`).
   - Python: `cd python && python -m build` produces the wheel and sdist.
2. Tag the commit `v<version>` and `go/v<version>` (the Go module is in `go/`). Tags are never
   moved, deleted or re-pushed.
3. Create the GitHub Release `v<version>` with the six files and a `SHA256SUMS` file.
4. Publish the same files (no rebuild) with the manual workflows:
   - **PyPI** — `Publish Python to PyPI` (`.github/workflows/publish-pypi.yml`), Trusted
     Publishing. One-time PyPI setup: a (pending) trusted publisher for project
     `myvocal-stt-realtime`, owner `MyVocal-AI`, repository `myvocal-stt-realtime-sdks`,
     workflow `publish-pypi.yml`, environment `pypi`.
   - **Maven Central** — `Publish Java to Maven Central` (`.github/workflows/publish-central.yml`).
     One-time setup: a Central Portal account with the verified namespace `ai.myvocal`, a
     Central user token, and a GPG key whose public key is on a public keyserver. Store
     `CENTRAL_TOKEN_USERNAME`, `CENTRAL_TOKEN_PASSWORD`, `GPG_PRIVATE_KEY` and
     `GPG_PASSPHRASE` as secrets of the `maven-central` environment. The workflow signs the
     files, adds `.md5`/`.sha1`/`.sha256`/`.sha512`, uploads the bundle and waits for
     validation; with `USER_MANAGED` you confirm the publication in the Portal.
   - **Go** — nothing to upload; `proxy.golang.org` serves the module from the tag.
5. A published version is never deleted or re-uploaded. If an upload times out, check the
   registry before retrying. A defect in a published version is fixed in a new version.
