# Releasing

Releases are cut by running the `release` workflow
(`.github/workflows/release.yml`) by hand. It builds, publishes and tags
everything; you never create the tag or the GitHub release yourself.

## Cut a release

1. Make sure `main` is green in CI and contains everything you want to ship.
2. Choose the version: `vMAJOR.MINOR.PATCH` (e.g. `v0.2.0`). Add a suffix such
   as `-rc.1` for a prerelease. See [Versioning](#versioning).
3. Run the workflow:
   - **GitHub UI:** *Actions* → *release* → *Run workflow*, leave *Use
     workflow from* on `main`, enter the version, and click *Run workflow*.
   - **CLI:**

     ```bash
     gh workflow run release.yml --ref main -f version=v0.2.0
     ```

4. Watch the run (*Actions* tab, or `gh run watch`). When all three jobs are green,
   the release is at `https://github.com/<owner>/<repo>/releases/tag/v0.2.0`.

The workflow creates the tag on the commit the run was started from (the head
of the branch selected in *Use workflow from*). Run it from `main`; releasing
from another branch tags a commit that `main` may never contain.

## What the workflow does

1. **Checks the version** matches `v1.2.3` or `v1.2.3-suffix`, and that no
   release for it has been published yet, and fails otherwise, before anything
   is built.
2. **Builds and pushes the image** (`image` job) for `linux/amd64` and
   `linux/arm64` to `ghcr.io/<owner>/<repo>`, with zstd layers, a BuildKit SBOM
   and provenance attestation, and generates an SPDX SBOM of it
   (`qbo-bq-image.spdx.json`). Tags for `v1.2.3`: `1.2.3`, `1.2` and `1` (plus
   `latest`). A prerelease is pushed only under its full version
   (`1.2.3-rc.1`).
3. **Builds the CLI binaries** (`binaries` job, in parallel):
   `qbo-bq-{linux,darwin}-{amd64,arm64}` and `SHA256SUMS`.
4. **Publishes the GitHub release** (`release` job, once both are green) and
   the `v1.2.3` tag, with notes generated from the merged pull requests, marked
   as a prerelease when the version has a suffix, and the SBOM and binaries
   attached.

Releases in this repository are immutable: once published, a release's assets
can't be added, replaced or removed, and its tag can't move. That is why every
asset is attached in the single step that publishes the release. If the image
or binaries job fails, no release is published, so you can re-run the failed
jobs.

The version is compiled into the binaries and the image; `qbo-bq version`
prints it.

## Versioning

- Follow [semantic versioning](https://semver.org). While the major version is
  `0`, bump the minor version for anything that changes behaviour or
  configuration, and the patch version for fixes.
- Call out in the release notes anything an operator must act on: new or
  renamed environment variables (`internal/config`, `terraform/`), Terraform
  module changes, and backfills (`qbo-bq sync --full --entities …`) needed
  for newly mapped fields.
- A version can be released only once. Running the workflow again for a
  published version fails at the version check, before any image tag is
  overwritten. To fix a bad release, release the next patch version.

## Other ways to trigger it

The workflow also runs when a `v*` tag is pushed (`git push origin v0.2.0`).
That produces the same result; the manual run above is the preferred path
because it validates the version before creating the tag.

Don't publish a release from the GitHub UI: it is immutable as soon as it is
published, so the workflow it triggers can't attach the SBOM and binaries and
fails.

## After releasing

- **Verify the image** (optional):

  ```bash
  syft ghcr.io/<owner>/<repo>:0.2.0
  ```

  ```bash
  grype ghcr.io/<owner>/<repo>:0.2.0
  ```

  The main module shows up with the release version, and Go stdlib with the
  toolchain version. Dependency vulnerabilities are tracked by Dependabot,
  not by the release workflow.
- **Deploy:** Cloud Run cannot pull from GHCR. Copy the image into Artifact
  Registry (or use an Artifact Registry remote repository for `ghcr.io`) and
  update the Terraform module's `image`; see
  [Deploying to Google Cloud](README.md#deploying-to-google-cloud).
- **Package visibility:** the GHCR package is private while the repository
  is. Pulling it then needs a token with `read:packages`; to make it public,
  use the package's *Package settings*.
