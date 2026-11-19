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

4. Watch the run (*Actions* tab, or `gh run watch`). When both jobs are green,
   the release is at `https://github.com/<owner>/<repo>/releases/tag/v0.2.0`.

The workflow creates the tag on the commit the run was started from (the head
of the branch selected in *Use workflow from*). Run it from `main`; releasing
from another branch tags a commit that `main` may never contain.

## What the workflow does

1. **Checks the version** matches `v1.2.3` or `v1.2.3-suffix` and fails
   otherwise, before anything is built.
2. **Builds and pushes the image** for `linux/amd64` and `linux/arm64` to
   `ghcr.io/<owner>/<repo>`, with zstd layers, a BuildKit SBOM and provenance
   attestation. Tags for `v1.2.3`: `1.2.3`, `1.2` and `1` (plus `latest`).
   A prerelease is pushed only under its full version (`1.2.3-rc.1`).
3. **Creates the GitHub release** and the `v1.2.3` tag, with notes generated
   from the merged pull requests, marked as a prerelease when the version has
   a suffix, and an SPDX SBOM of the image attached (`qbo-bq-image.spdx.json`).
4. **Attaches the CLI binaries**: `qbo-bq-{linux,darwin}-{amd64,arm64}` and
   `SHA256SUMS`.

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
- Don't re-run the workflow for a version that was already released: the tag
  stays where it is while the image tags and release assets get overwritten.
  To fix a bad release, release the next patch version.

## Other ways to trigger it

The workflow also runs when a `v*` tag is pushed (`git push origin v0.2.0`) or
a release is published from the GitHub UI with a new tag. Both produce the
same result; the manual run above is the preferred path because it validates
the version before creating the tag.

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
