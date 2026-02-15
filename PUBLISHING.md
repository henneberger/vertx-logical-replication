# Publishing

## Maven Central via GitHub Actions

This repository publishes to Maven Central using:

- workflow: `.github/workflows/release-central.yml`
- Maven profile: `central-release` in the root `pom.xml`
- Sonatype Central Publisher plugin

## One-time setup required

1. Sonatype Central account and namespace:
   - Create/sign in at `https://central.sonatype.com/`.
   - Verify control of the `dev.henneberger` namespace.
2. Generate a Central publishing token in Sonatype Central.
3. Create/export a GPG key used for artifact signing.
4. Add these GitHub repository secrets:
   - `MAVEN_CENTRAL_USERNAME`
   - `MAVEN_CENTRAL_TOKEN`
   - `MAVEN_GPG_PRIVATE_KEY` (ASCII-armored private key)
   - `MAVEN_GPG_PASSPHRASE`

## Release checklist

1. Set project version to a non-`SNAPSHOT` release in `pom.xml`.
2. Verify project metadata is present and correct (`licenses`, `developers`, `scm`, `url`).
3. Run full verification locally:
   ```bash
   mvn clean verify
   ```
4. Merge release changes to `main`.
5. Trigger publish either by:
   - publishing a GitHub Release targeting `main`, or
   - running `Release to Maven Central` via `workflow_dispatch`.

The workflow fails if `project.version` is still `-SNAPSHOT`.

## Produced artifacts

- Main jar
- Sources jar
- Javadocs jar
- `.asc` signatures for published artifacts
