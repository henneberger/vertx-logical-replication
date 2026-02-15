# Publishing

## Maven Central via GitHub Actions + Maven Release Plugin

This repository publishes to Maven Central using:

- workflow: `.github/workflows/release-central.yml` (manual trigger)
- `maven-release-plugin` (`release:prepare`, `release:perform`)
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

1. Ensure `main` has the desired next `-SNAPSHOT` version (for example `0.3.1-SNAPSHOT`).
2. Verify project metadata is present and correct (`licenses`, `developers`, `scm`, `url`).
3. Run full verification locally:
   ```bash
   mvn clean verify
   ```
4. Trigger `Release to Maven Central` in GitHub Actions (`workflow_dispatch`).
5. The workflow runs `release:prepare` (creates release commit + tag) and `release:perform` (deploys signed artifacts to Central), then commits the next development `-SNAPSHOT` version back to `main`.

## Produced artifacts

- Main jar
- Sources jar
- Javadocs jar
- `.asc` signatures for published artifacts
