# Publishing

## Release checklist

1. Update `pom.xml` version from `-SNAPSHOT` to a release version.
2. Ensure `README.adoc`, `url`, and `scm` fields are up to date.
3. Run full verification:
   ```bash
   mvn clean verify
   ```
4. Publish artifacts to your configured repository manager.
5. Tag the release in git and push.

## Produced artifacts

- Main jar
- Sources jar
- Javadocs jar
