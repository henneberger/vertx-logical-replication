# Building Vert.x PostgreSQL Logical Replication

## Prerequisites

- Maven 3.8+
- JDK 11+
- Docker (for integration tests)

## Regular build

```bash
mvn clean install
```

## Run tests

```bash
mvn test
```

Run a specific test:

```bash
mvn test -Dtest=Wal2JsonParserTest
```

## Integration tests

Integration tests use Testcontainers and require Docker.
If Docker is unavailable, integration tests are skipped automatically.

## IDE import

Import the project as a Maven project.
