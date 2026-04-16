# Apache Hive MR3 Agent Guide

## Repository Overview

Apache Hive MR3 is a fork of Apache Hive adapted to run on the MR3 execution engine (Hive on MR3). It is a large Maven monorepo (~35 modules) for SQL-on-Hadoop data warehousing.

## Build Commands

### Basic Build
```bash
mvn clean install -DskipTests -Pitests
```
- Requires Java 21 (see `maven.compiler.source` in pom.xml)
- `-Pitests` enables integration tests module

### Run Checkstyle
```bash
mvn checkstyle:checkstyle -Pitests
```
- Config: `checkstyle/checkstyle.xml`

### Run Tests in a Module
```bash
mvn test -pl module-name
```

### Standalone Metastore Unit Tests
```bash
cd standalone-metastore
mvn test -pl metastore-server -Dtest.groups=org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest
```
- Uses in-memory Derby by default (configured in surefire plugin)
- Tests are in `standalone-metastore/metastore-server/src/test/java/`

### Single Test Class
```bash
mvn test -pl module -Dtest=TestClassName
```

## Key Modules

| Module | Purpose |
|--------|---------|
| `standalone-metastore` | Independent metastore (can be built separately) |
| `ql` | Query language engine (core) |
| `metastore` | Hive's original metastore module |
| `serde` | Serialization/deserialization |
| `parser` | SQL parsing (ANTLR4) |
| `service` | HiveServer2 and other services |
| `itests` | Integration tests (requires `-Pitests`) |

## Build Profiles

- `itests` - Includes integration test modules in build
- `thriftif` - Generates Thrift code (requires `thrift.home` env var pointing to Thrift installation)
- `spotbugs` - Static analysis (`mvn com.github.spotbugs:spotbugs-maven-plugin:4.8.6.6:spotbugs`)
- `dist` - Creates distribution artifacts with CycloneDX SBOM
- `windows-test` - Windows-specific test workarounds (auto-activated on Windows)

## Code Generation

- **Thrift**: Activate `thriftif` profile; requires `thrift.home` env var set to Thrift installation
- **ANTLR4**: Parser grammar in `parser/src/main/antlr4/`

## Important Conventions

1. **pom.xml formatting**: CI checks that pom.xml files are formatted correctly (`xmlstarlet edit -L` is used to detect issues)
2. **ASF License headers**: Required on all source files (enforced by RAT plugin)
3. **Checkstyle**: Runs automatically; excludes generated code in `**/gen/**` and thrift output
4. **Java version**: Hive 4.2.x requires Java 21

## CI/CD

- **Jenkinsfile**: Parallel test execution across 22 splits by default
- **GitHub Actions**: `build.yml` runs on macOS with JDK 21
- **dev-support/test-patch.sh**: Apache Yetus-based patch testing script

## Testing

- Unit tests use JUnit 5 (Jupiter) with JUnit Vintage for backward compatibility
- Metastore tests use in-memory Derby database by default
- Test groups are defined via `@Tag` annotations on test classes
- Standalone metastore default test group: `org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest`

## References

- Full documentation: https://mr3docs.datamonad.com/
- Apache Hive upstream: https://hive.apache.org/
- Build instructions: https://hive.apache.org/development/gettingstarted-latest/#building-hive-from-source
