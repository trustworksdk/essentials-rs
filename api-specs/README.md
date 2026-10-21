# Simulator API specifications

The OpenAPI documents of the two simulators, distributed together under one version:

| Service | Original (OpenAPI 3.1) | Rendition for client generators (OpenAPI 3.0.3) |
|---|---|---|
| Exchange simulator | `exchange-simulator/openapi.yaml` | `exchange-simulator/openapi-3.0.json` |
| Payment gateway simulator | `payment-gateway/openapi.yaml` | `payment-gateway/openapi-3.0.json` |

The 3.1 documents are the source of truth. Each service's contract tests hold them to the code: every route, request,
response and webhook event. Each service also serves its own at `GET /openapi.yaml`.

The 3.0 renditions are derived from them (`simulator_kit::openapi30`) and committed, so they can be shipped as files.
Use them to generate clients in any language. Use the 3.1 documents for anything else (documentation, validation,
mock servers). Client generators often handle 3.1 poorly: openapi-generator 7.14 marks 3.1 support as beta, and the
Java clients it produces from these documents do not compile. The renditions differ only in how they say things:

- "X or null" becomes `nullable: true`, and `const` becomes a one-value `enum`;
- 3.0 cannot express `unevaluatedProperties` across `allOf`, so event schemas accept extra properties there;
- the top-level `webhooks` section is left out. The event schemas (`TradeExecutedEvent` and the others) remain in
  `components`, so generators still produce a model per event.

## Getting the documents

| You want | Take |
|---|---|
| A Java (Maven/Gradle) build that generates clients | The Maven artifact `dk.trustworks.simulators:simulator-api-specs` (below) |
| The documents for any other language or tool | The four files above, from this repository at the tag of the version you target (`simulators-v<version>`) |
| The spec of the service you are actually running | `GET /openapi.yaml` on that service (3.1) |

The artifact holds:

```
openapi/exchange-simulator/openapi.yaml
openapi/exchange-simulator/openapi-3.0.json
openapi/payment-gateway/openapi.yaml
openapi/payment-gateway/openapi-3.0.json
```

## Versioning

One version covers both simulators' APIs. It appears in five places, and the contract tests fail if any of them
differ:

- `exchange-simulator/Cargo.toml` and `payment-gateway/Cargo.toml` (`version`);
- both documents (`info.version`);
- `api-specs/pom.xml` (`<version>`).

Bump the version whenever either document changes: the minor version for additions, the major version for anything
that breaks a client. While the version is 0.x, a minor bump can be breaking. A published version is never
republished; GitHub Packages refuses that anyway. Tag each published version `simulators-v<version>`.

## Using it from a Java project (Maven)

[`example-consumer/pom.xml`](example-consumer/pom.xml) is a complete, working example that compiles both clients.
In your own `pom.xml`:

1. **Unpack the documents** with `maven-dependency-plugin` (`unpack`, phase `initialize`) from
   `dk.trustworks.simulators:simulator-api-specs:<version>` into `target/simulator-api-specs`.
2. **Generate a client per service** with `openapi-generator-maven-plugin`, one execution each, from
   `target/simulator-api-specs/openapi/<service>/openapi-3.0.json`. The example uses the `java` generator with the
   `native` library (`java.net.http` and Jackson), Jakarta annotations, and its own package per service:

   | Service | APIs | Models |
   |---|---|---|
   | Exchange | `dk.trustworks.simulators.exchange.api` (`OrdersApi`, `InstrumentsApi`, `MarketDataApi`, …) | `…exchange.model` |
   | Payment gateway | `dk.trustworks.simulators.payments.api` (`PaymentsApi`, `StatementsApi`, …) | `…payments.model` |

   With another library (`restclient`, `webclient`, `feign`) the generated code stays the same, apart from the
   runtime dependencies it needs.
3. **Add the generated clients' runtime dependencies**: Jackson (`jackson-databind`, `jackson-datatype-jsr310`),
   `jackson-databind-nullable`, `jakarta.annotation-api`. They are listed in the example.

Webhook payloads: deserialize the request body into the event model (`TradeExecutedEvent` and so on) after verifying
the signature. The generated clients do not verify signatures.

### Repository access

GitHub Packages needs a token **even to read** a package. Add the repository and a server entry with a token that has
`read:packages`:

```xml
<!-- pom.xml -->
<repositories>
  <repository>
    <id>github-simulators</id>
    <url>https://maven.pkg.github.com/trustworksdk/essentials-rs</url>
  </repository>
</repositories>
```

```xml
<!-- ~/.m2/settings.xml -->
<servers>
  <server>
    <id>github-simulators</id>
    <username>YOUR_GITHUB_USER</username>
    <password>${env.GITHUB_TOKEN}</password>
  </server>
</servers>
```

## Building and publishing

Both scripts run Maven in a container (`maven:3.9.9-eclipse-temurin-21`), so only Docker is needed. Downloaded
dependencies are kept in `~/.m2-docker` (set `M2_DIR` to use another directory).

- `./check.sh` builds the artifact into that local repository, then generates and compiles the example consumer's
  two clients from it. Run it after changing a document.
- `M2_DIR=~/.m2 ./check.sh` does the same into your own local repository, so a Java build on this machine resolves
  `simulator-api-specs` without GitHub Packages (and without the repository and token setup below). With Maven
  installed, `mvn install` in this folder does the same. Maven runs as you, so nothing in the repository ends up
  owned by root.
- `./publish.sh` runs the contract tests (one version everywhere, renditions current), then `check.sh`, then
  `mvn deploy` to `https://maven.pkg.github.com/trustworksdk/essentials-rs`. It needs `GITHUB_ACTOR` and a
  `GITHUB_TOKEN` with `write:packages` in the environment. `maven-settings.xml` passes them to Maven, and they are
  never written to disk.

After changing a document, regenerate its rendition and check it:

```bash
UPDATE_OPENAPI_30=1 cargo test -j 1 -p exchange-simulator --test openapi the_openapi_30_rendition_is_current
UPDATE_OPENAPI_30=1 cargo test -j 1 -p payment-gateway --test openapi the_openapi_30_rendition_is_current
```

There is no CI yet: publishing is manual.
