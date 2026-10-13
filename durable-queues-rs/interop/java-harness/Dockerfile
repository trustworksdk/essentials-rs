# syntax=docker/dockerfile:1
# The Java node of the Phase 6 interop tests. Nothing Java is installed in the devcontainer: Maven and the JDK only
# exist in the build stage. Build with BuildKit (the ~/.m2 cache mount needs it):
#   docker build -t durable-queues-java-harness interop/java-harness
FROM maven:3.9.16-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -o package

FROM eclipse-temurin:25-jre
# libfaketime, for the clock-skew scenario only: inactive unless LD_PRELOAD and FAKETIME are set at run time
RUN apt-get update \
 && apt-get install -y --no-install-recommends libfaketime \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/lib/*-linux-gnu/faketime/libfaketime.so.1 /usr/local/lib/libfaketime.so.1
COPY --from=build /build/target/java-harness.jar /app/java-harness.jar
ENTRYPOINT ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-jar", "/app/java-harness.jar"]
