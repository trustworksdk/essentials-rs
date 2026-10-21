#!/usr/bin/env bash
# Builds simulator-api-specs into a local Maven repository and compiles the example consumer against it: both
# services' Java clients generated from the shipped documents. Maven runs in a container; only Docker is needed.
# The local repository is ~/.m2-docker (override with M2_DIR), kept between runs. The container is limited to
# MAVEN_MEMORY (default 1536m).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$here")"
m2="${M2_DIR:-$HOME/.m2-docker}"
image="${MAVEN_IMAGE:-maven:3.9.9-eclipse-temurin-21}"
# Capped, so it fits next to a running IDE and Rust toolchain in a small machine.
memory="${MAVEN_MEMORY:-1536m}"
mkdir -p "$m2"

mvn() {
  local dir="$1"
  shift
  # As the calling user, so nothing written (the repository included) ends up owned by root.
  docker run --rm --memory "$memory" --user "$(id -u):$(id -g)" -e MAVEN_OPTS="-Xmx768m" \
    -e MAVEN_CONFIG=/var/maven/.m2 -v "$root:$root" -v "$m2:/var/maven/.m2" -w "$dir" "$image" \
    mvn -B -q -Duser.home=/var/maven "$@"
}

echo "Building simulator-api-specs into $m2"
mvn "$here" install
echo "Generating and compiling the Java clients (example-consumer)"
mvn "$here/example-consumer" clean compile
echo "OK: both clients compile from the artifact's 3.0 renditions"
