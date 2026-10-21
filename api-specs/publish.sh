#!/usr/bin/env bash
# Publishes simulator-api-specs to GitHub Packages (https://maven.pkg.github.com/trustworksdk/essentials-rs).
#
# Needs GITHUB_ACTOR (your GitHub user) and GITHUB_TOKEN (a token with write:packages) in the environment; they are
# handed to Maven through maven-settings.xml and never written to disk. A version can be published once: bump it
# first (both crates, both documents, api-specs/pom.xml; see README.md).
#
# Before publishing, the contract tests that guard the artifact run: one version everywhere, renditions current.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$here")"
: "${GITHUB_ACTOR:?set GITHUB_ACTOR to your GitHub user}"
: "${GITHUB_TOKEN:?set GITHUB_TOKEN to a token with write:packages}"
m2="${M2_DIR:-$HOME/.m2-docker}"
image="${MAVEN_IMAGE:-maven:3.9.9-eclipse-temurin-21}"
memory="${MAVEN_MEMORY:-1536m}"
mkdir -p "$m2"

echo "Checking the documents (contract tests)"
(cd "$root" && cargo test -j 1 -p payment-gateway --test openapi)
(cd "$root" && cargo test -j 1 -p exchange-simulator --test openapi)
"$here/check.sh"

echo "Publishing"
docker run --rm --memory "$memory" --user "$(id -u):$(id -g)" -e MAVEN_OPTS="-Xmx768m" -e GITHUB_ACTOR -e GITHUB_TOKEN \
  -e MAVEN_CONFIG=/var/maven/.m2 -v "$root:$root" -v "$m2:/var/maven/.m2" -w "$here" "$image" \
  mvn -B -q -Duser.home=/var/maven -s maven-settings.xml deploy
echo "Published simulator-api-specs $(sed -n 's:.*<version>\(.*\)</version>.*:\1:p' "$here/pom.xml" | head -1)"
