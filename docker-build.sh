#!/usr/bin/env bash

set -euo pipefail
set -x

if [ -f release.properties ]; then
    VERSION=$(grep 'project.rel.pl.net.was\\:trino-faker=' release.properties | cut -d'=' -f2)
else
    VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
fi
TAG=nineinchnick/trino-faker:$VERSION

docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -t "$TAG" \
    --build-arg VERSION="$VERSION" \
    --push \
    .
