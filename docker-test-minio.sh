#!/bin/sh

docker compose -f docker/Linux_ARM64_JDK22_Minio/docker-compose.yml build --build-arg github_pat=$GITHUB_CIRCLECI_PAT
docker compose -f docker/Linux_ARM64_JDK22_Minio/docker-compose.yml up --exit-code-from galia-plugin-s3

