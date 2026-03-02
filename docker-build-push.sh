#!/usr/bin/env bash
# ============================================================
# Build and push the custom Airflow image to GHCR
# Usage: ./docker-build-push.sh [-t <tag>] [--build-only] [--push-only] [--public]
# ============================================================

set -euo pipefail

GHCR_USER="ajlile98"   # <-- update this
IMAGE_NAME="themeparks-airflow"
TAG="latest"
BUILD_ONLY=false
PUSH_ONLY=false
MAKE_PUBLIC=false
GITHUB_PAT=""

usage() {
    echo "Usage: $0 [-u <ghcr-user>] [-t <tag>] [--build-only] [--push-only] [--public]"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--user)       GHCR_USER="$2"; shift 2 ;;
        -t|--tag)        TAG="$2";       shift 2 ;;
        --build-only)    BUILD_ONLY=true;   shift ;;
        --push-only)     PUSH_ONLY=true;    shift ;;
        --public)        MAKE_PUBLIC=true;  shift ;;
        -h|--help)       usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

FULL_IMAGE="ghcr.io/${GHCR_USER}/${IMAGE_NAME}:${TAG}"

step() { echo -e "\n==> $*"; }

# ── Login ─────────────────────────────────────────────────────
if [[ "$BUILD_ONLY" == false ]]; then
    step "Logging in to GHCR"
    echo "Enter your GitHub PAT (needs write:packages scope):"
    read -rs GITHUB_PAT
    echo
    echo "${GITHUB_PAT}" | docker login ghcr.io -u "${GHCR_USER}" --password-stdin
fi

# ── Build ──────────────────────────────────────────────────────
if [[ "$PUSH_ONLY" == false ]]; then
    step "Building ${FULL_IMAGE}"
    docker build \
        -f Dockerfile.ghcr \
        -t "${FULL_IMAGE}" \
        .
    echo "Build succeeded: ${FULL_IMAGE}"
fi

# ── Push ───────────────────────────────────────────────────────
if [[ "$BUILD_ONLY" == false ]]; then
    step "Pushing ${FULL_IMAGE}"
    docker push "${FULL_IMAGE}"
    echo "Push succeeded: ${FULL_IMAGE}"
fi

# ── Set visibility to public ───────────────────────────────────
if [[ "$MAKE_PUBLIC" == true ]]; then
    if [[ -z "$GITHUB_PAT" ]]; then
        echo "Enter your GitHub PAT to update package visibility:"
        read -rs GITHUB_PAT
        echo
    fi
    step "Setting ${IMAGE_NAME} visibility to public"
    HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
        -X PATCH \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${GITHUB_PAT}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        -d '{"visibility":"public"}' \
        "https://api.github.com/user/packages/container/${IMAGE_NAME}")
    if [[ "$HTTP_STATUS" == "200" ]]; then
        echo "Package visibility set to public."
    else
        echo "Warning: GitHub API returned HTTP ${HTTP_STATUS}. Check PAT scopes (needs read:packages + write:packages) or visit:"
        echo "  https://github.com/users/${GHCR_USER}/packages/container/${IMAGE_NAME}/settings"
    fi
fi
