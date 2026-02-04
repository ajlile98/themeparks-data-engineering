#!/bin/bash
# Build and deploy Theme Parks Docker image to DockerHub
# Usage: ./build-and-deploy.sh [version]

set -e  # Exit on error

# =============================================================================
# Configuration
# =============================================================================

# IMPORTANT: Set this to your actual DockerHub USERNAME (not email)
# The repository must exist on DockerHub before pushing
# Create it at: https://hub.docker.com/repository/create
DOCKERHUB_USERNAME="${DOCKERHUB_USERNAME:-anlile}"
IMAGE_NAME="themeparks"
DOCKERFILE_PATH="docker/Dockerfile"

# Get version from argument or default to latest
VERSION="${1:-latest}"

# Full image names
IMAGE_TAG="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${VERSION}"
IMAGE_LATEST="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:latest"

# =============================================================================
# Color output
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# =============================================================================
# Pre-flight checks
# =============================================================================

log_info "Starting build and deploy process..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    log_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if logged into DockerHub by checking credentials
# Try to access DockerHub - if this fails, we're not logged in
if ! docker pull hello-world > /dev/null 2>&1; then
    log_warn "Cannot access DockerHub. You may need to run: docker login"
    log_warn "Continuing anyway..."
fi

# Check if Dockerfile exists
if [ ! -f "$DOCKERFILE_PATH" ]; then
    log_error "Dockerfile not found at: $DOCKERFILE_PATH"
    exit 1
fi

# Check if uv.lock exists
if [ ! -f "uv.lock" ]; then
    log_error "uv.lock not found. Run 'uv lock' first."
    exit 1
fi

log_info "Pre-flight checks passed"

# =============================================================================
# Build image
# =============================================================================

log_info "Building Docker image: $IMAGE_TAG"
log_info "This may take a few minutes..."

docker build \
    -t "$IMAGE_TAG" \
    -f "$DOCKERFILE_PATH" \
    --build-arg BUILDKIT_INLINE_CACHE=1 \
    .

if [ $? -ne 0 ]; then
    log_error "Docker build failed"
    exit 1
fi

log_info "Build completed successfully"

# =============================================================================
# Tag image
# =============================================================================

if [ "$VERSION" != "latest" ]; then
    log_info "Tagging image as latest"
    docker tag "$IMAGE_TAG" "$IMAGE_LATEST"
    
    # Extract major and minor versions for additional tags
    if [[ $VERSION =~ ^([0-9]+)\.([0-9]+)\. ]]; then
        MAJOR="${BASH_REMATCH[1]}"
        MINOR="${BASH_REMATCH[2]}"
        
        MAJOR_TAG="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${MAJOR}"
        MINOR_TAG="${DOCKERHUB_USERNAME}/${IMAGE_NAME}:${MAJOR}.${MINOR}"
        
        log_info "Tagging image as $MAJOR and $MAJOR.$MINOR"
        docker tag "$IMAGE_TAG" "$MAJOR_TAG"
        docker tag "$IMAGE_TAG" "$MINOR_TAG"
    fi
fi

# =============================================================================
# Test image
# =============================================================================

log_info "Testing image..."
if docker run --rm "$IMAGE_TAG" --version > /dev/null 2>&1; then
    log_info "Image test passed"
else
    log_warn "Image test failed (non-critical)"
fi

# =============================================================================
# Push to DockerHub
# =============================================================================

log_info "Pushing image to DockerHub..."

# Push version tag
log_info "Pushing $IMAGE_TAG"
if ! docker push "$IMAGE_TAG"; then
    log_error "Failed to push image to DockerHub"
    log_error ""
    log_error "Common issues:"
    log_error "1. Repository doesn't exist - Create it at: https://hub.docker.com/repository/create"
    log_error "2. Wrong username - Your username is: $(docker info 2>&1 | grep Username | awk '{print $2}')"
    log_error "3. Not logged in - Run: docker login"
    log_error ""
    log_error "To create the repository:"
    log_error "  - Go to: https://hub.docker.com/repository/create"
    log_error "  - Name: $IMAGE_NAME"
    log_error "  - Visibility: Public (or Private)"
    exit 1
fi

# Push latest tag
if [ "$VERSION" != "latest" ]; then
    log_info "Pushing $IMAGE_LATEST"
    docker push "$IMAGE_LATEST"
    
    # Push major/minor tags if they exist
    if [[ $VERSION =~ ^([0-9]+)\.([0-9]+)\. ]]; then
        log_info "Pushing $MAJOR_TAG"
        docker push "$MAJOR_TAG"
        
        log_info "Pushing $MINOR_TAG"
        docker push "$MINOR_TAG"
    fi
fi

log_info "Push completed successfully"

# =============================================================================
# Summary
# =============================================================================

echo ""
log_info "========================================="
log_info "Deployment Summary"
log_info "========================================="
log_info "Image: $IMAGE_TAG"
log_info "Size: $(docker images $IMAGE_TAG --format "{{.Size}}")"
log_info "DockerHub: https://hub.docker.com/r/${DOCKERHUB_USERNAME}/${IMAGE_NAME}"
log_info "========================================="
echo ""
log_info "To pull this image:"
echo "  docker pull $IMAGE_TAG"
echo ""
log_info "To update Kubernetes DAGs, set:"
echo "  IMAGE_NAME=$IMAGE_TAG"
