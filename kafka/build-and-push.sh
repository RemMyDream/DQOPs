set -e

DOCKER_USERNAME="${1:-votaquangnhat}"
IMAGE_NAME="custom-debezium"
VERSION="${2:-latest}"
FULL_IMAGE="${DOCKER_USERNAME}/${IMAGE_NAME}:${VERSION}"

echo "======================================"
echo "Building Custom Debezium Image"
echo "======================================"
echo "Image: ${FULL_IMAGE}"
echo ""

# Build the image
echo "🔨 Building Docker image..."
docker build -t ${FULL_IMAGE} -f kafka/Dockerfile kafka/

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "✅ Build successful!"
echo ""

# Ask for DockerHub login
echo "🔐 Logging in to DockerHub..."
docker login

if [ $? -ne 0 ]; then
    echo "❌ DockerHub login failed!"
    exit 1
fi

# Push the image
echo ""
echo "📤 Pushing image to DockerHub..."
docker push ${FULL_IMAGE}

if [ $? -ne 0 ]; then
    echo "❌ Push failed!"
    exit 1
fi

echo ""
echo "======================================"
echo "✅ Success!"
echo "======================================"
echo "Image pushed: ${FULL_IMAGE}"
echo ""
echo "Next steps:"
echo "1. Update k8s/kafka.yaml to use: ${FULL_IMAGE}"
echo "2. Run: kubectl apply -f k8s/kafka.yaml"
echo "3. Or add to Helm chart"
echo ""
