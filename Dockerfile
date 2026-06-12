# Willitude Data MCP Server - container image
# Suitable for local testing or deployment on AWS (ECS, EC2, etc.)
#
# Build:
#   docker build -t willitude-mcp .
#
# Run locally with your SSO credentials mounted:
#   docker run --rm \
#     -e AWS_PROFILE=YongseokMacProfile \
#     -v ~/.aws:/root/.aws:ro \
#     -v $HOME/.willitude:/root/.willitude \
#     willitude-mcp
#
# On AWS with IAM task role: just run the container; no ~/.aws mount needed.
# You may want to mount a large volume at /root/.willitude for the cache.

FROM python:3.12-slim-bookworm

# System deps (git for some uv operations, build tools rarely needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /usr/local/bin/

WORKDIR /app

# Copy only what is needed for dependency resolution first (better layer caching)
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev --no-install-project || uv sync --no-dev --no-install-project

# Now copy the source
COPY . .

# Install the project itself
RUN uv sync --frozen --no-dev

# Default cache location inside container (override with env + volume mount in practice)
ENV WILLITUDE_CACHE_DIR=/root/.willitude/willitude-data
ENV PYTHONUNBUFFERED=1

# The entrypoint runs the MCP server on stdio
ENTRYPOINT ["uv", "run", "willitude-mcp"]
# For SSE mode in the future you could expose a port and change transport
# EXPOSE 8000
# CMD ["uv", "run", "willitude-mcp", "--transport", "sse", "--port", "8000"]
