# Stage 1: Use the official Python 3.12 image based on Debian Trixie (slim version)
FROM python:3.12-slim-trixie

# Set environment variables for better Python and uv behavior
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off

# Set the working directory inside the container
WORKDIR /workspace

# Install system dependencies including curl and ca-certificates
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv using pip
# We do this in its own step so it can be cached
RUN pip install uv

# Copy only the dependency file first to leverage Docker's build cache.
# This layer will only be rebuilt if requirements.txt changes.
COPY uv.lock /workspace/
# https://github.com/astral-sh/uv/issues/6722#issuecomment-3329232328
RUN cat > pyproject.toml <<EOF
[project]
name = "snakemake-executor-plugin-googlebatch"
dynamic = ['version']
EOF

# Install Python dependencies using uv.
# The --system flag tells uv to install into the global site-packages,
# which is standard practice inside a container.
RUN UV_PROJECT_ENVIRONMENT=/usr/local uv sync  --no-install-package my-root-package --frozen

# --- Optional: Create a non-root user for security ---
# VS Code dev containers often handle user creation for you. If you need to
# manage it manually, uncomment the following lines.
# RUN useradd -ms /bin/bash vscode
# USER vscode

# NOTE: No CMD or ENTRYPOINT is specified.
# This is intentional. The dev container will be kept running by the
# `command: sleep infinity` in the docker-compose.yml file,
# allowing VS Code to attach to it.
