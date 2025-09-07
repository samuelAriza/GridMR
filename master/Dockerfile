FROM python:3.10-slim

# -------------------------------------------------------------------
# Metadata
# -------------------------------------------------------------------
LABEL maintainer="GridMR Team"
LABEL version="1.0.0"
LABEL description="GridMR Master Node - Production Ready"

# -------------------------------------------------------------------
# Environment variables
# -------------------------------------------------------------------
# PYTHONUNBUFFERED: ensures logs are flushed directly to stdout/stderr
# PYTHONDONTWRITEBYTECODE: avoids writing .pyc files
# PIP_NO_CACHE_DIR: disables pip cache to reduce image size
# PIP_DISABLE_PIP_VERSION_CHECK: skips pip update check for faster installs
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# -------------------------------------------------------------------
# Security: create non-root user for running the service
# -------------------------------------------------------------------
RUN groupadd -r gridmr && \
    useradd -r -g gridmr -d /app -s /bin/bash gridmr

# -------------------------------------------------------------------
# Set working directory
# -------------------------------------------------------------------
WORKDIR /app

# -------------------------------------------------------------------
# Copy source code and set ownership to non-root user
# -------------------------------------------------------------------
COPY --chown=gridmr:gridmr . /app

# -------------------------------------------------------------------
# Install Python dependencies
# -------------------------------------------------------------------
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir fastapi uvicorn pydantic pydantic_settings httpx

# -------------------------------------------------------------------
# Create application directories and set proper permissions
# -------------------------------------------------------------------
RUN mkdir -p /app/data /app/logs && \
    chown -R gridmr:gridmr /app && \
    chmod -R 755 /app

# -------------------------------------------------------------------
# Switch to non-root user
# -------------------------------------------------------------------
USER gridmr

# -------------------------------------------------------------------
# Expose service port
# -------------------------------------------------------------------
EXPOSE 8000

# -------------------------------------------------------------------
# Start the FastAPI application using Uvicorn
# --reload is enabled for development (should be removed in production)
# -------------------------------------------------------------------
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]