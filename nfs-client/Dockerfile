FROM ubuntu:22.04

# -------------------------------------------------------------------
# Base image: Ubuntu 22.04 (Jammy Jellyfish)
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# Install required packages
# - nfs-common: Provides client utilities to mount NFS shares
# - iputils-ping: Allows network connectivity testing
# -------------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y nfs-common iputils-ping && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# Keep container running
# Using 'tail -f /dev/null' ensures the container stays alive
# until manually stopped or used for interactive operations.
# -------------------------------------------------------------------
CMD tail -f /dev/null