FROM ubuntu:22.04

# -------------------------------------------------------------------
# Base image: Ubuntu 22.04 (Jammy Jellyfish)
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# Install required packages
# - nfs-kernel-server: Provides NFS server capabilities
# -------------------------------------------------------------------
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y nfs-kernel-server && \
    mkdir -p /exports && \
    chown -R nobody:nogroup /exports && \
    chmod 777 /exports && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# Copy NFS exports configuration
# This file defines the directories shared by the NFS server
# and the clients that have access permissions.
# -------------------------------------------------------------------
COPY exports /etc/exports

# -------------------------------------------------------------------
# Expose required NFS ports
# - 2049: NFS service
# - 111: RPC bind service (TCP/UDP)
# -------------------------------------------------------------------
EXPOSE 2049 111/tcp 111/udp

# -------------------------------------------------------------------
# Start NFSv4 server
# - rpcbind: Starts the RPC bind service
# - exportfs: Exports the directories defined in /etc/exports
# - rpc.nfsd: Starts the NFS daemon (v4)
# - rpc.mountd: Handles mount requests from clients
# -------------------------------------------------------------------
CMD rpcbind -w && \
    exportfs -rv && \
    rpc.nfsd -V 4 && \
    rpc.mountd -F -V 4