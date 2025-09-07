#!/bin/bash
set -e

# Ensure directories exist and correct perms
mkdir -p /exports/input /exports/output
chown -R nobody:nogroup /exports
chmod -R 0777 /exports

# Start rpcbind and nfs server in foreground
service rpcbind start
service nfs-kernel-server start

# Keep container alive
tail -f /dev/null