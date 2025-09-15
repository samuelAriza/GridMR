# Docker & Sudo Management for GridMR

## 🐳 Docker Permissions Setup

### Problem
If you see errors like "permission denied" when running Docker commands, it means your user doesn't have Docker permissions.

### Quick Solutions

#### Option 1: Use the Docker Helper Script
```bash
# Fix permissions permanently (recommended)
./scripts/docker_helper.sh fix-perms

# Then log out and back in, or restart your system
```

#### Option 2: Manual Setup
```bash
# Add your user to the docker group
sudo usermod -aG docker $USER

# Apply the new group membership
newgrp docker

# Or log out and back in
```

#### Option 3: Temporary Fix
```bash
# Set Docker socket permissions (temporary)
sudo chmod 666 /var/run/docker.sock
```

### Verification
After setup, verify Docker works without sudo:
```bash
docker ps
```

## 🚀 Running GridMR with Different Permissions

### Automatic Detection
Our scripts automatically detect your Docker setup:

```bash
# The scripts will ask for permission and handle sudo automatically
./scripts/quick_start_example.sh
./scripts/run_performance_tests.sh setup
```

### Manual Docker Commands

If you need to run Docker commands manually:

**With Docker group membership (no sudo needed):**
```bash
docker compose up -d
docker compose ps
docker compose logs master
docker stats
```

**With sudo (password required):**
```bash
sudo docker compose up -d
sudo docker compose ps
sudo docker compose logs master
sudo docker stats
```

## 🛠️ Docker Helper Script Usage

The `docker_helper.sh` script provides convenient Docker management:

```bash
# Start GridMR (handles permissions automatically)
./scripts/docker_helper.sh start

# Check status
./scripts/docker_helper.sh status

# View logs
./scripts/docker_helper.sh logs
./scripts/docker_helper.sh logs master

# Stop services
./scripts/docker_helper.sh stop

# Fix permissions permanently
./scripts/docker_helper.sh fix-perms
```

## 🔧 Troubleshooting

### "Permission denied" errors
```bash
# Check if Docker daemon is running
sudo systemctl status docker

# Start Docker if needed
sudo systemctl start docker

# Check user groups
groups $USER

# If 'docker' is not in the list, add it:
sudo usermod -aG docker $USER
```

### "Cannot connect to Docker daemon"
```bash
# Check if Docker is installed
docker --version

# Check if Docker service is running
sudo systemctl status docker

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker
```

### Sudo password prompts
If you're repeatedly prompted for your password:

1. **Best solution**: Add user to docker group and restart session
2. **Temporary**: Use `sudo -v` before running scripts to cache credentials
3. **Alternative**: Configure sudoers for Docker (advanced)

### Script hangs waiting for password
If scripts hang waiting for sudo password:
```bash
# Run this first to cache sudo credentials
sudo -v

# Then run your GridMR script
./scripts/run_performance_tests.sh setup
```

## 📋 Environment-Specific Instructions

### Ubuntu/Debian
```bash
# Install Docker
sudo apt update
sudo apt install docker.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER

# Restart or log out/in
sudo reboot
```

### CentOS/RHEL/Fedora
```bash
# Install Docker
sudo dnf install docker docker-compose-plugin

# Start and enable Docker
sudo systemctl start docker
sudo systemctl enable docker

# Add user to docker group
sudo usermod -aG docker $USER

# Restart session
sudo reboot
```

### macOS
```bash
# Install Docker Desktop from docker.com
# Docker Desktop handles permissions automatically
# No sudo needed with Docker Desktop
```

### Windows (WSL2)
```bash
# In WSL2 terminal
sudo apt install docker.io docker-compose-plugin

# Start Docker service
sudo service docker start

# Add user to docker group
sudo usermod -aG docker $USER

# Restart WSL or reboot
```

## 💡 Best Practices

1. **Permanent Setup**: Always prefer adding your user to the docker group over using sudo
2. **Security**: Be aware that docker group membership gives root-equivalent access
3. **Scripts**: Use our provided scripts which handle permissions automatically
4. **Verification**: Always test `docker ps` without sudo after setup

## 🆘 Getting Help

If you continue having Docker permission issues:

1. Check the Docker documentation for your OS
2. Verify Docker installation: `docker --version`
3. Check Docker service: `sudo systemctl status docker`
4. Try the Docker helper script: `./scripts/docker_helper.sh fix-perms`
5. As last resort, run GridMR scripts with sudo access when prompted