# CI/CD Setup Guide

## Pipelines

| File | Trigger | What it does |
|------|---------|--------------|
| `.github/workflows/ci.yml` | Every push / PR to main | Build Java bridge (Maven), check Python SCADA API |
| `.github/workflows/cd.yml` | Push to `main` only | Build Docker images → push to GHCR → deploy to VMs |

## Flow

```
Push to main
    │
    ├─► CI: Build Java + Test ──────────────────────────────────────────┐
    │                                                                    │
    └─► CD:                                                             │
          [1] Build bridge image  → ghcr.io/.../pas-scada-bridge:sha   │
          [2] Build scada image   → ghcr.io/.../pas-scada-api:sha      │
          [3] Deploy TMS VM   (10.4.0.23) → docker compose up bridge   │
          [4] Deploy SCADA VM (10.4.0.25) → docker compose up scada-api│
```

## GitHub Secrets to Configure

Go to: **GitHub repo → Settings → Secrets and variables → Actions → New repository secret**

| Secret Name      | Value |
|------------------|-------|
| `SSH_PRIVATE_KEY` | Private SSH key that can log into both VMs |
| `TMS_VM_HOST`     | `10.4.0.23` |
| `SCADA_VM_HOST`   | `10.4.0.25` |
| `VM_USER`         | SSH username on both VMs (e.g. `thiru`) |
| `SCADA_AES_KEY`   | Base64 AES-256-GCM key used for encryption |

## One-time VM Setup

Run these commands **once** on each VM before the first deploy:

### TMS VM (10.4.0.23)
```bash
mkdir -p ~/pas-bridge
# Add your SSH public key to authorized_keys if not done already
echo "<your-public-key>" >> ~/.ssh/authorized_keys
```

### SCADA VM (10.4.0.25)
```bash
mkdir -p ~/pas-scada
echo "<your-public-key>" >> ~/.ssh/authorized_keys
```

## Generate SSH Key Pair (if you don't have one)

```bash
ssh-keygen -t ed25519 -C "github-cicd" -f ~/.ssh/github_cicd -N ""

# Copy public key to both VMs
ssh-copy-id -i ~/.ssh/github_cicd.pub thiru@10.4.0.23
ssh-copy-id -i ~/.ssh/github_cicd.pub thiru@10.4.0.25

# Add private key content to GitHub Secret SSH_PRIVATE_KEY
cat ~/.ssh/github_cicd
```

## Docker Images Published

| Image | Registry |
|-------|----------|
| Bridge (Java) | `ghcr.io/thirunavukkarasuthangaraj/pas-scada-bridge` |
| SCADA API (Python) | `ghcr.io/thirunavukkarasuthangaraj/pas-scada-api` |

Images are tagged with both `:latest` and `:git-sha` for rollback.

## Rollback

To roll back to a previous version on a VM:

```bash
# On TMS VM
cd ~/pas-bridge
docker compose stop bridge
docker run -d --name pas-scada-bridge \
  ghcr.io/thirunavukkarasuthangaraj/pas-scada-bridge:<previous-sha>
```
