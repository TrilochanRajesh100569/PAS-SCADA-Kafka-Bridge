#!/usr/bin/env bash
# stop.sh — companion to start.sh.
#
# Default behaviour: stop the background port-forwards started by start.sh.
# Pods, Artemis container, and minikube cluster are left running so you can
# resume quickly with start.sh.
#
# Flags (pass any combination):
#   --pods       also scale all deployments in pinkline + scada to 0
#   --artemis    also stop the Artemis container on the host
#   --minikube   also `minikube stop` (preserves cluster state)
#   --all        --pods + --artemis + --minikube
#   --wipe       full teardown — deletes namespaces, stops Artemis,
#                runs `minikube delete`. DESTRUCTIVE.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MESSAGING_INFRA="${MESSAGING_INFRA:-/d/pinkline/messaging-infra}"
PF_DIR="$SCRIPT_DIR/.port-forwards"
PF_PIDFILE="$PF_DIR/pids"

log()  { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*"; }

DO_PODS=0; DO_ARTEMIS=0; DO_MINIKUBE=0; DO_WIPE=0
for arg in "$@"; do
  case "$arg" in
    --pods)     DO_PODS=1 ;;
    --artemis)  DO_ARTEMIS=1 ;;
    --minikube) DO_MINIKUBE=1 ;;
    --all)      DO_PODS=1; DO_ARTEMIS=1; DO_MINIKUBE=1 ;;
    --wipe)     DO_WIPE=1 ;;
    *) warn "unknown flag: $arg" ;;
  esac
done

# 1. Always stop port-forwards first.
log "Stopping port-forwards"
if [ -f "$PF_PIDFILE" ]; then
  while read -r pid; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null && ok "killed pid $pid" || true
  done < "$PF_PIDFILE"
  rm -f "$PF_PIDFILE"
fi
# Belt-and-braces: kill any orphan kubectl port-forward we didn't track.
pkill -f "kubectl.*port-forward" 2>/dev/null && ok "killed orphan kubectl port-forwards" || true

# 2. --wipe: full teardown.
if [ "$DO_WIPE" = "1" ]; then
  log "WIPE: deleting namespaces"
  kubectl delete ns pinkline scada --ignore-not-found
  log "WIPE: stopping Artemis"
  docker compose -f "$MESSAGING_INFRA/docker-compose.yml" down 2>/dev/null || true
  log "WIPE: minikube delete"
  minikube delete 2>/dev/null || true
  ok "wipe complete"
  exit 0
fi

# 3. --pods: scale down all deployments.
if [ "$DO_PODS" = "1" ]; then
  log "Scaling deployments to 0"
  for d in $(kubectl -n pinkline get deploy -o name 2>/dev/null); do
    kubectl -n pinkline scale "$d" --replicas=0 >/dev/null && ok "scaled $d in pinkline"
  done
  for d in $(kubectl -n scada get deploy -o name 2>/dev/null); do
    kubectl -n scada scale "$d" --replicas=0 >/dev/null && ok "scaled $d in scada"
  done
fi

# 4. --artemis: stop the host Artemis container.
if [ "$DO_ARTEMIS" = "1" ]; then
  log "Stopping Artemis"
  docker compose -f "$MESSAGING_INFRA/docker-compose.yml" down 2>/dev/null \
    && ok "Artemis stopped" \
    || warn "Artemis compose-down failed (already down?)"
fi

# 5. --minikube: stop the cluster (preserve state).
if [ "$DO_MINIKUBE" = "1" ]; then
  log "Stopping minikube"
  minikube stop 2>/dev/null && ok "minikube stopped" || warn "minikube stop failed"
fi

ok "Done. Resume later with: bash \"$SCRIPT_DIR/start.sh\""
