#!/usr/bin/env bash
set -euo pipefail

# Fallback: load repo devcontainer env file when lifecycle command env is missing.
# This keeps postCreate reliable even if --env-file points elsewhere.
if [[ -z "${GIT_USERNAME:-}" || -z "${GIT_EMAIL:-}" || -z "${GIT_PAT:-}" ]]; then
  if [[ -f ".devcontainer/setup.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source ".devcontainer/setup.env"
    set +a
  fi
fi

echo "[postCreate] Checking SSH setup for GitHub access..."

SSH_DIR="$HOME/.ssh"
HOST_SSH_DIR="/mnt/host-ssh"
CURRENT_UID="$(id -u)"
CURRENT_USER="$(id -un)"
SSH_KEY_PATH="${SSH_KEY_PATH:-}"
GIT_USERNAME="${GIT_USERNAME:-}"
GIT_EMAIL="${GIT_EMAIL:-}"
GIT_PAT="${GIT_PAT:-}"

if [[ ! -d "$SSH_DIR" ]]; then
  mkdir -p "$SSH_DIR" 2>/dev/null || true
fi

resolve_host_key_from_env() {
  local configured_key="$1"
  local rel_path=""

  if [[ -z "$configured_key" ]]; then
    return 1
  fi

  if [[ "$configured_key" == "~/.ssh/"* ]]; then
    rel_path="${configured_key#~/.ssh/}"
  elif [[ "$configured_key" == "$HOME/.ssh/"* ]]; then
    rel_path="${configured_key#"$HOME/.ssh/"}"
  elif [[ "$configured_key" == *"/.ssh/"* ]]; then
    rel_path="${configured_key#*"/.ssh/"}"
  else
    rel_path="$(basename "$configured_key")"
  fi

  if [[ -n "$rel_path" ]]; then
    printf "%s/%s\n" "$HOST_SSH_DIR" "$rel_path"
    return 0
  fi

  return 1
}

# If the host has ~/.ssh mounted read-only at /mnt/host-ssh, copy the minimum files
# into the container-owned ~/.ssh. This avoids OpenSSH refusing to read bind-mounted
# files whose UID/GID don't match the container user.
if [[ -d "$HOST_SSH_DIR" ]]; then
  echo "[postCreate] Detected host SSH mount at $HOST_SSH_DIR; copying into $SSH_DIR (container-owned)."

  chmod 700 "$SSH_DIR" 2>/dev/null || true

  # Copy config (optional)
  if [[ -f "$HOST_SSH_DIR/config" && ! -f "$SSH_DIR/config" ]]; then
    install -m 600 "$HOST_SSH_DIR/config" "$SSH_DIR/config" 2>/dev/null || true
  fi

  # Copy known_hosts (optional)
  if [[ -f "$HOST_SSH_DIR/known_hosts" && ! -f "$SSH_DIR/known_hosts" ]]; then
    install -m 600 "$HOST_SSH_DIR/known_hosts" "$SSH_DIR/known_hosts" 2>/dev/null || true
  fi

  SELECTED_KEY_DEST=""
  if [[ -n "$SSH_KEY_PATH" ]]; then
    SELECTED_HOST_KEY="$(resolve_host_key_from_env "$SSH_KEY_PATH" || true)"
    if [[ -n "$SELECTED_HOST_KEY" && -f "$SELECTED_HOST_KEY" ]]; then
      SELECTED_KEY_NAME="$(basename "$SELECTED_HOST_KEY")"
      SELECTED_KEY_DEST="$SSH_DIR/$SELECTED_KEY_NAME"

      if [[ ! -f "$SELECTED_KEY_DEST" ]]; then
        install -m 600 "$SELECTED_HOST_KEY" "$SELECTED_KEY_DEST" 2>/dev/null || true
      fi
      if [[ -f "$SELECTED_HOST_KEY.pub" && ! -f "$SELECTED_KEY_DEST.pub" ]]; then
        install -m 644 "$SELECTED_HOST_KEY.pub" "$SELECTED_KEY_DEST.pub" 2>/dev/null || true
      fi

      SSH_CONFIG_FILE="$SSH_DIR/config"
      touch "$SSH_CONFIG_FILE" 2>/dev/null || true
      chmod 600 "$SSH_CONFIG_FILE" 2>/dev/null || true

      if ! grep -q "IdentityFile ~/.ssh/$SELECTED_KEY_NAME" "$SSH_CONFIG_FILE" 2>/dev/null; then
        {
          echo ""
          echo "# sdmxflow devcontainer managed block"
          echo "Host github.com"
          echo "  HostName github.com"
          echo "  User git"
          echo "  IdentityFile ~/.ssh/$SELECTED_KEY_NAME"
          echo "  IdentitiesOnly yes"
        } >>"$SSH_CONFIG_FILE"
      fi

      echo "[postCreate] Using SSH key from SSH_KEY_PATH: ~/.ssh/$SELECTED_KEY_NAME"
    else
      echo "[postCreate] WARNING: SSH_KEY_PATH is set but key was not found in host mount: $SSH_KEY_PATH"
    fi
  fi

  # Copy common key names if present (idempotent; do not overwrite)
  for key in id_ed25519 id_rsa id_ecdsa; do
    if [[ -f "$HOST_SSH_DIR/$key" && ! -f "$SSH_DIR/$key" ]]; then
      install -m 600 "$HOST_SSH_DIR/$key" "$SSH_DIR/$key" 2>/dev/null || true
    fi
    if [[ -f "$HOST_SSH_DIR/$key.pub" && ! -f "$SSH_DIR/$key.pub" ]]; then
      install -m 644 "$HOST_SSH_DIR/$key.pub" "$SSH_DIR/$key.pub" 2>/dev/null || true
    fi
  done
else
  echo "[postCreate] NOTE: No host SSH mount found at $HOST_SSH_DIR."
  echo "[postCreate] If you need private GitHub access, either mount ~/.ssh (recommended mount target is $HOST_SSH_DIR) or use SSH agent forwarding."
fi

# Best-effort permissions; on bind mounts this may fail (that's ok).
chmod 700 "$SSH_DIR" 2>/dev/null || true

# OpenSSH is strict about ownership/permissions of ~/.ssh and ~/.ssh/config.
# In devcontainers, bind-mounting ~/.ssh from the host can result in UID mismatch
# (e.g., host user != container user), which makes ssh refuse to read config.
if [[ -f "$SSH_DIR/config" ]]; then
  chmod 600 "$SSH_DIR/config" 2>/dev/null || true
fi

SSH_DIR_UID="$(stat -c %u "$SSH_DIR" 2>/dev/null || echo "")"
SSH_CONFIG_UID=""
if [[ -f "$SSH_DIR/config" ]]; then
  SSH_CONFIG_UID="$(stat -c %u "$SSH_DIR/config" 2>/dev/null || echo "")"
fi

SSH_OWNERSHIP_OK=true

if [[ -n "$SSH_DIR_UID" && "$SSH_DIR_UID" != "$CURRENT_UID" && "$SSH_DIR_UID" != "0" ]]; then
  echo "[postCreate] WARNING: $SSH_DIR is owned by UID $SSH_DIR_UID but current user is $CURRENT_USER (UID $CURRENT_UID)."
  echo "[postCreate] This commonly breaks ssh with: 'Bad owner or permissions on ~/.ssh/config'."
  echo "[postCreate] Recommended fix (already enabled in this repo): set devcontainer.json 'updateRemoteUserUID': true and rebuild the container."
  echo "[postCreate] Alternative: use SSH agent forwarding instead of mounting ~/.ssh."
  SSH_OWNERSHIP_OK=false
fi

if [[ -n "$SSH_CONFIG_UID" && "$SSH_CONFIG_UID" != "$CURRENT_UID" && "$SSH_CONFIG_UID" != "0" ]]; then
  echo "[postCreate] WARNING: $SSH_DIR/config is owned by UID $SSH_CONFIG_UID but current user is $CURRENT_USER (UID $CURRENT_UID)."
  echo "[postCreate] ssh will refuse to read this file until UID matches or you stop mounting it."
  SSH_OWNERSHIP_OK=false
fi

if [[ "$SSH_OWNERSHIP_OK" != "true" ]]; then
  echo "[postCreate] Skipping GitHub SSH probe due to UID/ownership mismatch."
  echo "[postCreate] Rebuild the devcontainer to apply UID sync, then re-run: ssh -T git@github.com"
  # Continue with the rest of postCreate (Docker checks, etc.)
else

  # Ensure known_hosts exists to avoid interactive prompts.
  KNOWN_HOSTS="$SSH_DIR/known_hosts"
  touch "$KNOWN_HOSTS" 2>/dev/null || true
  chmod 600 "$KNOWN_HOSTS" 2>/dev/null || true

  if ! ssh-keygen -F github.com -f "$KNOWN_HOSTS" >/dev/null 2>&1; then
    echo "[postCreate] Adding github.com host key to known_hosts (best-effort)..."
    ssh-keyscan -t rsa,ecdsa,ed25519 github.com >>"$KNOWN_HOSTS" 2>/dev/null || true
  fi

  # Basic check: do we see any private keys?
  shopt -s nullglob
  key_candidates=("$SSH_DIR/id_ed25519" "$SSH_DIR/id_rsa" "$SSH_DIR/id_ecdsa")
  if [[ -n "${SELECTED_KEY_DEST:-}" ]]; then
    key_candidates+=("$SELECTED_KEY_DEST")
  fi
  shopt -u nullglob

  if (( ${#key_candidates[@]} == 0 )); then
    echo "[postCreate] WARNING: No standard private key files found in $SSH_DIR (id_ed25519/id_rsa/id_ecdsa)."
    echo "[postCreate] If you use a non-standard key name, ensure your ssh config references it."
  fi

  # Non-interactive GitHub auth probe.
  # GitHub commonly returns exit code 1 on successful auth ("You've successfully authenticated").
  set +e
  SSH_OUTPUT=$(ssh \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=accept-new \
    -T git@github.com 2>&1)
  SSH_RC=$?
  set -e

  if echo "$SSH_OUTPUT" | grep -qi "successfully authenticated"; then
    echo "[postCreate] SUCCESS: GitHub SSH authentication works."
  elif [[ $SSH_RC -eq 255 ]]; then
    echo "[postCreate] WARNING: GitHub SSH check failed (ssh returned 255)."
    echo "[postCreate] Output:"
    echo "$SSH_OUTPUT"
    echo "[postCreate] Common fixes:"
    echo "[postCreate] - Ensure your host key is mounted into the container (see .devcontainer/devcontainer.json mounts)."
    echo "[postCreate] - Check permissions/ownership of mounted ~/.ssh (ssh is strict)."
    echo "[postCreate] - Confirm your key is added to GitHub and not passphrase-blocked for non-interactive use."
  else
    echo "[postCreate] NOTE: GitHub SSH check did not confirm authentication."
    echo "[postCreate] ssh exit code: $SSH_RC"
    echo "[postCreate] Output:"
    echo "$SSH_OUTPUT"
  fi

fi

echo "[postCreate] Configuring git identity..."
if [[ -n "$GIT_USERNAME" ]]; then
  git config --global user.name "$GIT_USERNAME"
else
  echo "[postCreate] WARNING: GIT_USERNAME is not set; skipping git user.name"
fi

if [[ -n "$GIT_EMAIL" ]]; then
  git config --global user.email "$GIT_EMAIL"
else
  echo "[postCreate] WARNING: GIT_EMAIL is not set; skipping git user.email"
fi

if [[ -n "$GIT_PAT" ]]; then
  git config --global credential.helper store
  CREDENTIALS_FILE="$HOME/.git-credentials"
  touch "$CREDENTIALS_FILE"
  chmod 600 "$CREDENTIALS_FILE" 2>/dev/null || true

  if ! grep -q "@github.com" "$CREDENTIALS_FILE" 2>/dev/null; then
    printf "https://x-access-token:%s@github.com\n" "$GIT_PAT" >>"$CREDENTIALS_FILE"
  fi
else
  echo "[postCreate] NOTE: GIT_PAT is not set; HTTPS git credential bootstrap skipped."
fi


echo "[postCreate] Ensuring GitHub CLI (gh) is installed..."
if command -v gh >/dev/null 2>&1; then
  echo "[postCreate] gh is already installed."
else
  echo "[postCreate] gh not found; attempting installation via apt (best-effort)."

  install_gh() {
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y && apt-get install -y gh
  }

  # In devcontainers, sudo is usually available. Support both root and non-root.
  set +e
  if [[ "$(id -u)" == "0" ]]; then
    install_gh
    RC=$?
  elif command -v sudo >/dev/null 2>&1; then
    sudo -n true >/dev/null 2>&1
    if [[ $? -eq 0 ]]; then
      sudo bash -lc "$(declare -f install_gh); install_gh"
      RC=$?
    else
      echo "[postCreate] NOTE: sudo is not available without a password; cannot install gh automatically."
      RC=1
    fi
  else
    echo "[postCreate] NOTE: sudo not available; cannot install gh automatically."
    RC=1
  fi
  set -e

  if [[ $RC -eq 0 ]]; then
    echo "[postCreate] gh installed successfully."
  else
    echo "[postCreate] WARNING: Unable to install gh automatically."
    echo "[postCreate] You can install it manually inside the container (as root), or use SSH auth instead."
  fi
fi

