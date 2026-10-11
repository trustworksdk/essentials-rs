#!/usr/bin/env bash
# =============================================================================
# Fetch Plugin Marketplaces (SEED DIRECTORY) — host-side, credential-safe
#
# For users on the Trustworks Claude organization: plugins are delivered
# automatically via organization settings once Claude Code is authenticated
# (e.g. via CLAUDE_CODE_OAUTH_TOKEN or `claude setup-token`). This seed step
# is OPTIONAL for them — if it can't authenticate, plugins still arrive via
# org settings.
#
# For users outside the TW Claude org (contractors, partners, external devs
# with GitHub access): this script bakes plugins into a host-side directory so
# they're available without a TW Claude account.
#
# SECURITY MODEL (do not change without care):
#   - Runs on the HOST via initializeCommand — before every container start.
#   - Clones/updates the marketplace repo into .devcontainer/.plugin-marketplace,
#     which devcontainer.json LIVE bind-mounts (read-only) into the container at
#     /opt/claude-seed/marketplaces. Because it is a bind-mount (not a Dockerfile
#     COPY), every container START — not just a rebuild — serves the freshly
#     pulled plugin versions.
#   - GIT CREDENTIALS STAY ON THE HOST AND NEVER ENTER THE CONTAINER. This is
#     deliberate: the dev container has no GitHub access, which removes a common
#     supply-chain / token-theft attack vector. Keep it that way.
#
# RELIABILITY MODEL:
#   - This script is NON-BLOCKING and NEVER aborts container creation. If it
#     cannot authenticate or reach GitHub, it prints clear guidance and exits 0
#     so the dev container still starts (without pre-seeded plugins).
#   - It NEVER triggers an interactive credential pop-up. Surprise credential
#     dialogs mid-"Reopen in Container" are the #1 source of confusion, so all
#     git operations here run with prompts suppressed. Credentials already
#     cached on the host (via `gh` or a credential manager) are still used —
#     only the blocking pop-up is suppressed so WE control the messaging.
#
# The seeded settings entries are written with "autoUpdate": true so Claude Code
# refreshes the marketplace and updates installed plugins at startup.
# =============================================================================

# NOTE: intentionally NOT using `set -e`. A failed git/jq call must never abort
# container creation — we handle every failure explicitly and exit 0.
set -uo pipefail

# ---------------------------------------------------------------------------
# Suppress ALL interactive credential prompts for git operations in this script.
# Cached host credentials still work; only the blocking GUI/terminal prompt is
# suppressed, so a missing/invalid credential fails FAST and cleanly instead of
# popping a confusing dialog. We then print our own guidance.
# ---------------------------------------------------------------------------
export GIT_TERMINAL_PROMPT=0   # no terminal username/password prompt
export GCM_INTERACTIVE=never   # Git Credential Manager: never show its GUI
export GIT_ASKPASS=true        # disable the askpass GUI fallback (`true` = no creds)

# ---------------------------------------------------------------------------
# Marketplace Registry
# Format: "marketplace-name|repo-url"
# The marketplace name becomes the directory name under the plugin seed dir.
# Add more entries to include additional marketplaces.
# ---------------------------------------------------------------------------
MARKETPLACES=(
  "trustworks-plugins|https://github.com/trustworksdk/plugin-marketplace.git"
)

# Directory where marketplaces are cloned (inside .devcontainer/, gitignored)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEVCONTAINER_DIR="$(dirname "$SCRIPT_DIR")"
MARKETPLACE_DIR="$DEVCONTAINER_DIR/.plugin-marketplace"
mkdir -p "$MARKETPLACE_DIR"

# The clone is managed, not a working copy: Claude Code in the container loads
# it as a plugin marketplace, and this script runs on the HOST. Every git call
# below runs with an EMPTY hooks directory and fsmonitor off — a hook planted in
# the clone's .git/hooks (reference-transaction fires on fetch and reset) or a
# core.fsmonitor program in its .git/config would otherwise run on the host —
# and an existing clone is reset to upstream on every start. These checks do
# not make a container-writable clone safe (.git/config has more such keys);
# the control is devcontainer.json's read-only mount of .devcontainer/.
NO_HOOKS_DIR="$(mktemp -d 2>/dev/null || echo "")"
if [ -n "$NO_HOOKS_DIR" ]; then
  trap 'rm -rf "$NO_HOOKS_DIR"' EXIT
  git() { command git -c core.hooksPath="$NO_HOOKS_DIR" -c core.fsmonitor=false "$@"; }
else
  git() { command git -c core.fsmonitor=false "$@"; }
fi

# ---------------------------------------------------------------------------
# Up-front banner (printed on every start, kept short). Tells the developer
# what is about to happen and how to authenticate the HOST — BEFORE any git
# operation runs, so the guidance is never "after the fact".
# ---------------------------------------------------------------------------
cat <<'EOF'

==================================================================
 Trustworks plugin marketplace — host-side fetch
==================================================================
 Runs on your HOST (not in the container) and clones the PRIVATE
 marketplace repo using your host GitHub access. Your git
 credentials STAY on the host and never enter the dev container —
 that is deliberate: the container has no GitHub access, which
 removes a common attack vector.

 Because the repo is private, your host must be authenticated to
 GitHub. The reliable one-time setup is:

     gh auth login
     gh auth setup-git

 If the fetch below cannot authenticate, the container STILL starts
 (without pre-seeded plugins) and full troubleshooting is printed.
==================================================================
EOF

# ---------------------------------------------------------------------------
# Detailed, correct auth guidance — printed only when access actually fails.
# ---------------------------------------------------------------------------
print_auth_help() {
  cat <<'EOF'

  ------------------------------------------------------------------
  Could not access the marketplace repo from the host.

  The dev container will STILL start — plugins just won't be
  pre-seeded. (Trustworks Claude-org members receive plugins via
  org settings regardless of this step.)

  To enable pre-seeded plugins, authenticate on your HOST and then
  rebuild / reopen in container:

  RECOMMENDED — GitHub CLI (handles the org's SAML SSO for you):
      # Don't have the gh CLI yet? GitHub's OFFICIAL sources are
      # https://docs.github.com/en/github-cli and https://cli.github.com
      # (both are GitHub-owned and point to the github.com/cli/cli repo).
      # Install via GitHub's recommended package managers — the binary is
      # GitHub-built; the manifests are community-maintained:
      #   macOS:   brew install gh
      #   Windows: winget install --id GitHub.cli   (or: choco/scoop install gh)
      # Strictly first-party (GitHub-built & signed) — .msi / signed binaries:
      #   https://github.com/cli/cli/releases
      #   Verify provenance: gh attestation verify <downloaded-file> --owner cli
      gh auth login
      gh auth setup-git          # make git use the gh credential

  ALTERNATIVE — Personal Access Token (PAT):
    1. Create a token at https://github.com/settings/tokens
         classic:      'repo' scope
         fine-grained: read access to trustworksdk/plugin-marketplace
    2. IMPORTANT — the trustworksdk org enforces SAML SSO. On the
       token page click "Configure SSO" -> Authorize for
       'trustworksdk'. Without this the token returns HTTP 403 even
       though it looks valid. This is the #1 cause of "my PAT does
       not work".
    3. When git asks, use the token as the PASSWORD (any username).

  Already entered a credential and it KEEPS failing? Your credential
  manager cached the bad one and reuses it silently. Clear it:
      gh auth login                 # overwrites the stored credential
      # or, plain git credential manager:
      printf 'protocol=https\nhost=github.com\n\n' | git credential reject
      # Windows: open "Credential Manager" -> Windows Credentials ->
      #          remove the github.com entry, then retry.

  Windows note: initializeCommand needs bash + git on PATH
  (Git for Windows provides both).
  ------------------------------------------------------------------

EOF
}

ANY_SKIPPED=0

for entry in "${MARKETPLACES[@]}"; do
  MARKETPLACE_NAME="${entry%%|*}"
  REPO_URL="${entry#*|}"
  TARGET_DIR="$MARKETPLACE_DIR/$MARKETPLACE_NAME"

  echo ""
  echo "=== Plugin marketplace: $MARKETPLACE_NAME ==="
  echo "    Source: $REPO_URL"

  # -------------------------------------------------------------------------
  # Verify access WITHOUT triggering an interactive prompt. If the repo is
  # reachable (public, or cached host credentials are valid) this succeeds
  # silently; otherwise it fails fast and we print guidance.
  # -------------------------------------------------------------------------
  if ! git ls-remote "$REPO_URL" HEAD >/dev/null 2>&1; then
    echo "    Could not authenticate / reach the repo from the host."
    print_auth_help
    ANY_SKIPPED=1
    continue
  fi
  echo "    Access verified."

  # -------------------------------------------------------------------------
  # Clone or update (non-fatal — a failure here also degrades gracefully).
  # -------------------------------------------------------------------------
  # An existing clone is reused only while its origin is still $REPO_URL, and
  # it is forced to match upstream exactly: reset --hard + clean -ffdx discard
  # any edit, tracked or untracked (`pull --ff-only` kept them). Develop plugins
  # in a separate checkout, never in .plugin-marketplace/.
  if [ -d "$TARGET_DIR/.git" ] \
     && [ "$(git -C "$TARGET_DIR" remote get-url origin 2>/dev/null)" != "$REPO_URL" ]; then
    echo "    Clone's origin is not $REPO_URL. Re-cloning..."
    rm -rf "$TARGET_DIR"
  fi
  if [ -d "$TARGET_DIR/.git" ]; then
    echo "    Updating to upstream (local changes in the clone are discarded)..."
    if git -C "$TARGET_DIR" fetch --quiet origin 2>/dev/null \
       && git -C "$TARGET_DIR" reset --quiet --hard '@{u}' 2>/dev/null \
       && git -C "$TARGET_DIR" clean -ffdx --quiet 2>/dev/null; then
      echo "    Updated to $(git -C "$TARGET_DIR" rev-parse --short HEAD)"
    else
      echo "    Update failed. Re-cloning..."
      rm -rf "$TARGET_DIR"
      if git clone --quiet "$REPO_URL" "$TARGET_DIR"; then
        echo "    Re-cloned at $(git -C "$TARGET_DIR" rev-parse --short HEAD)"
      else
        echo "    Re-clone failed."
        print_auth_help
        ANY_SKIPPED=1
        continue
      fi
    fi
  else
    echo "    Cloning..."
    if git clone --quiet "$REPO_URL" "$TARGET_DIR"; then
      echo "    Cloned at $(git -C "$TARGET_DIR" rev-parse --short HEAD)"
    else
      echo "    Clone failed."
      print_auth_help
      ANY_SKIPPED=1
      continue
    fi
  fi
done

echo ""
if [ "$ANY_SKIPPED" -eq 1 ]; then
  echo "Some marketplaces were skipped (see guidance above). Continuing — the container will still start."
else
  echo "All plugin marketplaces are up to date."
fi
echo ""

# =============================================================================
# Sync extraKnownMarketplaces in .claude/settings.json
#
# Single source of truth: the MARKETPLACES array above. This rewrites entries
# under .extraKnownMarketplaces whose source.path points into the seed
# directory (/opt/claude-seed/marketplaces/), while preserving any other
# user-managed entries and other top-level settings.
#
# Non-fatal: if settings.json or jq is missing we warn and exit 0 — never block
# container creation over the settings sync.
# =============================================================================
PROJECT_ROOT="$(dirname "$DEVCONTAINER_DIR")"
SETTINGS_FILE="$PROJECT_ROOT/.claude/settings.json"

if [ ! -f "$SETTINGS_FILE" ]; then
  echo "Skipping settings.json sync — $SETTINGS_FILE not found."
  echo "(Claude Code settings will not be auto-populated.)"
  echo ""
  exit 0
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "NOTE: jq is not installed on the host, so .claude/settings.json was NOT"
  echo "synced (the container still starts). Install jq and rebuild to sync"
  echo "plugin marketplaces into settings.json:"
  echo "  macOS:         brew install jq"
  echo "  Debian/Ubuntu: sudo apt-get install jq"
  echo "  Fedora/RHEL:   sudo dnf install jq"
  echo "  Windows:       winget install jqlang.jq   (or: choco install jq / scoop install jq)"
  echo ""
  exit 0
fi

# Build the object of seeded entries from the MARKETPLACES array.
SEEDED_JSON='{}'
for entry in "${MARKETPLACES[@]}"; do
  NAME="${entry%%|*}"
  SEEDED_JSON=$(jq --arg name "$NAME" '
    . + {
      ($name): {
        "source": {
          "source": "directory",
          "path": ("/opt/claude-seed/marketplaces/" + $name)
        },
        "autoUpdate": true
      }
    }
  ' <<< "$SEEDED_JSON")
done

# Rewrite settings.json: drop existing seed-owned entries, add current ones,
# leave everything else (permissions, sandbox, other marketplaces) untouched.
TMP_SETTINGS=$(mktemp)
if jq --argjson seeded "$SEEDED_JSON" '
  .extraKnownMarketplaces //= {}
  | .extraKnownMarketplaces |= with_entries(
      select((.value.source.path // "") | startswith("/opt/claude-seed/marketplaces/") | not)
    )
  | .extraKnownMarketplaces += $seeded
  | if (.extraKnownMarketplaces | length) == 0 then del(.extraKnownMarketplaces) else . end
' "$SETTINGS_FILE" > "$TMP_SETTINGS"; then
  mv "$TMP_SETTINGS" "$SETTINGS_FILE"
  echo "Synced .claude/settings.json: ${#MARKETPLACES[@]} seeded marketplace(s) in extraKnownMarketplaces."
else
  rm -f "$TMP_SETTINGS"
  echo "NOTE: settings.json sync failed (left unchanged). The container still starts."
fi
echo ""
exit 0
