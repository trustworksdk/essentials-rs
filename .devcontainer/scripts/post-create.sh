#!/usr/bin/env bash
# =============================================================================
# DevContainer Post-Create Script
# Runs after the container is created to perform runtime setup
# =============================================================================
set -e

echo "Running post-create setup..."

# Problems that leave the container short of what its config asked for. Each
# step warns and carries on, so one failed install does not stop the rest; the
# list is printed as a banner at the very end, where it cannot scroll past.
SETUP_PROBLEMS=()
setup_problem() {
    SETUP_PROBLEMS+=("$1")
    echo "  WARNING: $1"
}

# =============================================================================
# Directory Ownership
# =============================================================================
echo "Ensuring directory permissions..."

# Nothing in this script can repair a root-owned path: vscode has no sudo (see
# the Dockerfile's sudo block; the firewall rules there never cover chown). The
# Dockerfile pre-creates every volume mountpoint as vscode, so this is a volume
# from an older build, or one a root process wrote into. Name the host-side fix.
fix_owner() {
    setup_problem "$1 is not owned by vscode. From the host: docker exec -u root ${PROJECT_NAME:-<project>}-devcontainer chown -R vscode:vscode $1"
}

# Ensure vscode user owns all home directories
directories=(
    "$HOME/.npm"
    "$HOME/.cargo"
    "$HOME/.rustup"
    "$HOME/.cache"
    "$HOME/.local"
    "$HOME/.config"
    "$HOME/.claude"
)

for dir in "${directories[@]}"; do
    if [ -d "$dir" ]; then
        # Only fix ownership if not already correct
        if [ "$(stat -c '%U' "$dir" 2>/dev/null)" != "vscode" ]; then
            fix_owner "$dir"
        fi
    fi
done

# In-repo build directories on volumes (BUILD_DIR_VOLUMES). The Dockerfile
# pre-creates them as vscode, so a root-owned one is a volume created before
# that path was added, or a path missing from the build arg.
for d in ${BUILD_DIR_VOLUMES:-}; do
    if [ -d "/workspace/$d" ] && [ "$(stat -c '%U' "/workspace/$d" 2>/dev/null)" != "vscode" ]; then
        fix_owner "/workspace/$d"
    fi
done

# uv cache. UV_CACHE_DIR comes from devcontainer.json "remoteEnv", which reaches
# vscode's processes only — never root's, so a root-context `uv` run cannot
# leave root-owned entries in this volume. The default covers a tool that does
# not pass remoteEnv to lifecycle commands. The check looks two levels down: a
# root-owned bucket (e.g. sdists-v9) under a vscode-owned top directory makes
# every `uv` command fail with "Failed to initialize cache … Permission denied".
export UV_CACHE_DIR="${UV_CACHE_DIR:-$HOME/.uv-cache}"
mkdir -p "$UV_CACHE_DIR" 2>/dev/null || true
if [ -d "$UV_CACHE_DIR" ] && [ -n "$(find "$UV_CACHE_DIR" -maxdepth 2 ! -user vscode -print -quit 2>/dev/null)" ]; then
    fix_owner "$UV_CACHE_DIR"
fi

# =============================================================================
# NPM Configuration
# =============================================================================
echo "Configuring NPM..."

if command -v npm &> /dev/null; then
    # Configure npm script security based on ALLOW_NPM_SCRIPTS
    if [ "${ALLOW_NPM_SCRIPTS:-false}" = "true" ]; then
        echo "  NPM scripts are ENABLED (ignore-scripts=false)"
        npm config set ignore-scripts false
    else
        echo "  NPM scripts are DISABLED for security (ignore-scripts=true)"
        echo "     To run scripts manually: npm run <script> --ignore-scripts=false"
        npm config set ignore-scripts true
    fi

    # Node is installed via the devcontainer `node` feature, which uses nvm.
    # nvm is incompatible with `prefix` / `globalconfig` in ~/.npmrc and emits
    # `Your user's .npmrc file ... has a globalconfig and/or a prefix setting`
    # on every shell startup if either is set. Globals under nvm live in
    # $NVM_DIR/versions/node/<version>/bin — already on PATH via nvm.sh, no
    # custom prefix needed. Heal any leftover entries from earlier setups that
    # wrote `prefix=$HOME/.npm-global` into these dotfiles (both ~/.npmrc and
    # ~/.bashrc are on a persisted volume, so they survive container rebuilds).
    if [ -f "$HOME/.npmrc" ]; then
        sed -i '/^[[:space:]]*prefix[[:space:]]*=/d; /^[[:space:]]*globalconfig[[:space:]]*=/d' "$HOME/.npmrc"
    fi
    if [ -f "$HOME/.bashrc" ]; then
        sed -i '\|export PATH="\$HOME/\.npm-global/bin:\$PATH"|d' "$HOME/.bashrc"
    fi

    echo "  Node.js version: $(node --version)"
    echo "  NPM version: $(npm --version)"
fi

# =============================================================================
# Rust / Cargo (Conditional)
# Installed here (not in the Dockerfile) because ~/.cargo and ~/.rustup are
# volume-mounted — a build-time install would be masked by the stale volume on
# rebuild. The install is idempotent: rustup is only
# bootstrapped once; later runs just re-assert the default toolchain. rustfmt
# and clippy ship in rustup's default profile; rust-analyzer + rust-src are
# added explicitly so `rust-analyzer` lands on PATH (as a rustup proxy in
# ~/.cargo/bin) for external Claude Code LSP plugins — symmetric with
# jdtls/csharp-ls/pyright/typescript-language-server.
# =============================================================================
if [ "${INSTALL_RUST:-false}" = "true" ]; then
    RUST_CHANNEL="${RUST_TOOLCHAIN:-stable}"

    if ! command -v rustup &> /dev/null; then
        echo "Installing Rust via rustup (channel: ${RUST_CHANNEL})..."
        # --no-modify-path keeps PATH management in our profile.d/.bashrc entries
        # rather than rustup's own shell-rc injection.
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
            | sh -s -- -y --no-modify-path --default-toolchain "${RUST_CHANNEL}"
    else
        echo "Rust already installed — ensuring default toolchain ${RUST_CHANNEL}..."
        rustup default "${RUST_CHANNEL}" 2>/dev/null || true
    fi

    # Source cargo env for the rest of this script.
    # shellcheck disable=SC1091
    [ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"

    if command -v rustup &> /dev/null; then
        # rust-analyzer (LSP) + rust-src (rust-analyzer needs std sources for
        # completion/go-to-def). rustfmt + clippy are already in the default profile.
        rustup component add rust-analyzer rust-src 2>/dev/null \
            || echo "  WARNING: 'rustup component add rust-analyzer rust-src' failed."

        echo "  Rust version: $(rustc --version 2>/dev/null)"
        echo "  Cargo version: $(cargo --version 2>/dev/null)"

    else
        echo "  WARNING: rustup not found after install. Retry manually: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    fi
fi

# =============================================================================
# Language Servers (always-on for baseline runtimes)
# typescript-language-server — requires npm (Node.js DevContainer feature, only
#   available at runtime, not at Dockerfile build time).
# pyright — requires uv (installed at Dockerfile build time).
# Both installs are idempotent: skip if the binary is already on PATH.
# =============================================================================
echo "Setting up language servers..."

# TypeScript LSP. --ignore-scripts matches the project's NPM security posture.
# typescript is pinned to the 6.x line: it is the last release that ships
# lib/tsserver.js, which typescript-language-server requires. typescript@7 (the
# native Go port, npm "latest" since 7.0) ships no tsserver.js, so an unpinned
# install leaves the server unable to initialise and Claude Code's LSP tool
# silently never comes up. The guard also checks for tsserver.js so a container
# that already holds a broken 7.x next to the server heals on the next run.
# Revisit the pin once Claude Code's typescript-lsp plugin drives `tsgo --lsp`.
if command -v npm &> /dev/null; then
    if ! command -v typescript-language-server &> /dev/null \
        || [ ! -f "$(npm root -g)/typescript/lib/tsserver.js" ]; then
        echo "  Installing typescript-language-server + typescript@6 (npm)..."
        npm install -g --ignore-scripts typescript@6 typescript-language-server \
            || echo "  WARNING: typescript-language-server install failed."
    fi
fi

# Pyright (uv tool).
if command -v uv &> /dev/null; then
    if ! command -v pyright &> /dev/null; then
        echo "  Installing pyright (uv tool)..."
        UV_TOOL_BIN_DIR="$HOME/.local/bin" \
        UV_TOOL_DIR="$HOME/.local/share/uv/tools" \
        uv tool install pyright \
            || echo "  WARNING: pyright install failed."
    fi
fi

# =============================================================================
# Claude Code CLI Installation (native install)
# =============================================================================
if [ "${INSTALL_CLAUDE:-false}" = "true" ]; then
    echo "Installing Claude Code CLI..."

    curl -fsSL https://claude.ai/install.sh | bash
    echo "  Claude Code CLI installed"
fi

# =============================================================================
# Claude Code default permission mode → "auto" (Conditional)
# Auto mode MUST live in USER settings ($CLAUDE_CONFIG_DIR/settings.json, default
# ~/.claude/settings.json): as of Claude Code v2.1.142 the "auto" value of
# permissions.defaultMode is IGNORED in project/local .claude/settings.json so a
# repo can't grant itself auto mode. It therefore cannot be baked into the
# committed project settings the way acceptEdits/bypassPermissions are — those
# are written to .claude/settings.json at generation time and work fine there.
# Gated on CLAUDE_DEFAULT_MODE_AUTO, which the generator sets in containerEnv
# only when the user chose Auto mode. Best-effort: auto mode also needs an
# eligible account (Claude Code v2.1.83+); an ineligible session silently starts
# in "default" mode. The merge is idempotent so it survives rebuilds without
# clobbering other user settings.
# =============================================================================
if [ "${CLAUDE_DEFAULT_MODE_AUTO:-false}" = "true" ] && [ "${INSTALL_CLAUDE:-false}" = "true" ]; then
    echo "Setting Claude Code default permission mode to 'auto' (user settings)..."
    CLAUDE_USER_SETTINGS="${CLAUDE_CONFIG_DIR:-$HOME/.claude}/settings.json"
    mkdir -p "$(dirname "$CLAUDE_USER_SETTINGS")"
    if command -v jq &> /dev/null; then
        _tmp="$(mktemp)"
        if [ -s "$CLAUDE_USER_SETTINGS" ] && jq -e . "$CLAUDE_USER_SETTINGS" > /dev/null 2>&1; then
            jq '.permissions.defaultMode = "auto"' "$CLAUDE_USER_SETTINGS" > "$_tmp" && mv "$_tmp" "$CLAUDE_USER_SETTINGS"
        else
            # Missing/empty/invalid file — start fresh.
            jq -n '{ permissions: { defaultMode: "auto" } }' > "$CLAUDE_USER_SETTINGS"
            rm -f "$_tmp"
        fi
        echo "  → permissions.defaultMode=auto in $CLAUDE_USER_SETTINGS"
    else
        echo "  WARNING: jq not found; skipping auto-mode default (set permissions.defaultMode=auto in ~/.claude/settings.json manually)."
    fi
fi

# =============================================================================
# Claude Code status line (Conditional)
# Renders model, context usage, git branch and worktree in the footer:
#   <model>[:effort] [<fast>] │ <used>/<window> (<pct>%) │ <branch> [<worktree>] │ <dir>
# Claude Code has NO built-in setting for this (no showContext / contextMeter key
# exists) — statusLine is the only supported route, and it runs a command per
# render with the session state as JSON on stdin.
#
# USER settings, not project settings, on purpose: project settings WIN over user
# settings, so a tracked statusLine would silently replace whatever footer every
# contributor already chose. A footer is a personal display preference, unlike
# worktree.baseRef which is a repo-level opinion about behaviour.
#
# Idempotent and non-destructive: an existing statusLine (the developer's own, or
# one from a previous run pointing elsewhere) is never overwritten. To re-point it
# at this script, delete the statusLine key and re-create the container.
# =============================================================================
if [ "${CLAUDE_STATUSLINE:-false}" = "true" ] && [ "${INSTALL_CLAUDE:-false}" = "true" ]; then
    echo "Configuring Claude Code status line (user settings)..."
    # Sibling of this script — never hardcode /workspace, the generated project's
    # workspace folder is whatever the user named it.
    STATUSLINE_SH="$(cd "$(dirname "$0")" && pwd)/statusline.sh"
    CLAUDE_USER_SETTINGS="${CLAUDE_CONFIG_DIR:-$HOME/.claude}/settings.json"
    if [ ! -f "$STATUSLINE_SH" ]; then
        echo "  WARNING: $STATUSLINE_SH not found; skipping status line."
    elif ! command -v jq &> /dev/null; then
        echo "  WARNING: jq not found; skipping status line."
    else
        chmod +x "$STATUSLINE_SH" 2>/dev/null || true
        mkdir -p "$(dirname "$CLAUDE_USER_SETTINGS")"
        if [ ! -s "$CLAUDE_USER_SETTINGS" ] || ! jq -e . "$CLAUDE_USER_SETTINGS" > /dev/null 2>&1; then
            echo '{}' > "$CLAUDE_USER_SETTINGS"
        fi
        if [ "$(jq -r 'has("statusLine")' "$CLAUDE_USER_SETTINGS")" = "true" ]; then
            echo "  → statusLine already set in $CLAUDE_USER_SETTINGS — left untouched."
        else
            _tmp="$(mktemp)"
            if jq --arg cmd "$STATUSLINE_SH" \
                  '.statusLine = { type: "command", command: $cmd, padding: 0 }' \
                  "$CLAUDE_USER_SETTINGS" > "$_tmp"; then
                mv "$_tmp" "$CLAUDE_USER_SETTINGS"
                echo "  → statusLine → $STATUSLINE_SH"
            else
                rm -f "$_tmp"
                echo "  WARNING: failed to write statusLine to $CLAUDE_USER_SETTINGS."
            fi
        fi
    fi
fi

# =============================================================================
# claude-rc — Remote Control launcher (with Claude Code)
# Puts `claude-rc` on PATH: a two-line shim that runs the sibling claude-rc.sh,
# which starts `claude --remote-control` with the variables that block Remote
# Control cleared for that one process (DO_NOT_TRACK stays container-wide; see
# the script's header). A shim rather than a symlink: .devcontainer/ is
# read-only in here and a Windows checkout drops the exec bit, so `bash <file>`
# is the only form that always works. ~/.local/bin is in the image, not a
# volume, so the shim is rewritten on every create and follows the script's
# location if the workspace folder is renamed.
# =============================================================================
if [ "${INSTALL_CLAUDE:-false}" = "true" ]; then
    CLAUDE_RC_SH="$(cd "$(dirname "$0")" && pwd)/claude-rc.sh"
    if [ -f "$CLAUDE_RC_SH" ]; then
        mkdir -p "$HOME/.local/bin"
        printf '#!/usr/bin/env bash\nexec bash %q "$@"\n' "$CLAUDE_RC_SH" > "$HOME/.local/bin/claude-rc"
        chmod 0755 "$HOME/.local/bin/claude-rc"
        echo "  → claude-rc → $CLAUDE_RC_SH"
    else
        echo "  WARNING: $CLAUDE_RC_SH not found; skipping the claude-rc launcher."
    fi
fi

# =============================================================================
# Knowledge-graph indexer resolver (three-way mutual exclusion)
# At most one indexer may install: GitNexus, CodeGraph, or graphify. The
# generator never emits more than one as "true", but if a hand-edit sets
# several, first-enabled wins by this precedence (GITNEXUS > CODEGRAPH >
# GRAPHIFY, preserving the historical "GitNexus wins" behaviour). Each indexer
# block below guards on EFFECTIVE_INDEXER so only the winner runs.
# =============================================================================
EFFECTIVE_INDEXER="none"
if   [ "${INSTALL_GITNEXUS:-false}"  = "true" ]; then EFFECTIVE_INDEXER="gitnexus"
elif [ "${INSTALL_CODEGRAPH:-false}" = "true" ]; then EFFECTIVE_INDEXER="codegraph"
elif [ "${INSTALL_GRAPHIFY:-false}"  = "true" ]; then EFFECTIVE_INDEXER="graphify"
fi
# Warn once if more than one indexer flag was set (a misconfiguration).
_indexer_count=0
for _f in "${INSTALL_GITNEXUS:-false}" "${INSTALL_CODEGRAPH:-false}" "${INSTALL_GRAPHIFY:-false}"; do
    [ "$_f" = "true" ] && _indexer_count=$((_indexer_count + 1))
done
if [ "$_indexer_count" -gt 1 ]; then
    echo "  WARNING: multiple knowledge-graph indexers enabled — they are mutually exclusive. Installing '${EFFECTIVE_INDEXER}' only (precedence: GitNexus > CodeGraph > graphify). Set the others to false in devcontainer.json."
fi

# =============================================================================
# CodeGraph Knowledge Graph (Conditional, experimental)
# Mutually exclusive with GitNexus and graphify (see the indexer resolver
# above). CodeGraph is a plain MCP server (not a Claude Code plugin), so we
# install the global CLI, run the non-interactive `codegraph init --yes` (init +
# initial index), and write a project-scoped .mcp.json registering it as
# `codegraph serve --mcp` (stdio transport). Auto-syncs on file events with a
# 2s debounce; rerun `codegraph index` after large refactors to be safe.
# =============================================================================
if [ "${EFFECTIVE_INDEXER}" = "codegraph" ]; then
        echo "Setting up CodeGraph (experimental)..."

        if ! npm install -g @colbymchenry/codegraph 2>&1; then
            echo "  WARNING: CodeGraph install failed. You can retry later with: npm install -g @colbymchenry/codegraph"
        else
            # Disable telemetry (defence-in-depth alongside the CODEGRAPH_TELEMETRY=0
            # and DO_NOT_TRACK=1 containerEnv vars). This persists the opt-out into
            # CodeGraph's own config, so it sticks even if the env vars are ever
            # dropped. Idempotent — safe to run on every container start / skill rerun.
            echo "  Disabling CodeGraph telemetry..."
            codegraph telemetry off 2>&1 || echo "  NOTE: 'codegraph telemetry off' failed (env vars CODEGRAPH_TELEMETRY=0 / DO_NOT_TRACK=1 still apply)."

            # Report a root-owned mountpoint. Docker creates the mount point at
            # volume-attach time as root:root if it didn't exist in the image —
            # fixed at build time, so this catches a volume from an older build.
            if [ -d /workspace/.codegraph ] && [ "$(stat -c '%U' /workspace/.codegraph 2>/dev/null)" != "vscode" ]; then
                fix_owner /workspace/.codegraph
            fi

            # Initial index. `init` always indexes (`-i` is a no-op since 1.6).
            # `--yes` is load-bearing: VS Code runs postCreateCommand on a pty, so
            # CodeGraph sees a TTY and its post-index offers (e.g. "Index it now?"
            # for a gitignored nested repo such as .devcontainer/.plugin-marketplace/)
            # block on a prompt the read-only creation terminal cannot answer.
            # `--yes` takes the defaults and prints the opt-in hint instead; the
            # </dev/null stops any future prompt that ignores --yes from hanging.
            if [ -d "/workspace" ] && [ "$(ls -A /workspace 2>/dev/null)" ]; then
                echo "  Indexing codebase (codegraph init --yes)..."
                cd /workspace && codegraph init --yes </dev/null 2>&1 \
                    || echo "  WARNING: CodeGraph indexing failed. post-create.sh runs only on container creation, so retry by hand: cd /workspace && codegraph index"
            else
                echo "  Workspace empty — skipping initial index. Run 'codegraph init' after cloning your project."
            fi

            # Write project-scoped .mcp.json so Claude Code discovers the server.
            # Only write if the file is absent — we don't want to clobber a user's config.
            if [ -d "/workspace" ] && [ ! -f /workspace/.mcp.json ]; then
                cat > /workspace/.mcp.json <<'JSON'
{
  "mcpServers": {
    "codegraph": {
      "type": "stdio",
      "command": "codegraph",
      "args": ["serve", "--mcp"]
    }
  }
}
JSON
                echo "  Wrote /workspace/.mcp.json registering codegraph MCP server."
            elif [ -f /workspace/.mcp.json ] && ! grep -q '"codegraph"' /workspace/.mcp.json 2>/dev/null; then
                echo "  NOTE: .mcp.json already exists but does not include codegraph. Add the codegraph entry manually:"
                echo '       {"mcpServers":{"codegraph":{"type":"stdio","command":"codegraph","args":["serve","--mcp"]}}}'
            fi
        fi

        echo "  CodeGraph setup complete"
fi

# =============================================================================
# Native LSP layer (Conditional) — Claude Code's built-in LSP tool
# The graph/compression/governance tools above are tree-sitter (syntactic) only;
# they provide NO compiler-accurate go-to-definition, find-references, hover
# types, or live diagnostics. This layer supplies that via Claude Code's native
# LSP tool + per-language Code-Intelligence plugins from the OFFICIAL,
# Anthropic-curated marketplace (claude-plugins-official). The language-server
# binaries are already installed/gated elsewhere (jdtls, csharp-ls,
# kotlin-language-server, rust-analyzer, typescript-language-server, pyright) and
# the official plugins require exactly those binaries on PATH. Registration is
# idempotent (claude plugin install no-ops if already present).
# =============================================================================
if [ "${ENABLE_LSP_TOOL:-0}" = "1" ] && [ "${INSTALL_CLAUDE:-false}" = "true" ] && command -v claude &> /dev/null; then
    echo "Registering native LSP plugins (official marketplace)..."
    export PATH="$HOME/.local/bin:$PATH"
    # Ensure the official marketplace is known (auto-available on recent Claude
    # Code; add explicitly as a no-op safety net). Failures are non-fatal.
    claude plugin marketplace add anthropics/claude-plugins-official 2>/dev/null \
        || true

    _lsp_install() {
        # $1 = plugin name on claude-plugins-official
        claude plugin install "$1@claude-plugins-official" --scope project 2>&1 \
            || echo "  INFO: LSP plugin '$1' not installed automatically — install from /plugin (Discover) if needed."
    }

    # Always-on runtimes (Node + Python baselines).
    _lsp_install typescript-lsp
    _lsp_install pyright-lsp
    # Gated per enabled runtime — binary is on PATH only when that runtime is installed.
    [ "${INSTALL_RUST:-false}"       = "true" ] && _lsp_install rust-analyzer-lsp
    echo "  LSP plugin registration complete (ENABLE_LSP_TOOL=1)."
fi

# =============================================================================
# Python/UV Configuration
# =============================================================================
echo "Configuring Python/UV..."

# No /usr/local/bin/python symlink: creating one needs root, which vscode no
# longer has. The Python feature installs under /usr/local/python/current/bin,
# and python.defaultInterpreterPath in devcontainer.json points there directly.

# Verify UV installation
if command -v uv &> /dev/null; then
    echo "  UV version: $(uv --version)"
fi

# Verify Python installation
if command -v python &> /dev/null; then
    echo "  Python version: $(python --version)"
fi

# =============================================================================
# Git Configuration
# =============================================================================
echo "Configuring Git..."

# Source per-developer overrides (git identity, personal env). This file is
# gitignored — copy .devcontainer/.env.local.example to .env.local to create it.
ENV_LOCAL="/workspace/.devcontainer/.env.local"
ENV_LOCAL_EXAMPLE="/workspace/.devcontainer/.env.local.example"
if [ -f "$ENV_LOCAL" ]; then
    echo "  Loading $ENV_LOCAL"
    # Strip CRLF on the fly — .env.local is gitignored, so Windows editors
    # may save it with CRLF and break sourcing (values get a trailing \r).
    # shellcheck disable=SC1090
    set -a
    . <(tr -d '\r' < "$ENV_LOCAL")
    set +a
fi

# Apply git identity (populated by .env.local, or containerEnv if baked).
if [ -n "${GIT_USER_NAME}" ]; then
    git config --global user.name "${GIT_USER_NAME}"
    echo "  Git user.name: ${GIT_USER_NAME}"
fi
if [ -n "${GIT_USER_EMAIL}" ]; then
    git config --global user.email "${GIT_USER_EMAIL}"
    echo "  Git user.email: ${GIT_USER_EMAIL}"
fi

# Big warning when the per-developer .env.local mechanism is the intended
# path (example file exists) but the developer has not created their copy.
# This re-prints on every container start until .env.local exists, so it's
# hard to miss.
if [ -f "$ENV_LOCAL_EXAMPLE" ] && [ ! -f "$ENV_LOCAL" ]; then
    cat <<'WARN'

============================================================================

  WARNING: GIT IDENTITY NOT CONFIGURED

  .devcontainer/.env.local is missing. Git has no name/email — your
  next `git commit` will fail (or record an empty author).

  Quick setup — run in the project root (host or container):

      cp .devcontainer/.env.local.example .devcontainer/.env.local

  Then edit .devcontainer/.env.local and set your name + email.
  Re-run post-create.sh, or rebuild the container, to apply:

      bash .devcontainer/scripts/post-create.sh

  Details: README.md > Git Identity

============================================================================

WARN
elif [ -z "${GIT_USER_NAME}" ] && [ -z "${GIT_USER_EMAIL}" ]; then
    # Bake strategy was chosen but values were left empty, or someone
    # deleted .env.local.example. Smaller hint — there's no canonical fix.
    echo "  Git identity not configured. Set GIT_USER_NAME / GIT_USER_EMAIL"
    echo "  in devcontainer.json -> containerEnv, or run"
    echo "  'git config --global user.name/email' inside the container."
    echo "  See README.md -> Git Identity for details."
fi

# safe.directory is an exact path, so /workspace does not cover the isolation worktrees Claude
# Code creates under /workspace/.claude/worktrees/; the bind mount intermittently reports another
# owner, and git then refuses a fresh worktree ("dubious ownership"). --replace-all --fixed-value
# keeps one entry each instead of appending a duplicate on every run.
git config --global --replace-all --fixed-value safe.directory /workspace /workspace 2>/dev/null || true
git config --global --replace-all --fixed-value safe.directory '/workspace/.claude/worktrees/*' '/workspace/.claude/worktrees/*' 2>/dev/null || true

# Bind-mount stat-race mitigation. /workspace is a `consistency=delegated` bind
# mount, so inode/ctime drift makes git's default stat check report phantom
# "local changes" on a clean tree — which aborts `git rebase`/`checkout` with
# "Your local changes ... would be overwritten" (the file list even varies run
# to run). checkStat=minimal compares only mtime+size (ignoring ctime/inode/
# uid/gid/dev) and trustctime=false ignores ctime, removing the false positives
# while still detecting real edits. Per-clone local config, so re-applied on
# every container rebuild.
git -C /workspace config core.checkStat minimal 2>/dev/null || true
git -C /workspace config core.trustctime false 2>/dev/null || true

git config --global pull.rebase true 2>/dev/null || true

# =============================================================================
# Docker Content Trust (Conditional)
# =============================================================================
if [ "${ENABLE_DCT:-false}" = "true" ]; then
    echo "  Docker Content Trust is ENABLED"
    echo 'export DOCKER_CONTENT_TRUST=1' >> "$HOME/.bashrc"
else
    echo "  Docker Content Trust is disabled (most images are unsigned)"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=============================================="
echo "DevContainer Setup Complete! (${PROJECT_NAME:-devcontainer})"
echo "=============================================="
echo ""
echo "Installed Runtimes:"
echo "  Node.js + NPM"
echo "  Python + UV"
if [ "${INSTALL_RUST:-false}" = "true" ]; then
    echo "  Rust + Cargo (rustfmt, clippy, rust-analyzer)"
    [ "${INSTALL_RUST_WASM:-false}" = "true" ] && echo "    → WASM tooling: wasm32-unknown-unknown, trunk, cargo-leptos"
fi
if [ "${INSTALL_CLAUDE:-false}" = "true" ]; then
    echo "  Claude Code CLI"
    if [ -n "${CLAUDE_CODE_OAUTH_TOKEN:-}" ]; then
        echo "    → Authenticated via CLAUDE_CODE_OAUTH_TOKEN"
    else
        echo "    → To authenticate: run 'claude' and follow the OAuth flow."
        echo "      If the browser shows 'localhost refused to connect', the sign-in"
        echo "      page will instead display a login code — paste it at the CLI's"
        echo "      'Paste code here if prompted:' prompt. Credentials are saved to"
        echo "      ~/.claude/ and persist across rebuilds via the claude-config volume."
        echo ""
        echo "      If subscription tokens keep expiring, use 'claude setup-token'"
        echo "      (1-year token) and add it to ~/.bashrc:"
        echo "        echo 'export CLAUDE_CODE_OAUTH_TOKEN=<token>' >> ~/.bashrc"
        echo ""
        echo "      See 'Authenticating Claude Code' at the top of README.md for details."
    fi
fi
if [ "${INSTALL_CLAUDE:-false}" = "true" ] && [ -x "$HOME/.local/bin/claude-rc" ]; then
    echo "  claude-rc (Claude Code with Remote Control, telemetry opt-out kept)"
    echo "    → Run 'claude-rc' instead of 'claude' to continue the session from claude.ai/code or the app"
fi
if [ "${CLAUDE_STATUSLINE:-false}" = "true" ] && [ "${INSTALL_CLAUDE:-false}" = "true" ]; then
    echo "  Status line (model · context usage · branch · worktree)"
    echo "    → Registered in user settings; edit or disable in ~/.claude/settings.json"
fi
echo ""
echo "Installed Language Servers:"
command -v typescript-language-server &> /dev/null && echo "  typescript-language-server"
command -v pyright &> /dev/null && echo "  pyright"
[ "${INSTALL_RUST:-false}" = "true" ] && command -v rust-analyzer &> /dev/null && echo "  rust-analyzer"
if [ "${ENABLE_LSP_TOOL:-0}" = "1" ]; then
    echo "    → Claude Code native LSP tool ENABLED (semantic go-to-def / find-refs / diagnostics)."
    echo "      Prefer the LSP tool over grep for symbol navigation; trust its results."
fi
echo ""

# =============================================================================
# Setup check — keep it last, so the banner is the end of the creation log
# Every tool this config asked for must be on PATH now, whatever made an install
# above fail (each one only warns). The script still exits 0: do not turn this
# into a failure without first checking that postStartCommand, which starts the
# egress firewall, still runs after a failed postCreateCommand.
# =============================================================================
export PATH="$HOME/.local/bin:$HOME/.bun/bin:$HOME/.cargo/bin:$PATH"
require_tool() {
    # $1 = command, $2 = what should have provided it
    if ! command -v "$1" &> /dev/null; then
        SETUP_PROBLEMS+=("$1 is missing ($2)")
    fi
}
require_tool uv "Dockerfile"
require_tool npm "Node.js feature"
require_tool pyright "post-create.sh: uv tool install pyright"
require_tool typescript-language-server "post-create.sh: npm install -g"
if [ "${INSTALL_BUN:-false}" = "true" ]; then require_tool bun "post-create.sh: bun installer"; fi
if [ "${INSTALL_RUST:-false}" = "true" ]; then
    require_tool rustup "post-create.sh: rustup installer"
    require_tool rust-analyzer "post-create.sh: rustup component add"
    if [ "${INSTALL_RUST_WASM:-false}" = "true" ]; then
        require_tool trunk "post-create.sh: cargo install trunk"
        require_tool cargo-leptos "post-create.sh: cargo install cargo-leptos"
    fi
fi
if [ "${INSTALL_JAVA:-false}" = "true" ]; then require_tool jdtls "Dockerfile"; fi
if [ "${INSTALL_KOTLIN_LSP:-false}" = "true" ]; then require_tool kotlin-language-server "Dockerfile"; fi
if [ "${INSTALL_DOTNET:-false}" = "true" ]; then require_tool csharp-ls "Dockerfile"; fi
if [ "${INSTALL_CLAUDE:-false}" = "true" ]; then require_tool claude "post-create.sh: Claude Code installer"; fi
if [ "${EFFECTIVE_INDEXER}" = "codegraph" ]; then require_tool codegraph "post-create.sh: npm install -g"; fi
if [ "${EFFECTIVE_INDEXER}" = "graphify" ]; then require_tool graphify "post-create.sh: uv tool install graphifyy"; fi
if [ "${INSTALL_HEADROOM:-false}" = "true" ]; then require_tool headroom "post-create.sh: uv tool install headroom-ai"; fi
if [ "${INSTALL_SKILLSPECTOR:-false}" = "true" ]; then require_tool skillspector "post-create.sh: uv tool install skillspector"; fi
if [ "${INSTALL_BEADS:-false}" = "true" ]; then require_tool bd "post-create.sh: npm install -g @beads/bd"; fi
if [ "${INSTALL_RTK:-false}" = "true" ]; then require_tool rtk "Dockerfile"; fi
if [ "${INSTALL_SENTRUX:-false}" = "true" ]; then require_tool sentrux "Dockerfile"; fi

if [ "${#SETUP_PROBLEMS[@]}" -gt 0 ]; then
    echo "============================================================================"
    echo ""
    echo "  SETUP INCOMPLETE: ${#SETUP_PROBLEMS[@]} problem(s)"
    echo ""
    for _problem in "${SETUP_PROBLEMS[@]}"; do
        echo "  - $_problem"
    done
    echo ""
    echo "  The WARNING lines above give each cause. Once it is fixed, re-run:"
    echo "      bash .devcontainer/scripts/post-create.sh"
    echo ""
    echo "============================================================================"
    echo ""
fi
