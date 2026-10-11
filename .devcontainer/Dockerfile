# =============================================================================
# Multi-Language DevContainer Dockerfile
# Supports: Rust/Cargo, Node.js/NPM, Python/UV
# Node.js and Python are installed via DevContainer features
# =============================================================================

# -----------------------------------------------------------------------------
# Build Arguments - Project Identity
# Used for container naming, Docker labels, and distinguishing instances.
# -----------------------------------------------------------------------------
ARG PROJECT_NAME="devcontainer"

# -----------------------------------------------------------------------------
# Build Arguments - Installation Toggles (override in devcontainer.json)
# Safe defaults: everything off. devcontainer.json build.args enables what's needed.
# -----------------------------------------------------------------------------
ARG INSTALL_CLAUDE="false"
# Rust toolchain (rustup-managed). INSTALL_RUST_WASM adds the wasm32 target plus
# trunk/cargo-leptos for WASM/full-stack web projects and requires INSTALL_RUST=true.
ARG INSTALL_RUST="false"
ARG INSTALL_RUST_WASM="false"
ARG INSTALL_GITNEXUS="false"
ARG INSTALL_CODEGRAPH="false"
ARG INSTALL_JETBRAINS="false"
ARG INSTALL_DOCS_PIPELINE="false"
ARG BUILD_DIR_VOLUMES=""
ARG ALLOW_NPM_SCRIPTS="true"
ARG ENABLE_DCT="false"

# Git identity is passed via containerEnv in devcontainer.json (not build args)
# to avoid leaking PII into image layers. post-create.sh reads it at runtime.

# -----------------------------------------------------------------------------
# Build Arguments - Version Configuration
# -----------------------------------------------------------------------------
# Rust toolchain channel (stable / beta / nightly). Installed by post-create.sh
# via rustup — the channel string is passed straight to `rustup --default-toolchain`.
ARG RUST_TOOLCHAIN="stable"

# -----------------------------------------------------------------------------
# JetBrains Configuration
# -----------------------------------------------------------------------------
ARG JETBRAINS_BACKEND="IntelliJ"

# -----------------------------------------------------------------------------
# Base Image
# -----------------------------------------------------------------------------
FROM mcr.microsoft.com/devcontainers/base:debian AS base

# Re-declare ARGs after FROM (they don't persist across stages)
ARG PROJECT_NAME
ARG INSTALL_CLAUDE
ARG INSTALL_RUST
ARG INSTALL_RUST_WASM
ARG INSTALL_GITNEXUS
ARG INSTALL_CODEGRAPH
ARG INSTALL_JETBRAINS
ARG INSTALL_DOCS_PIPELINE
ARG BUILD_DIR_VOLUMES
ARG ALLOW_NPM_SCRIPTS
ARG ENABLE_DCT
ARG RUST_TOOLCHAIN
ARG JETBRAINS_BACKEND

# Store build args as environment variables for runtime scripts.
# GIT_USER_NAME and GIT_USER_EMAIL are intentionally NOT stored as ENV —
# they're applied by post-create.sh via git config and don't need to persist
# in image layers (which would leak PII if the image is shared).
ENV PROJECT_NAME=${PROJECT_NAME} \
    INSTALL_CLAUDE=${INSTALL_CLAUDE} \
    INSTALL_RUST=${INSTALL_RUST} \
    INSTALL_RUST_WASM=${INSTALL_RUST_WASM} \
    RUST_TOOLCHAIN=${RUST_TOOLCHAIN} \
    INSTALL_GITNEXUS=${INSTALL_GITNEXUS} \
    INSTALL_CODEGRAPH=${INSTALL_CODEGRAPH} \
    INSTALL_JETBRAINS=${INSTALL_JETBRAINS} \
    INSTALL_DOCS_PIPELINE=${INSTALL_DOCS_PIPELINE} \
    BUILD_DIR_VOLUMES=${BUILD_DIR_VOLUMES} \
    ALLOW_NPM_SCRIPTS=${ALLOW_NPM_SCRIPTS} \
    ENABLE_DCT=${ENABLE_DCT} \
    JETBRAINS_BACKEND=${JETBRAINS_BACKEND}

# -----------------------------------------------------------------------------
# System Setup
# -----------------------------------------------------------------------------
USER root

# Set noninteractive mode to prevent apt prompts during build
ENV DEBIAN_FRONTEND=noninteractive

# Install base system dependencies and configure locales
RUN apt-get update && \
    apt-get -y install --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    apt-transport-https \
    git \
    vim \
    jq \
    tree \
    # ripgrep — installs the `rg` binary. Fast recursive search that agent
    # tooling and interactive shells reach for by default; Debian trixie ships
    # 14.x in main.
    ripgrep \
    htop \
    unzip \
    wget \
    locales \
    build-essential \
    # Sandbox dependencies (bubblewrap + socat for Claude Code native sandbox)
    # Always installed (~200KB) so /sandbox is available without rebuild
    bubblewrap \
    socat && \
    # Generate locales
    sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen && \
    # macOS (and JetBrains Gateway, which forwards the client locale) injects a
    # bare LC_CTYPE=UTF-8 into remote terminals. "UTF-8" is not a valid locale
    # name on Linux, so every shell startup prints:
    #   bash: warning: setlocale: LC_CTYPE: cannot change locale (UTF-8)
    # Setting LANG/LC_ALL does NOT silence it — bash warns about the invalid
    # LC_CTYPE regardless. Defining a locale literally named "UTF-8" (mapped to
    # en_US/UTF-8) makes setlocale() resolve it, so the warning disappears no
    # matter what the IDE/host forwards.
    localedef -i en_US -f UTF-8 UTF-8 && \
    # Apply security updates
    apt-get upgrade -y && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    # Create docker group and add vscode user
    groupadd -f docker && usermod -aG docker vscode && \
    # Set default locale
    update-locale LANG=en_US.UTF-8

# Set locale environment variables
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

# -----------------------------------------------------------------------------
# Node.js - Installed via DevContainer feature (ghcr.io/devcontainers/features/node)
# Node.js v22+ respects container memory limits automatically — no explicit
# --max-old-space-size needed. Override via containerEnv if you hit heap limits.
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Rust / Cargo
# rustup itself is installed as the vscode user in post-create.sh (it lands in
# ~/.cargo + ~/.rustup, both volume-mounted — a build-time install would be
# masked by the stale volume on rebuild). Here we only write the profile.d entry
# so `~/.cargo/bin` (rustc/cargo/rust-analyzer proxies) is on PATH in login shells.
# -----------------------------------------------------------------------------
RUN if [ "${INSTALL_RUST}" = "true" ]; then \
    echo '. "$HOME/.cargo/env"' >> /etc/profile.d/rust.sh && \
    chmod +x /etc/profile.d/rust.sh; \
    fi

# -----------------------------------------------------------------------------
# Python - Installed via DevContainer feature (ghcr.io/devcontainers/features/python)
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# UV Package Manager Installation
# -----------------------------------------------------------------------------
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /tmp/uv-binaries/
RUN cp /tmp/uv-binaries/uv /usr/local/bin/uv && \
    cp /tmp/uv-binaries/uvx /usr/local/bin/uvx && \
    chmod +x /usr/local/bin/uv /usr/local/bin/uvx && \
    rm -rf /tmp/uv-binaries

# UV environment variables
ENV UV_LINK_MODE=copy
# UV_CACHE_DIR is deliberately NOT set here. A Dockerfile ENV reaches every
# process, whatever its user, and the container starts as root
# ("containerUser": "root"). One root-context `uv` run against a cache inside
# vscode's home leaves root-owned entries in the cache volume; every later `uv`
# command as vscode then fails ("Failed to initialize cache … Permission
# denied"), across rebuilds, and vscode has no sudo to repair it. It is set for
# the remote user only, in devcontainer.json "remoteEnv".

# -----------------------------------------------------------------------------
# Claude Code CLI (Conditional)
# Installed via native installer in post-create script (requires Node.js at runtime)
# ENV set conditionally to keep non-Claude images clean.
# -----------------------------------------------------------------------------
RUN if [ "${INSTALL_CLAUDE}" = "true" ]; then \
    echo 'export CLAUDE_CONFIG_DIR=/home/vscode/.claude' >> /etc/profile.d/claude.sh && \
    chmod +x /etc/profile.d/claude.sh; \
    fi

# -----------------------------------------------------------------------------
# Docs Pipeline (Conditional)
# Sphinx + sphinx-needs + PlantUML + Graphviz toolchain used by the
# trustworks-plugins methodology plugins (refac, arch, discovery, workspace) to
# render `.. uml::`, `.. needflow::`, and `.. graphviz::` directives during
# `make -C docs html`.
#
# What this installs and why:
#   - graphviz       — provides `dot`, picked up at config-load via
#                      `shutil.which("dot")`. Required for needflow and
#                      Graphviz-backed PlantUML diagrams. ~5 MB.
#   - default-jre-headless — runs `plantuml.jar`.
#   - fonts-dejavu-core — covers PlantUML's default font set; without it,
#                      diagrams can render with missing-glyph boxes.
#   - pandoc         — document converter. discovery's `/discovery:adopt` uses
#                      it to convert existing reStructuredText docs to MyST
#                      Markdown; without it adopt falls back to a weaker
#                      regex-based conversion.
#
# The docs site needs Sphinx 9.1, which needs Python 3.12 or newer — python3 is
# the Python DevContainer feature below, so keep its version at 3.12+.
#
# Python packages (sphinx, sphinx-needs, sphinxcontrib-plantuml, etc.) are NOT
# installed here — `setup-docs` from each plugin creates `docs/.venv/` and
# pip-installs them on demand, so the system image stays lean.
# -----------------------------------------------------------------------------
RUN if [ "${INSTALL_DOCS_PIPELINE}" = "true" ]; then \
    set -eux && \
    echo "Installing docs pipeline (graphviz, pandoc, JRE if needed, fonts)..." && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    graphviz \
    pandoc \
    fonts-dejavu-core \
    default-jre-headless && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*; \
    fi

# -----------------------------------------------------------------------------
# Plugin Marketplace Seed Directory
# Marketplaces are cloned on the host by fetch-marketplaces.sh (initializeCommand)
# and LIVE bind-mounted into the container at /opt/claude-seed/marketplaces at
# runtime (see devcontainer.json "mounts"). They are intentionally NOT COPYed
# into the image: a baked COPY only refreshes on rebuild, so a plain container
# start would keep serving stale plugin versions. The bind-mount means every
# START picks up whatever initializeCommand just pulled. No git credentials
# enter the image. We create the mount point so the bind target exists.
# -----------------------------------------------------------------------------
RUN mkdir -p /opt/claude-seed/marketplaces

# -----------------------------------------------------------------------------
# Directory Structure & Permissions
# -----------------------------------------------------------------------------
# Create base directories (always needed)
RUN mkdir -p \
    /home/vscode/.npm \
    /home/vscode/.uv-cache \
    /home/vscode/.cache/pip \
    /home/vscode/.local/bin \
    /workspace && \
    # Rust/Cargo directories (conditional) — pre-created so the named volumes
    # mounted at ~/.cargo/{registry,git,bin} and ~/.rustup inherit vscode
    # ownership instead of root:root (which would EACCES the rustup install).
    # ~/.rustup is persisted too so the toolchain (~200 MB) is not re-downloaded.
    if [ "${INSTALL_RUST}" = "true" ]; then \
    mkdir -p /home/vscode/.cargo/registry /home/vscode/.cargo/git /home/vscode/.cargo/bin /home/vscode/.rustup; \
    fi && \
    # JetBrains directories (conditional)
    if [ "${INSTALL_JETBRAINS}" = "true" ]; then \
    mkdir -p /home/vscode/.local/share/JetBrains/RemoteDev \
    /home/vscode/.cache/JetBrains/RemoteDev \
    /home/vscode/.cache/JetBrains \
    /home/vscode/.config/JetBrains; \
    fi && \
    # Claude Code directories (conditional)
    if [ "${INSTALL_CLAUDE}" = "true" ]; then \
    mkdir -p /home/vscode/.claude /home/vscode/.cache/claude-code; \
    fi && \
    # CodeGraph directory (conditional) — pre-created so the named volume
    # mounted at /workspace/.codegraph inherits vscode ownership instead of
    # defaulting to root:root, which would EACCES `codegraph init`.
    if [ "${INSTALL_CODEGRAPH}" = "true" ]; then \
    mkdir -p /workspace/.codegraph; \
    fi && \
    # In-repo build directories (BUILD_DIR_VOLUMES) — pre-created so each named
    # volume mounted there inherits vscode ownership. Docker copies ownership
    # from the image path even though /workspace itself is a bind mount; without
    # this the volume is root:root, and vscode has no sudo to fix it.
    for d in ${BUILD_DIR_VOLUMES}; do \
    case "$d" in /*|*..*) echo "ERROR: BUILD_DIR_VOLUMES entry '$d' must be relative to /workspace" >&2; exit 1;; esac; \
    mkdir -p "/workspace/$d"; \
    done && \
    chown -R vscode:vscode /home/vscode /workspace

# -----------------------------------------------------------------------------
# Final Configuration
# -----------------------------------------------------------------------------
# Reset to interactive mode
ENV DEBIAN_FRONTEND=dialog

# -----------------------------------------------------------------------------
# Sudo: no general root for the vscode user
# -----------------------------------------------------------------------------
# The base image gives vscode passwordless sudo for everything
# (/etc/sudoers.d/vscode). This config has no egress firewall, so vscode needs
# no sudo rules at all; the base rule is removed so nothing running as vscode
# (an AI agent included) gets root through sudo. Note that Docker-in-Docker is
# enabled: vscode is in the docker group, which is root-equivalent regardless.
# A developer who needs root runs, from the HOST:
#   docker exec -u root -it <project>-devcontainer bash
# The build fails if a blanket rule survives.
RUN rm -f /etc/sudoers.d/vscode && \
    if sudo -l -U vscode | grep -qE 'NOPASSWD: *ALL *$|\) *ALL *$'; then \
    echo "ERROR: vscode still has blanket sudo — find the rule under /etc/sudoers*" >&2; exit 1; \
    fi

# Switch to non-root user for security
USER vscode

# Configure PATH for user. Rust PATH entries are added conditionally so
# non-interactive (non-login) shells also resolve them (profile.d only loads in
# login shells; ~/.bashrc loads in interactive shells).
RUN echo 'export PATH="$HOME/.local/bin:$PATH"' >> /home/vscode/.bashrc && \
    if [ "${INSTALL_RUST}" = "true" ]; then \
    echo '. "$HOME/.cargo/env"' >> /home/vscode/.bashrc; \
    fi

# Set working directory
WORKDIR /workspace

# Labels for documentation and container identification
# PROJECT_NAME is set via devcontainer.json build.args for per-project identity
LABEL org.opencontainers.image.title="${PROJECT_NAME}-devcontainer" \
    org.opencontainers.image.description="Development container for ${PROJECT_NAME}" \
    org.opencontainers.image.version="1.0.0" \
    devcontainer.project="${PROJECT_NAME}"
