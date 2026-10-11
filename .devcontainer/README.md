# DevContainer Configuration

This project (`essentials-rs`) uses a devcontainer for reproducible development environments: Rust (stable), Node.js 24 and Python 3.14, with Docker-in-Docker for Testcontainers and a ready-to-run PostgreSQL.

## Authenticating Claude Code

Run `claude` in the container terminal — it triggers the standard OAuth flow and saves credentials to `~/.claude/` (volume-mounted, so they persist across container restarts and rebuilds). No env vars needed.

### The paste-code fallback — important to know

Claude Code tries to open a browser and redirect back to a localhost callback after you sign in. Inside a container, that callback often can't complete:

- **VS Code devcontainers:** usually works via port forwarding, but port forwarding sometimes silently fails — browser shows "localhost refused to connect".
- **Claude CoWork / headless Docker sandbox:** no port forwarding, callback always fails.

When the callback can't complete, Anthropic's sign-in page automatically falls back to showing you a **login code**. The CLI prompts `Paste code here if prompted:` — copy the code from the browser and paste it back into the terminal. Auth completes the same way.

So the flow end-to-end:

1. Run `claude` in the container.
2. It prints an OAuth URL. If the browser doesn't open automatically, press `c` to copy the URL, then paste into your host browser.
3. Sign in on Anthropic.
4. Either the browser redirects back to localhost (great, done), or it shows a login code — paste the code into the CLI prompt.
5. Credentials are saved to `~/.claude/` and persist across rebuilds.

> **If the fallback itself is flaky** — browser shows "localhost refused to connect" and copying the URL again only *sometimes* surfaces a login code — that's a port-forward timing race. The CLI binds a random local port for the callback; VS Code's auto-forward detection sometimes loses the race against the browser redirect, and whether Anthropic's sign-in page falls back to a code is state-dependent. See [Recommended: pre-authenticate on the host](#recommended-pre-authenticate-on-the-host-most-reliable) below for a permanent fix that sidesteps the callback entirely.

### Recommended: pre-authenticate on the host (most reliable)

Skip the in-container browser dance by generating the token **on the host** (where OAuth works reliably) and letting `devcontainer.json` propagate it in:

```bash
# On the HOST, not inside the container:
claude setup-token
# Copy the printed token, then:
echo 'export CLAUDE_CODE_OAUTH_TOKEN=<token>' >> ~/.zshrc    # or ~/.bashrc
# Reopen the devcontainer
```

`devcontainer.json` → `containerEnv` already includes `"CLAUDE_CODE_OAUTH_TOKEN": "${localEnv:CLAUDE_CODE_OAUTH_TOKEN}"`, so the host variable is passed through automatically. Inside the container, `claude` reads it at startup — no OAuth listener, no port forward, no race. Harmless when the host variable is unset.

Properties of the resulting token:
- **1-year lifetime** — no silent 24h expiry like subscription tokens in some sandboxes.
- **Inference-only** — it cannot start a Remote Control session; sign in with `claude auth login` for that.
- **Survives rebuilds** — it lives on the host; the container just reads it.

Not available to Claude CoWork users (no host shell access) — they should rely on the paste-code fallback above.

### Running `claude setup-token` inside the container (CoWork / no host access)

If you can't set the token on the host (Claude CoWork, remote Docker host, no local shell), run `setup-token` from inside the container instead. It still requires completing an OAuth flow in a browser **once**, but after that, the 1-year token avoids both the expiring-subscription-token issue (seen at ~24h in some sandboxes) and the need for future `/login` runs:

```bash
claude setup-token
# Copy the printed token, then:
echo 'export CLAUDE_CODE_OAUTH_TOKEN=<token>' >> ~/.bashrc
# Or ~/.zshrc if that's what you use
# Apply to the current shell:
export CLAUDE_CODE_OAUTH_TOKEN=<token>
```

To refresh, re-run `claude setup-token` and replace the line in `~/.bashrc`.

### Remote Control

Run **`claude-rc`** instead of `claude` to continue the session from claude.ai/code or the Claude app (arguments pass through: `claude-rc --resume`, `claude-rc remote-control`). The container keeps `DO_NOT_TRACK=1` for everything; `claude-rc` clears only the variables that would block Remote Control, for that one process — see [Remote Control](#remote-control-1) below. It needs a `claude auth login` sign-in; a `setup-token` token cannot start it.

### Things that don't work

- `claude login --headless` — not a real flag.

## Git Identity

Each developer supplies their own git name/email via **`.devcontainer/.env.local`** — a gitignored file that lives with the devcontainer config. Nothing identity-related is committed; each teammate keeps their own copy.

### First-time setup

```bash
# From the project root:
cp .devcontainer/.env.local.example .devcontainer/.env.local
# Then edit .devcontainer/.env.local — for example:
cat > .devcontainer/.env.local <<'EOF'
export GIT_USER_NAME="Lasse Cramon"
export GIT_USER_EMAIL="lasse.cramon@trustworks.dk"
EOF
```

`post-create.sh` sources `/workspace/.devcontainer/.env.local` on every container creation and runs `git config --global user.name/email` with the values. Because the file lives on the bind-mounted workspace, it survives container rebuilds without any host-shell configuration — and it works in Claude CoWork (no host access required).

### Changing your identity later

Edit `.devcontainer/.env.local` (on the host — `.devcontainer/` is read-only inside the container) and rebuild the container.

### If `.env.local` is missing

`post-create.sh` prints a **prominent boxed warning** with a copy-pastable `cp .env.local.example .env.local` command and leaves git unconfigured. The warning re-prints on every container start until `.env.local` exists — it's hard to miss.

### Fallback: configure in-container (not persistent)

You can run `git config --global user.name "…"` / `user.email "…"` from inside the container, but `~/.gitconfig` is **not** volume-mounted, so those values do not survive a rebuild. `.env.local` is the durable mechanism.

### Alternative: bake the identity into `containerEnv`

For a solo/private repo where committing a specific identity is intentional, set `GIT_USER_NAME` / `GIT_USER_EMAIL` to literal strings in `devcontainer.json` → `containerEnv` instead of leaving them empty. The values land in the shared `devcontainer.json` (visible to anyone with repo access) but stay out of image layers. Not recommended for shared repos.

## Configured Runtimes

| Runtime | Version | Package Manager |
|---------|---------|-----------------|
| Rust | stable (rustup) | Cargo |
| Node.js | 24 LTS | NPM |
| Python | 3.14 | UV |
| Docker (in-container) | latest Docker CE | Docker Compose v2 |

## Language Servers

The devcontainer ships with these LSPs already on `PATH`. The official `claude-plugins-official` Code-Intelligence plugins (column 3) launch them — these plugins require exactly the binaries listed here.

| LSP | Binary | Official plugin | Installed by | Gate |
|-----|--------|-----------------|--------------|------|
| TypeScript | `typescript-language-server` | `typescript-lsp` | `post-create.sh` (npm) | Always — Node.js is always installed |
| Python | `pyright` (`pyright-langserver`) | `pyright-lsp` | `post-create.sh` (uv tool) | Always — Python is always installed |
| Rust | `rust-analyzer` | `rust-analyzer-lsp` | `post-create.sh` (`rustup component add rust-analyzer rust-src`) | `INSTALL_RUST=true` |

Verify after a rebuild with `which <name> && <name> --version`.

When the **native LSP tool** is enabled (`ENABLE_LSP_TOOL=1`, the default), `post-create.sh` installs the matching `*-lsp` plugin from the official `claude-plugins-official` marketplace for every enabled runtime, giving Claude Code compiler-accurate go-to-definition, find-references, hover types, and live diagnostics. See **Native LSP tool** below.

## Rebuilding the Container

After changing `.devcontainer/` files (on the host):
- **VS Code:** `Dev Containers: Rebuild Container` from the command palette
- **JetBrains Gateway:** Disconnect and reconnect to the container
- **CLI:** `docker build -f .devcontainer/Dockerfile --build-arg INSTALL_RUST=true --build-arg INSTALL_CLAUDE=true --build-arg PROJECT_NAME=essentials-rs -t essentials-rs-devcontainer .devcontainer`

## Configuration Files

| File | Purpose |
|------|---------|
| `.devcontainer/devcontainer.json` | Single source of truth: build args, volumes, extensions, resource limits |
| `.devcontainer/Dockerfile` | Container image build with conditional runtime installs |
| `.devcontainer/scripts/post-create.sh` | Runtime setup that runs after container creation |
| `.devcontainer/scripts/fetch-marketplaces.sh` | Plugin marketplace fetcher — runs on host via initializeCommand |
| `.devcontainer/scripts/statusline.sh` | Claude Code footer: model · context usage · branch · worktree. Registered in **user** settings by `post-create.sh`. |
| `.devcontainer/scripts/claude-rc.sh` | `claude-rc` — starts Claude Code with Remote Control while the telemetry opt-out stays in force. Put on `PATH` by `post-create.sh`. |
| `.devcontainer/.env.local.example` | Per-developer git-identity template (committed) — copy to `.env.local` |
| `.devcontainer/.env.local` | Your personal git name/email (gitignored, sourced by post-create.sh) |
| `.claude/settings.json` | Claude Code config: worktree base, marketplaces |
| `.mcp.json` | MCP server registrations — `codegraph`, written by `post-create.sh` if absent |
| `docker-compose.postgres.yml` | PostgreSQL service for Docker-in-Docker |

All configuration changes are made in `devcontainer.json` via `build.args`. The Dockerfile is the build recipe — you normally don't need to edit it.

### Installation Toggles

| Build arg | Value | Effect |
|-----------|-------|--------|
| `INSTALL_CLAUDE` | `true` | Claude Code CLI (native installer in `post-create.sh`) |
| `INSTALL_RUST` | `true` | rustup + cargo, rustfmt, clippy, rust-analyzer |
| `INSTALL_RUST_WASM` | `false` | wasm32 target + trunk + cargo-leptos + binaryen |
| `RUST_TOOLCHAIN` | `stable` | Channel passed to rustup |
| `INSTALL_CODEGRAPH` | `true` | CodeGraph knowledge-graph indexer (MCP) |
| `INSTALL_GITNEXUS` | `false` | GitNexus indexer (mutually exclusive with CodeGraph) |
| `INSTALL_JETBRAINS` | `true` | JetBrains Gateway directories |
| `INSTALL_DOCS_PIPELINE` | `false` | graphviz + pandoc + JRE + fonts for the methodology plugins' docs sites |
| `ALLOW_NPM_SCRIPTS` | `true` | npm lifecycle scripts allowed |
| `BUILD_DIR_VOLUMES` | `""` | In-repo build dirs on container volumes (off) |
| `ENABLE_DCT` | `false` | Docker Content Trust |

## Adding or Removing Runtimes

### To add a runtime later:
1. Edit `devcontainer.json` → set the relevant `build.args` toggle to `"true"`
2. For Node.js/Python: add or update the entry in the `features` section
3. Rebuild the container

Java, .NET, Bun and Playwright were left out of this config, and their Dockerfile sections were removed with them. To add one (for example Java for the Java ↔ Rust interop tests), re-run `/devcontainer-generator:generate-dev-container` on the host and choose "Custom refinement".

### To remove a runtime:
1. Set its `build.args` toggle to `"false"`
2. Remove its DevContainer feature entry (if applicable)
3. Remove related VS Code extensions from `customizations.vscode.extensions`
4. Remove related cache volume mounts from `mounts`
5. Rebuild the container

## Environment Persistence

This project has a persistent environment configured via `CLAUDE_ENV_FILE` (`/etc/sandbox-persistent.sh`).

- Environment variables stored in `/etc/sandbox-persistent.sh` persist across all bash invocations
- Use `echo "export VAR_NAME=value" >> /etc/sandbox-persistent.sh` to add persistent variables
- Use `bash -l -c "your-command"` to ensure the persistent environment is loaded

**CRITICAL: Never add shell completion scripts to the persistent environment file.** They will break the bash tool. Only add core initialization scripts (e.g., `nvm.sh`, `sdkman-init.sh`).

## Claude Code Permission Mode

This devcontainer is configured with **auto** as the default permission mode. Because Claude Code ignores `auto` in committed project settings (since v2.1.142), it is applied by `post-create.sh` to your **user** settings (`~/.claude/settings.json`), gated on `CLAUDE_DEFAULT_MODE_AUTO=true` in `devcontainer.json` → `containerEnv`.

**Auto mode** uses a background safety classifier to review each action. It blocks prompt injection, unexpected deploys, and data exfiltration while letting routine dev work flow. There is no egress firewall in this container, so the classifier is the main guard on what Claude does.

- Best-effort: needs an eligible account (Claude Code v2.1.83+); an ineligible session silently starts in `default` mode
- Configure trusted infrastructure via `autoMode.environment` in `.claude/settings.local.json` (NOT `.claude/settings.json` — the classifier ignores `autoMode` from shared project settings)

To change the mode: set `CLAUDE_DEFAULT_MODE_AUTO` to `"false"` and edit `~/.claude/settings.json` → `permissions.defaultMode`, or pass `--permission-mode <mode>` at startup.

## Status Line

Claude Code's footer shows neither the active model nor how full the context window is, and there is no built-in setting for either — `statusLine` is the only supported route. `.devcontainer/scripts/statusline.sh` fills it in:

```
Opus 5:high │ 162k/1.0M (16%) │ ⎇ main │ /workspace
Sonnet 5 ⚡fast │ 187k/200k (93%) │ ⎇ main │ /workspace
Haiku 4.5 │ 45k/200k (22%) │ ⎇ fix/gate-external ⧉ agent-3 │ …/wt/agent-3
```

Context usage is colour-coded (green, amber from 60 %, red from 85 %); a detached HEAD shows its short SHA in amber. The branch comes from `.git/HEAD` directly — which also resolves plain `git worktree` checkouts — so a render costs ~5 ms and never forks `git`.

`post-create.sh` registers it in your **user** settings (`~/.claude/settings.json`) on container start, gated on `CLAUDE_STATUSLINE` in `devcontainer.json` → `containerEnv`. User scope is deliberate: project settings *win over* user settings, so a committed `statusLine` would silently replace whatever footer every contributor already had. An existing `statusLine` of yours is never overwritten.

To turn it off: set `CLAUDE_STATUSLINE` to `"false"` and delete the `statusLine` key from `~/.claude/settings.json`.

If you extend it, note that a dirty-state indicator needs `git status`, which scans the whole index on every render — cache it or set a `refreshInterval`, or the footer will lag.

## Remote Control

Remote Control continues a session running in this container from [claude.ai/code](https://claude.ai/code) or the Claude app. Start it with **`claude-rc`** instead of `claude` — every argument passes through (`claude-rc --resume`, `claude-rc --continue`, `claude-rc remote-control` for server mode).

Why a separate command: `devcontainer.json` sets `DO_NOT_TRACK=1` for every process as a cross-tool telemetry opt-out, and some of the variables in that family switch off the feature-flag fetch Remote Control depends on. `claude-rc` (`scripts/claude-rc.sh`, put on `PATH` by `post-create.sh`) handles that for one `claude` process and leaves the container-wide setting alone:

| Variable | What `claude-rc` does |
|---|---|
| `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC`, `DISABLE_GROWTHBOOK` | Always cleared — they always block Remote Control |
| `DO_NOT_TRACK`, `DISABLE_TELEMETRY` | **Kept** on Claude Code 2.1.283 and later, which no longer lets them block Remote Control. Cleared on older versions, or with `CLAUDE_RC_CLEAR_TELEMETRY_OPTOUT=1` if your organization requires Trusted Devices — and then re-exported into every Bash command Claude runs (through `CLAUDE_ENV_FILE`), so the tools it starts keep the opt-out |

`DISABLE_ERROR_REPORTING` and `CLAUDE_CODE_DISABLE_FEEDBACK_SURVEY` stay set in every case; neither affects Remote Control.

Remote Control also needs a full claude.ai sign-in: a `claude setup-token` token (`CLAUDE_CODE_OAUTH_TOKEN`) cannot start it, and neither can an `ANTHROPIC_BASE_URL` pointing at a gateway. `claude-rc` warns about both.

## Docker-in-Docker

Docker is available inside the container. You can run containers, use Docker Compose, and use tools like Testcontainers (`testcontainers-rs` for the durable-queues integration tests).

- Docker commands work directly: `docker ps`, `docker compose up`
- The container runs in privileged mode (`--privileged` in runArgs) and starts as root (`"containerUser": "root"`, present in every config) so the Docker daemon can start; your terminal and the IDE still run as `vscode`
- **Security cost:** `vscode` is in the `docker` group, which is root-equivalent. Anything running in the container — an AI agent included — can become root through Docker, regardless of the sudo restriction.
- To remove: delete the `docker-in-docker` feature and replace `--privileged` with `"--security-opt", "apparmor=docker-default"` in runArgs

## PostgreSQL

PostgreSQL runs as a standalone container via Docker-in-Docker. Run these from `/workspace`:

```bash
# Start PostgreSQL
docker compose -f docker-compose.postgres.yml up -d

# Stop (preserves data)
docker compose -f docker-compose.postgres.yml down

# Stop and wipe data
docker compose -f docker-compose.postgres.yml down -v
```

Connection: `postgresql://postgres:postgres@localhost:5432/app_db`

Handy for manual runs and for pointing the Java and Rust durable-queue implementations at one shared schema. Testcontainers-based tests start their own database and do not need it.

## Rust

The Rust toolchain is managed by **rustup** (channel: `stable`) and installed by `post-create.sh` as the `vscode` user. `cargo`, `rustfmt`, and `clippy` come from rustup's default profile; `rust-analyzer` and `rust-src` are added as components so the LSP is on `PATH` for the VS Code extension and the Claude Code LSP plugin.

The repo root is a Cargo workspace (like essentials-project's parent `pom.xml`); each component is a member crate listed in the root `Cargo.toml`. Run cargo from the root:

```bash
cargo build              # Build all members
cargo test               # Run tests (add -p durable-queues-rs for one crate)
cargo clippy             # Lint (rust-analyzer also runs this on save)
cargo fmt                # Format
```

**Caching.** `~/.cargo/registry`, `~/.cargo/git`, `~/.cargo/bin`, and `~/.rustup` are named volumes, so the crate cache, compiled tool binaries, and the toolchain itself persist across rebuilds — the ~200 MB toolchain isn't re-downloaded and source-built tools aren't recompiled.

**Build speed on macOS.** `target/` lives in the bind-mounted checkout, which is slow on macOS. If builds drag, set `BUILD_DIR_VOLUMES` to `"target"` (the workspace shares one `target/` at the root) and add the matching mount line (`"source=devcontainer-build-target-${devcontainerId},target=/workspace/target,type=volume"`), then rebuild. The host then sees `target/` empty.

## CodeGraph Knowledge Graph (experimental)

CodeGraph indexes the codebase into a Tree-sitter + SQLite knowledge graph at `/workspace/.codegraph/`, exposing 9 read-only MCP tools to Claude. CodeGraph is a plain MCP server (not a Claude Code plugin) — it is registered via project-scoped `.mcp.json`, generated by `post-create.sh` if absent.

```bash
# Initial setup (already done by post-create.sh on first build)
npm install -g @colbymchenry/codegraph
codegraph init --yes  # init + initial index; --yes skips every prompt

# Re-index after large refactors
codegraph index

# Check index freshness
codegraph status

# Available MCP tools (called by Claude automatically):
#   codegraph_files       — list project files known to the graph
#   codegraph_search      — search for symbols
#   codegraph_explore     — primary tool: returns full source from relevant files
#   codegraph_context     — build markdown context for a task
#   codegraph_node        — read a specific graph node
#   codegraph_callers     — find callers of a function
#   codegraph_callees     — find what a function calls
#   codegraph_impact      — assess blast radius of changes
#   codegraph_status      — index freshness / sync state
```

**Auto-sync.** CodeGraph watches the project via native OS file events with a 2-second debounce, so the graph stays fresh as you code — no manual re-index needed for normal editing.

**Telemetry is disabled** out of the box, three ways: the `CODEGRAPH_TELEMETRY=0` and `DO_NOT_TRACK=1` `containerEnv` vars in `devcontainer.json`, and a persisted `codegraph telemetry off` run by `post-create.sh`. Re-enable with `codegraph telemetry on` and by clearing those env vars if you ever want to opt back in.

**Two other outbound touches, both pinned off or unreachable.** From CodeGraph 1.5.0 the long-lived `codegraph serve --mcp` process background-checks GitHub for a newer release; `CODEGRAPH_NO_UPDATE_CHECK=1` disables it (as does `DO_NOT_TRACK=1`). And `codegraph install` / `codegraph upgrade` offer a one-time Pro beta-waitlist prompt that POSTs an email to `getcodegraph.com/api/waitlist` — strictly opt-in, and `post-create.sh` never runs either command, but don't answer it if you run them by hand.

**When to fall back to grep.** CodeGraph does not currently support SQL, Terraform/HCL, R, Scala, dbt — use `Grep` for those (the durable-queues SQL included). If `codegraph_status` reports out-of-sync (rare; usually after stopping the container during a `git pull`), Claude is instructed to fall back to `Grep` and warn you to rerun `codegraph index`.

**Mutual exclusivity.** At most one of `INSTALL_GITNEXUS` and `INSTALL_CODEGRAPH` may be `"true"` in `devcontainer.json`. To switch indexers, set the active one to `"false"`, set the other to `"true"`, and rebuild.

**Uninstall.** `codegraph uninit /workspace` removes `.codegraph/`. To stop loading the MCP server, remove the `codegraph` block from `.mcp.json`.

## Native LSP tool (semantic code intelligence)

Claude Code's built-in LSP tool is enabled (`ENABLE_LSP_TOOL=1` in `containerEnv`; auto-on for Claude Code ≥ 2.1.50). `post-create.sh` registers the matching Code-Intelligence plugin from the **official** `claude-plugins-official` marketplace for each enabled runtime (`typescript-lsp`, `pyright-lsp`, `rust-analyzer-lsp`). These plugins drive the language-server binaries already on `PATH` (see Language Servers above).

**This is the only layer that provides compiler-accurate semantics** — CodeGraph is tree-sitter (syntactic) and does **not** offer go-to-definition, find-references, type resolution, or diagnostics. The native LSP tool does.

Claude is instructed (in `CLAUDE.md`) to **prefer the LSP tool over grep for symbol navigation, use grep only for literal text, and trust the language server's results** rather than re-reading files to verify them.

**Parallel agents.** The language server is shared, so with `isolation: "worktree"` subagents it also sees the files they open under `.claude/worktrees/<agent>/` and reports them as non-project files with transient errors. The plugins have no path exclusion; `CLAUDE.md` tells Claude to disable the affected language's plugin in `.claude/settings.local.json` during a heavy fan-out. `devcontainer.json` excludes the worktree directory from the IDE's watcher and search.

- Only the official, Anthropic-curated marketplace is used (no community LSP marketplace). To add a language the official set doesn't cover, vet and add its plugin explicitly, or author a project-local `.lsp.json`.
- Disable per language with `claude plugin uninstall <name>@claude-plugins-official`; disable entirely by setting `ENABLE_LSP_TOOL=0` (or removing it) in `devcontainer.json` → `containerEnv`.

## Plugin Marketplace

Claude Code plugins (Trustworks plugin marketplace) are delivered through two complementary mechanisms:

1. **Seed directory** (host clone → live read-only bind-mount) — plugins cloned on the host and bind-mounted into the container, immediately available with no credentials inside the container
2. **`extraKnownMarketplaces`** (runtime) — marketplace declared in `.claude/settings.json` with `"source": "directory"` pointing to the local copy, so it's visible in `claude plugin list`

Every container start fetches the latest plugin versions automatically.

### How it works

1. `initializeCommand` runs `.devcontainer/scripts/fetch-marketplaces.sh` on your **host machine** before every container start
2. The script clones/updates the marketplace repo into `.devcontainer/.plugin-marketplace/` using **your host's** git credentials
3. `devcontainer.json` **live bind-mounts** that directory **read-only** into the container at `/opt/claude-seed/marketplaces/` (it is *not* copied into the image — so the latest plugins are served on every start, and no credentials are ever baked in)
4. `CLAUDE_CODE_PLUGIN_SEED_DIR` tells Claude Code where to find the pre-installed plugins
5. The script then syncs `extraKnownMarketplaces` in `.claude/settings.json` with the `MARKETPLACES` array (via `jq`), so every seeded marketplace is automatically visible in `claude plugin list`

**Git credentials never enter the container** — image *or* runtime filesystem. The host performs the authenticated clone; the container only ever sees the resulting read-only files and has no GitHub access. This is deliberate: it removes a common token-theft / supply-chain attack vector.

The fetch is **non-blocking**: if it can't authenticate or reach GitHub, the container still starts (just without pre-seeded plugins) and the script prints the guidance below. It also **never pops a surprise credential dialog** — so if it can't authenticate it fails fast and tells you what to do, rather than blocking the build behind an unexplained prompt.

### First-time setup — authenticate your HOST to GitHub (one-time)

The marketplace repo is **private**, so your host machine must be authenticated to GitHub *before* you reopen in the container. Do this once on your host:

```bash
# Recommended — GitHub CLI (handles the org's SAML SSO automatically).
#
# GitHub's OFFICIAL sources for the CLI are https://docs.github.com/en/github-cli
# and https://cli.github.com (both GitHub-owned, both pointing to github.com/cli/cli).
#
# Install via GitHub's recommended package managers — the binary is GitHub-built;
# the package manifests themselves are community-maintained:
#   macOS:   brew install gh
#   Windows: winget install --id GitHub.cli      (or: choco install gh / scoop install gh)
#
# Strictly first-party (GitHub-built & signed) — Windows .msi / signed binaries:
#   https://github.com/cli/cli/releases
#   Optionally verify provenance: gh attestation verify <file> --owner cli
gh auth login
gh auth setup-git      # makes git use the gh credential
```

> **Provenance note (for stricter / regulated setups):** brew and winget are the methods GitHub *recommends*, and they deliver GitHub's own signed binary — but the Homebrew formula and winget manifest are maintained in community repos, not by GitHub. The only fully first-party artifacts are the signed binaries (and Windows `.msi`) on the [releases page](https://github.com/cli/cli/releases), which you can verify against the `cli/cli` repo with `gh attestation verify <file> --owner cli`. The repo's authenticity is established by GitHub's own docs ([docs.github.com/en/github-cli](https://docs.github.com/en/github-cli)) and homepage ([cli.github.com](https://cli.github.com)) linking to it — not by the org's "verified" badge, which only attests domain identity.

Then **Rebuild Container** / **Reopen in Container**.

**Using a Personal Access Token (PAT) instead?**
1. Create a token at <https://github.com/settings/tokens> — classic: `repo` scope; fine-grained: read access to the marketplace repo.
2. **Authorize it for SSO.** The `trustworksdk` org enforces SAML SSO: on the token page click **"Configure SSO" → Authorize** for `trustworksdk`. Without this the token returns **HTTP 403** even though it looks valid — this is the #1 cause of "my PAT doesn't work".
3. When git prompts, paste the **token as the password** (any username).

**Entered a credential and it *keeps* failing?** Your OS credential manager cached the bad one and silently reuses it. Clear it and retry:

```bash
gh auth login                                            # overwrites the stored credential, or:
printf 'protocol=https\nhost=github.com\n\n' | git credential reject
# Windows: Credential Manager → Windows Credentials → remove the github.com entry.
```

> You do **not** need to authenticate inside the container — only on the host. If you're a member of the Trustworks Claude organization, plugins also arrive via org settings, so this seed step is optional for you.

### Adding or removing marketplaces

The `MARKETPLACES` array in `.devcontainer/scripts/fetch-marketplaces.sh` is the **single source of truth**. Edit it and rebuild — `.claude/settings.json` is kept in sync automatically.

```bash
MARKETPLACES=(
  "trustworks-plugins|https://github.com/trustworksdk/plugin-marketplace.git"
  "my-marketplace|https://github.com/myorg/my-plugins.git"
)
```

Format: `"name|repo-url"`. On the next `Rebuild Container`, matching entries are added to `extraKnownMarketplaces`; entries removed from the array are cleaned out of settings.json. Non-seed entries (e.g. inline `"source": "url"` marketplaces you added by hand) are preserved. An empty array is valid — the devcontainer still builds.

### Host dependencies

The fetch script needs `bash`, `git`, and `jq` on the host. On Windows, Git for Windows provides bash and git; install `jq` via `winget install jqlang.jq` (or `choco install jq` / `scoop install jq`). On macOS use `brew install jq`; on Linux use your package manager. The script fails fast with install hints if `jq` is missing.

## Windows hosts

Two host-side details fail *before* Docker is contacted, so they look like Docker problems when they are not.

**1. `C:\Program Files\Git\bin` must be on PATH.** `initializeCommand` runs on the host as `["sh", "-c", …]` — `cmd.exe` cannot parse its POSIX line — and the Git for Windows installer only adds `Git\cmd` (which holds `git.exe` alone). Add `Git\bin` once, then restart VS Code:

```powershell
[Environment]::SetEnvironmentVariable(
  'Path',
  [Environment]::GetEnvironmentVariable('Path','User').TrimEnd(';') + ';C:\Program Files\Git\bin',
  'User')
```

It is `sh`, not `bash`, on purpose: `C:\Windows\System32\bash.exe` is the WSL launcher and would run the script in the `docker-desktop` distro, which has no git and none of your credentials.

**2. Shell scripts must be checked out with LF.** `.gitattributes` pins `*.sh`, `*.bash`, `Makefile` and `*.mk` to LF, but a clone made before those rules existed keeps CRLF (`syntax error near unexpected token $'do\r'` inside the container). Re-check out just those files — this discards uncommitted edits to them, so commit or stash first:

```bash
git ls-files -z '*.sh' '*.bash' '*Makefile' '*.mk' | xargs -0 rm -f
git ls-files -z '*.sh' '*.bash' '*Makefile' '*.mk' | xargs -0 git checkout --
```

## Troubleshooting

### "SETUP INCOMPLETE" at the end of the creation log

`post-create.sh` keeps going when one install fails, then lists at the end every tool the configuration asked for that is not on `PATH`, plus any volume not owned by `vscode`. The container still starts; the `WARNING` lines earlier in the log give each cause. A root-owned path cannot be fixed from inside (`vscode` has no sudo) — run the printed command on the host:

```bash
docker exec -u root essentials-rs-devcontainer chown -R vscode:vscode <path>
docker exec -u vscode -w /workspace essentials-rs-devcontainer bash .devcontainer/scripts/post-create.sh
```

Or stop the container, `docker volume rm` the volume in question, and rebuild.

### Container fails to build
1. Ensure Docker has enough resources (recommend 8 GB RAM minimum)
2. Try `Dev Containers: Rebuild Container Without Cache`
3. Check build logs for specific errors

### Permission errors
`vscode` has no general sudo, so fix ownership from the **host**:
```bash
docker exec -u root essentials-rs-devcontainer chown -R vscode:vscode /home/vscode/.npm /home/vscode/.cargo /home/vscode/.cache
```

### Root access
`vscode` has no sudo at all. When you need root (for example to try an `apt-get install`), open a root shell from the host: `docker exec -u root -it essentials-rs-devcontainer bash`. Anything you want to keep belongs in the Dockerfile.

### Editing `.devcontainer/`
`.devcontainer/` is read-only inside the container — edit it on the host. It holds what runs on the host (`scripts/fetch-marketplaces.sh` from `initializeCommand` at every start, `devcontainer.json`) and the plugin-marketplace clone, so a writable copy would let anything in the container get code running on your machine. A `git checkout` that changes these files fails inside the container; switch such branches on the host. Not covered: plugin copies in `~/.claude/plugins/cache`, and git hooks or other tools you run on the host from this repository.

### Container name conflict
If Docker refuses to create the container with "name already in use":
```bash
docker rm essentials-rs-devcontainer  # Remove the old container
```
Then retry "Reopen in Container".

### Database connection refused
Ensure PostgreSQL is running inside the devcontainer:
```bash
docker compose -f docker-compose.postgres.yml up -d
```
