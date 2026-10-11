# DevContainer Configuration

## Claude Code Authentication

Run `claude` inside the container — standard OAuth, credentials saved to `~/.claude/` (volume-mounted, persists across rebuilds). When the browser can't redirect to the localhost callback (broken port forwarding, or CoWork/headless sandbox), the Anthropic sign-in page shows a login code instead — paste it into the CLI's `Paste code here if prompted:` prompt.

If subscription tokens keep expiring (~24h in some sandboxes) or you need non-interactive auth, use `claude setup-token` to get a 1-year token and add `export CLAUDE_CODE_OAUTH_TOKEN=<token>` to `~/.bashrc` (or `~/.zshrc`).

For Remote Control (continue a session from claude.ai/code or the app), start with `claude-rc` instead of `claude`; it needs a `claude auth login` sign-in, not a setup-token.

See "Authenticating Claude Code" in `.devcontainer/README.md` for full details.

## Git Identity

Per-developer git name/email lives in `.devcontainer/.env.local` (gitignored). Each developer copies `.devcontainer/.env.local.example` to `.env.local` and edits it; `post-create.sh` sources the file on container creation and runs `git config --global`. The file lives on the bind-mounted workspace, so it survives rebuilds — no host-shell config needed.

If `.env.local` is missing, `post-create.sh` prints a prominent boxed warning on every container start with a copy-pastable `cp .env.local.example .env.local` command. If the user's git identity is unset inside the container, point them at `.devcontainer/.env.local.example` first; in-container `git config --global` is a fallback that does **not** survive rebuilds (`~/.gitconfig` is not volume-mounted). See "Git Identity" in `.devcontainer/README.md` for details.

## Repository Layout

The root is a Cargo workspace, the Rust counterpart of essentials-project's parent `pom.xml`: each component is a member crate in its own folder (currently `durable-queues-rs/`), listed under `[workspace] members` in the root `Cargo.toml`. Shared package metadata (`edition`, `license`, `repository`) lives in `[workspace.package]`. Run cargo from the root; one `target/` and one `Cargo.lock` serve all members. A new component is a new folder plus a `members` entry.

## Configured Runtimes

| Runtime | Version | Package Manager |
|---------|---------|-----------------|
| Rust | stable (rustup) | Cargo |
| Node.js | 24 LTS | NPM |
| Python | 3.14 | UV |
| Docker (in-container) | latest Docker CE | Docker Compose v2 |

## Environment Persistence

This project has a persistent environment configured via `CLAUDE_ENV_FILE` (`/etc/sandbox-persistent.sh`).

- Environment variables stored in `/etc/sandbox-persistent.sh` persist across all bash invocations
- Use `echo "export VAR_NAME=value" >> /etc/sandbox-persistent.sh` to add persistent variables
- Use `bash -l -c "your-command"` to ensure the persistent environment is loaded

**CRITICAL: Never add shell completion scripts to the persistent environment file.** They will break the bash tool. Only add core initialization scripts (e.g., `nvm.sh`, `sdkman-init.sh`).

## Privileges

`vscode` has no general `sudo` — do not try `sudo apt-get install` or other root commands. User-level installs (`npm`, `uv tool`, `cargo install`) work. When a system package is genuinely needed, say so and propose the Dockerfile change; the developer rebuilds.

`.devcontainer/` is read-only inside the container, on purpose: it holds files the host runs. Do not try to write it, and never look for another path to the same files. When a change there is needed, show the exact edit and ask the developer to make it on the host. If a `git checkout` fails on `.devcontainer/` files, the branch switch belongs on the host.

## Docker and PostgreSQL

Docker-in-Docker is enabled, so `docker`, `docker compose` and Testcontainers work inside the container. A shared PostgreSQL is one command away: `docker compose -f docker-compose.postgres.yml up -d` (reachable at `localhost:5432`, `postgresql://postgres:postgres@localhost:5432/app_db`). Testcontainers-based integration tests start their own database and do not need it.

## Claude Code Permission Mode

This devcontainer is configured with **auto** as the default permission mode. It is applied by `post-create.sh` to the **user** settings file (`~/.claude/settings.json`) because `CLAUDE_DEFAULT_MODE_AUTO=true` in `containerEnv` — not to the committed project `.claude/settings.json`, where Claude Code ignores `auto` (since v2.1.142).

**Auto mode** uses a background safety classifier to review each action.

- Best-effort: requires an eligible account (Claude Code v2.1.83+); an ineligible session silently starts in `default` mode. On Bedrock/Vertex/Foundry it also needs `CLAUDE_CODE_ENABLE_AUTO_MODE=1` in `containerEnv`.
- Configure trusted infrastructure via `autoMode.environment` in `.claude/settings.local.json` (NOT `.claude/settings.json` — the classifier ignores `autoMode` from shared project settings)

To change: set `CLAUDE_DEFAULT_MODE_AUTO` to `"false"` in `devcontainer.json` (on the host) and edit `~/.claude/settings.json` → `permissions.defaultMode`, or pass `--permission-mode <mode>` at startup.

## Knowledge Graph: CodeGraph (experimental)

The codebase is indexed by CodeGraph into `/workspace/.codegraph/` (volume-mounted, persists across rebuilds). Registered as an MCP server in `.mcp.json` (`codegraph serve --mcp`, stdio transport). Auto-syncs on file events with a 2s debounce, so the index stays fresh as you code.

**Prefer the `codegraph_*` MCP tools over `Grep`/`Glob`/`Read` for code exploration**:
- `codegraph_files` / `codegraph_search` — find files, symbols
- `codegraph_explore` / `codegraph_context` / `codegraph_node` — read structural facts
- `codegraph_callers` / `codegraph_callees` / `codegraph_impact` — call chains, blast radius

Before relying on `codegraph_impact` / `codegraph_callers` for downstream decisions, call `codegraph_status`. If the index reports out-of-sync (e.g., after a `git pull` while the container was stopped), fall back to `Grep` and rerun `codegraph index` to rebuild.

CodeGraph does not currently support SQL, Terraform/HCL, R, Scala, dbt — fall back to `Grep` for those.

Re-index manually: `cd /workspace && codegraph index`. To uninstall: `codegraph uninit /workspace` (deletes `.codegraph/`).

Telemetry is disabled by default (via `CODEGRAPH_TELEMETRY=0` + `DO_NOT_TRACK=1` in `containerEnv` and a persisted `codegraph telemetry off`) — leave it off. The background release check is off too (`CODEGRAPH_NO_UPDATE_CHECK=1`). Do not run `codegraph install` / `codegraph upgrade` interactively on the user's behalf: they end with a Pro beta-waitlist prompt that posts an email address to getcodegraph.com.

## Code Intelligence: Native LSP tool

Claude Code's built-in LSP tool is enabled (`ENABLE_LSP_TOOL=1`), with per-language Code-Intelligence plugins (`rust-analyzer-lsp`, `typescript-lsp`, `pyright-lsp`) registered from the official `claude-plugins-official` marketplace, backed by the language-server binaries already installed in this container. This is the **only** layer here that provides compiler-accurate semantics — CodeGraph is tree-sitter (syntactic) and does not.

**Prefer the LSP tool over `Grep` for symbol navigation** — go-to-definition, find-all-references, hover types, list symbols, call hierarchy, and live diagnostics after edits. Use `Grep` only for literal text search. **Trust the language server's results** — do not re-read files to second-guess a definition or reference set the LSP returned; that wastes tokens and the LSP does not miss call sites the way grep can. After edits, act on the diagnostics the LSP reports (fix type errors / missing imports in the same turn).

**Diagnostics from agent worktrees are noise.** When subagents run with `isolation: "worktree"`, the shared language server also sees the files they open under `.claude/worktrees/<agent>/` and reports them as "non-project" files with transient errors — that is another agent's half-written tree, not yours. The Code-Intelligence plugins expose no path exclusion, so for a heavy parallel phase in one language disable that plugin in `.claude/settings.local.json` (e.g. `"enabledPlugins": {"rust-analyzer-lsp@claude-plugins-official": false}` — local settings override project settings) and re-enable it afterwards. The IDE-side excludes in `devcontainer.json` only quiet VS Code.

## Working with Subagents and Worktrees

- **Worktree base.** `.claude/settings.json` sets `worktree.baseRef: "head"`, so `isolation: "worktree"` agents, `EnterWorktree` and `--worktree` branch from the current local checkout — not from `origin/<default-branch>`, which lacks every unpushed commit. Keep a one-line guard in fan-out prompts anyway: "FIRST `git log -1 --oneline`; if the files you were asked to extend are missing, `git reset --hard <sha>`." An individual opts out with `"worktree": { "baseRef": "fresh" }` in the gitignored `.claude/settings.local.json`.
- **Coordination budget.** At most one coordination message per peer and one reply. No sign-off, thanks, or "done on my side" rounds — each costs tens of thousands of tokens across a fan-out and re-fires the parent's completion notification. Put merge guidance where the next agent will look (the issue tracker, a hand-off file), not in messages.
- **Reports.** Final report ≤ 30 lines. After reporting, stop — do not answer further peer messages.
- **Recurring loops.** A `/loop` prompt is one line that points at repo state ("continue X: check running agents and merges, then the open issues under Y"), never the original task text — that re-sends it on every fire.
- **Builds.** The exit code is the verdict: run the build as its own statement and print `EXIT=$?` on the same line. Never pipe it, never grep a quiet log for SUCCESS, never sum persisted test reports.

## Configuration Files

| File | Purpose |
|------|---------|
| `.devcontainer/devcontainer.json` | Single source of truth: build args, volumes, extensions, resource limits |
| `.devcontainer/Dockerfile` | Container image build with conditional runtime installs |
| `.devcontainer/scripts/post-create.sh` | Runtime setup that runs after container creation |
| `.devcontainer/scripts/statusline.sh` | Claude Code footer (model · context usage · branch · worktree). Registered in **user** settings by `post-create.sh` — never in project settings, which would override every contributor's own footer. |
| `.devcontainer/scripts/claude-rc.sh` | `claude-rc` launcher: Claude Code with Remote Control, telemetry opt-out kept. Do not remove `DO_NOT_TRACK` from `containerEnv` to make Remote Control work — use `claude-rc`. |
| `.devcontainer/.env.local.example` | Per-developer git-identity template (committed) — copy to `.env.local` |
| `.devcontainer/.env.local` | Per-developer git name/email (gitignored, sourced by post-create.sh) |
| `.mcp.json` | Project-scoped MCP server registrations. Generated by `post-create.sh` if absent; registers `codegraph` as `codegraph serve --mcp`. |
| `docker-compose.postgres.yml` | PostgreSQL service for Docker-in-Docker (`localhost:5432`) |

All configuration changes are made in `devcontainer.json` via `build.args`. The Dockerfile is the build recipe — you normally don't need to edit it.

See `.devcontainer/README.md` for detailed usage documentation, troubleshooting, and how to add/remove runtimes.
