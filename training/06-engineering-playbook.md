# Engineering Playbook

For engineers (Claude sessions + humans). Covers the repo landscape, environment setup, common footguns, and deploy discipline.

## Repo map

| Repo | Path | What it is | Deploy target |
|---|---|---|---|
| `pgam-direct` | `~/Desktop/pgam-direct` | SSP product + admin.pgammedia.com Next.js app | Vercel |
| `pgam-intelligence` | `~/Desktop/pgam-intelligence` | LL/TB optimization agent, scheduler, cross-cutting ops | Local + cron on Priyesh's machine |
| `pgam-dsp-dashboard` | `~/Desktop/pgam-dsp-dashboard` | DSP UI + backend | Vercel |
| `destination-com` | `~/Desktop/destination-com` | Travel platform, Next.js 16 | Vercel |
| `destination-app` | `~/Desktop/destination-app` | Native iOS+Android, Expo + Clerk | Expo EAS |
| `boxingnews` | `~/Desktop/boxingnews` | Content site + Sanity CMS + MSN pipeline | Vercel |
| `healthnation-web` | `~/Desktop/healthnation-web` | AI-only content site | Vercel |
| `visage` | `~/Desktop/visage` | Celebrity recognition product | (verify) |
| `pgam-recon` | `~/Desktop/finance_CC/pgam-recon` | Finance / SSP reconciliation | Local |
| `pgam-wealth-agent` | `~/Desktop/pgam-wealth-agent` | Personal wealth agent (not company) | Local |

## Neon layout

**One Neon project, two stacks:** `round-frog-99233431` on Launch tier.
- DSP → `public` schema
- SSP → `pgam_direct` schema
- Never cross-query without knowing which schema you're in.

**Static env vars:** DSP `NEON_*` env vars are disconnected from the Neon integration (2026-04-23) to unblock preview builds. Rotate them manually.

**HealthNation** has its own dedicated Neon project: `ep-still-pine-aqbb3g84`.

## Environment quirks

- **Node** v24
- **Python** 3.12 and 3.14 side-by-side
- **npm cache** permissions broken → use `/tmp` cache path
- **Homebrew** installed with Postgres + Redis
- **gh CLI** installed 2026-04-18 (may need auth on fresh machines).
  **Not present in cloud Claude sessions** — use the GitHub MCP tools there.

## Worktrees on pgam-dsp-dashboard (READ THIS)

Since 2026-07-10, `.env*` files are gitignored on `pgam-dsp-dashboard`. Every new worktree requires:

1. Symlink `node_modules` from the main tree
2. Symlink `.env.local` from the main tree

**If you skip step 2, `next build` in the pre-push hook fails.**

Also — **NEVER `git add -A` in a worktree here.** Inspect `git status --short` first. Untracked `.env*` in DSP worktrees leaked prod secrets to main on 2026-07-02. Always add files by name.

## Pre-push hook gotchas (DSP)

- Hook runs `next build` — this races with a `next build` in another window. Serialize builds.
- Stale `.git/rebase-merge/` directories from Finder duplication cause a false "rebasing" status. Delete the directory.
- Multi-session edit drift — always `git status` before writing, especially in worktrees.

## Vercel deploy discipline

- Every prod push should be tested in a preview build first
- Preview URL for DSP requires manual Vercel env var setup (see Neon static var note above)
- If a deploy fails with `Unexpected token '<' DOCTYPE` on client API calls, `NEXT_PUBLIC_API_URL` is missing `/api/v1` suffix

## Secret handling

- All prod secrets are managed in Vercel env or the appropriate CI secret store
- `.env` files stay local, never committed
- On offboarding, every credential the person touched rotates
- **Never `--no-verify`** to skip commit hooks unless Priyesh explicitly says so

## Committing / pushing

- Commit and push after every edit — default behavior, don't wait for approval
- Never `git reset --hard` or `push --force` to main without explicit approval
- Never delete branches without approval; investigate unfamiliar branches first

## Monday.com (task tracking)

- CLI: `~/Desktop/pgam-intelligence/scripts/monday_cli.py`
- Auth: `MONDAY_API_TOKEN` in `~/Desktop/pgam-intelligence/.env`
- Close a ticket after shipping: `python3 scripts/monday_cli.py close <item_id>`
- Default board: DSP Dev Work (18406313526)

## Analytics / observability

- **GA4 digest** — daily via GitHub Actions WIF for Destination + BoxingNews. No JSON keys (org policy blocks).
- **GSC API** — callable via `npx tsx scripts/gsc.ts ...` in boxingnews repo. SA `analytics-digest@pgam-analytics` reused from GA4 digest.
- **Partner Revenue Dashboard** — `admin.pgammedia.com`, LL+TB unified

## Integrations (Claude sessions: read this before asking a human)

Every programmatic route we already have — MCP connectors, REST clients, CLIs,
credentials by name — is catalogued in `.claude/INTEGRATIONS.md`, and the rule
governing when a manual ask is allowed is the **External Platform Access
Policy** in `CLAUDE.md`.

Two things trip sessions up repeatedly:

1. **MCP tools are deferred.** They are absent from the base tool list until
   `ToolSearch` loads them. A missing tool name is not missing access.
2. **Cloud sessions have no `.env` and GitHub-only egress.** Direct API calls
   to any other host return `CONNECT tunnel failed, 403` — that is org egress
   policy, not a bad token. Use the MCP connector.

`python3 scripts/check_integrations.py` prints which routes this session
actually has (credential presence only, never values).

## When you get stuck

1. Check `~/.claude/projects/-Users-priyeshpatel-Desktop-pgam-intelligence/memory/MEMORY.md` for a relevant memory
2. Check `git log` on the file to see when it last changed and why
3. Check Monday for related tickets — via `mcp__monday_com__*`, or
   `scripts/monday_cli.py` locally. Don't ask Priyesh to open the board.
4. Ask Priyesh — do not guess on P&L or partner-touching decisions
