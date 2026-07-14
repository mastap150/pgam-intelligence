# Non-Negotiables

Rules where breaking one costs real money, trust, or a client. Read this before touching anything. When in doubt, stop and ask.

## Commercial — data that must not leak

1. **Never leak agency names to SpringServe.** Entrepreneur, InHouse, and other agency identifiers stay in Neon only. Never in SS campaign, tag, or creative names. Never in SS notes.
2. **Rate hiding.** Gross CPM (what the advertiser pays us) is PGAM-dashboard-only. SpringServe demand tags only ever carry the media-cost CPM. Do not surface gross rates in SS ever.
3. **DSP CPM one-way ratchet.** The setup CPM on a campaign is a hard ceiling. Descending is fine (that's margin capture, with rollback safety). Ascending past setup CPM is not — the buyer agent must never do it, and neither should you.

## Partner-mandated limits (breaking these damages the relationship)

4. **Unruly write freeze** (dp=5). All automated writes to Unruly are choke-pointed off in `core/partner_freeze.py`. Do not remove the freeze until the compliance root-cause is fixed. If you're building a new writer, check the freeze list first.
5. **9 Dots contract floor.** Demands 692, 693, and 955 have a **$1.70 minimum** floor. Floors may go higher; they may never go lower. This is contractual.
6. **BidMachine partner QPS cap** (dp=40). The QPS cap is partner-mandated, not internal caution. 99% utilization is expected, not an opportunity to raise.

## People / access

7. **No P&L access for Vivek.** Codeowners locks Vivek out of financial paths; Clerk metadata restricts UI; SSP DB role is scoped. Do not widen his access. If a PR touches a P&L path, it does not merge without Priyesh review.
8. **Do not re-grant access to Joseph Roa** (offboarded 2026-05-19). All four DSP secrets were rotated. Auth lockdown shipped (PRs #200/#211/#213/#215).
9. **LL team UI access** — Sagar and possibly others have UI access. If you see unexplained drift in LL state that our ledger didn't cause, ask the team before assuming automation bug.

## Repo / code discipline

10. **Never `git add -A` in a worktree on pgam-dsp-dashboard.** Inspect `git status --short` first. Untracked `.env*` in DSP worktrees leaked prod secrets to main on 2026-07-02. Always add files by name.
11. **`.env` files are gitignored on pgam-dsp-dashboard** (since 2026-07-10). New worktrees must symlink both `node_modules` AND `.env.local` from the main tree, or the pre-push `next build` will fail.
12. **Always commit and push after edits.** Do not leave uncommitted work sitting on Priyesh's machine. Auto-commit is the default.
13. **QA after every ship.** Run the workflow, hit the endpoint, spot-check the DB row. No ship-and-assume.
14. **Never skip pre-commit / pre-push hooks (`--no-verify`, `--no-gpg-sign`)** unless Priyesh explicitly says to. If a hook fails, fix the underlying issue.
15. **Never `git reset --hard`, `push --force` to main, or delete a branch** without explicit approval. Investigate unfamiliar files/branches before overwriting — they may be someone's in-progress work.

## WP / content platform hygiene (learned the hard way)

16. **Dangling DNS is an attack surface.** The 2026-05-07 boxingnews `admin.` subdomain incident: a stale DNS record pointed at a recycled cPanel IP; a writer's WP creds were likely harvested. Delete DNS records for services you're not using.
17. **WP hygiene bar (regardless of host):** 2FA on every account, plugins/core auto-update, writers get Author role (not Editor/Admin), auto-update security patches. Multiple WPE hacks have been reported — the vector is always hygiene, not the host.

## Secret handling

18. **Secret rotation on offboarding.** When anyone with access leaves, every credential they touched rotates. Roa playbook is the reference.
19. **Managed secrets:** `DSP+SSP share one Neon project` (round-frog-99233431); DSP is `public`, SSP is `pgam_direct`. Do not cross the streams.
20. **`NEXT_PUBLIC_API_URL` on DSP must end in `/api/v1`.** A bare host breaks every client API call with `Unexpected token '<' DOCTYPE` errors. Common footgun on new deploys.

## When in doubt

Ask Priyesh before doing anything that:
- writes to a partner's system
- touches P&L data
- rotates or reveals a secret
- deletes anything on a shared system
- pushes to `main` on any repo without a preview build
