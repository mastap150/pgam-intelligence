# PGAM Training

Source of truth for onboarding and standard operating procedures. Everything a new hire needs to be productive without breaking something expensive.

## How this is organized

- `00-company.md` — what PGAM does, the two-stack rule, revenue targets, who we sell to
- `01-security-nonnegotiables.md` — the rules that, if broken, cost real money or trust. Read first.
- `02-dsp-playbook.md` — DSP (demand) ops: campaigns, ClearLine, buyer agent, rate hiding
- `03-ss-marketplace-playbook.md` — `/ss-marketplace` self-serve marketplace ops
- `04-ll-playbook.md` — LL supply platform ops
- `05-tb-playbook.md` — TB supply platform ops
- `06-engineering-playbook.md` — repos, worktrees, Neon, Vercel, deploy discipline
- `99-scribe-shotlist.md` — the ordered list of Scribe screen recordings to make
- `ONBOARDING.md` — day-1 / week-1 / week-2 sequence
- `MERGED.md` — every file concatenated. Paste into a Google Doc for a shareable link before Trainual is provisioned.

## How to use this

1. **New hire:** read `01-security-nonnegotiables.md` before touching anything. Then follow `ONBOARDING.md`.
2. **Editor (Priyesh / Claude):** edit these markdown files directly. Commit and push. Never let Trainual or a Google Doc drift ahead of the repo — this is the source of truth.
3. **Distribution:**
   - Right now: paste `MERGED.md` into a Google Doc, share the link.
   - Once Trainual is provisioned: each `.md` file becomes one Trainual Topic. Copy-paste in.
4. **Scribe:** work through `99-scribe-shotlist.md` in order. Attach recorded Scribe links inline in the relevant playbook file.

## Keeping this current

Any time a non-obvious operational rule gets established (a new floor, a new freeze, a repo pitfall discovered the hard way), add it here. If it lives only in `CLAUDE.md` / auto-memory, only Claude sessions benefit — humans need it here.
