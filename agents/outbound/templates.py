"""
agents/outbound/templates.py
────────────────────────────
Reference email sequences for the SDR agent. Instantly itself is the
source of truth at send-time — these strings exist here so that the
copy is versioned with the rest of the agent and so a code review on
PRs catches copy drift.

VOICE
─────
Jordan Reilly, plain-text BDR voice. Short. Specific. No marketing-
speak. No "I hope this finds you well." No "circling back." One
question or one hook per touch.

PERSONALIZATION VARIABLES
─────────────────────────
Instantly variable syntax: {{firstName}}, {{companyName}}, {{title}}.
Custom variables defined on the Instantly campaign:
    {{currentChannel}}   — what we believe they're running today
                           (only set when we have a real signal,
                            otherwise omit the sentence)
    {{vertical}}         — vertical hook for SKU 2 leads

CADENCE
───────
4 touches over 14 days. Day 0, +3, +7, +14. Touch 4 is the break-up.
A 5th reactivation touch ships on +30 only if reply rate on touches
1-4 is below 4%; otherwise we'd rather move the budget to fresh ICP.
"""

from __future__ import annotations


# ─────────────────────────────────────────────────────────────────────
# SKU 1 — Brand Awareness
# ─────────────────────────────────────────────────────────────────────
BRAND_AWARENESS_SEQUENCE = [
    {
        "day_offset": 0,
        "subject_options": [
            "{{companyName}} + CTV attention",
            "quick CTV question",
            "{{firstName}} — CTV attention layer",
        ],
        "body": (
            "{{firstName}} —\n\n"
            "PGAM Media runs CTV and OLV with two things most platforms don't "
            "ship: a per-impression attention score and TFN-matched call "
            "attribution, both included, not upcharged.\n\n"
            "If {{companyName}} is running CTV this quarter, worth 15 minutes "
            "to compare what we'd show you against your current reporting?\n\n"
            "Jordan\n"
            "PGAM Media"
        ),
    },
    {
        "day_offset": 3,
        "subject_options": [
            "re: {{companyName}} + CTV attention",
            "one data point",
        ],
        "body": (
            "{{firstName}} —\n\n"
            "Quick follow-up. On a recent B2B CTV brand campaign we ran "
            "through our DSP, per-creative attention scores let the brand "
            "team kill the bottom-third creatives on day 4 instead of day "
            "30. Same budget, ~22% lift on completion-weighted reach.\n\n"
            "Happy to walk through the dashboard live if useful.\n\n"
            "Jordan"
        ),
    },
    {
        "day_offset": 7,
        "subject_options": [
            "{{firstName}} — one question",
            "how are you measuring CTV today?",
        ],
        "body": (
            "{{firstName}} —\n\n"
            "Genuinely curious — how is {{companyName}} measuring CTV brand "
            "lift today? Pixel-based, panel, post-campaign survey, "
            "something else?\n\n"
            "Asking because every brand team we talk to has a different "
            "answer and most are unhappy with theirs.\n\n"
            "Jordan"
        ),
    },
    {
        "day_offset": 14,
        "subject_options": [
            "closing the loop",
            "last one from me",
        ],
        "body": (
            "{{firstName}} —\n\n"
            "Last note from me on this. If CTV measurement isn't on the "
            "roadmap I'll get out of your inbox.\n\n"
            "If it is, here's a 2-minute Loom showing how our attention "
            "layer reads on a live campaign: <TBD loom link>\n\n"
            "Either way, good luck this quarter.\n\n"
            "Jordan\n"
            "PGAM Media"
        ),
    },
]


# ─────────────────────────────────────────────────────────────────────
# SKU 2 — Performance / CPA-Call
# ─────────────────────────────────────────────────────────────────────
PERFORMANCE_SEQUENCE = [
    {
        "day_offset": 0,
        "subject_options": [
            "{{companyName}} — call CPA on CTV",
            "performance CTV for {{vertical}}",
            "{{firstName}} — quick call-CPA question",
        ],
        "body": (
            "{{firstName}} —\n\n"
            "PGAM Media runs performance CTV for call-driven advertisers "
            "in {{vertical}}. Per-call attribution down to creative, DMA "
            "and daypart — and we sell on a fully-loaded CPA-per-"
            "qualifying-call, not on media spend.\n\n"
            "Most programs we replace are paying $80–150 per qualifying "
            "call. Worth 15 minutes to see what our number would be for "
            "{{companyName}}?\n\n"
            "Jordan\n"
            "PGAM Media"
        ),
    },
    {
        "day_offset": 3,
        "subject_options": [
            "re: {{companyName}} — call CPA",
            "the differentiator",
        ],
        "body": (
            "{{firstName}} —\n\n"
            "Quick follow-up. The reason CPA drops on our stack isn't a "
            "smarter bid algorithm — it's that we per-call attribute on "
            "day 3, not day 30. Bad creative or bad sources get paused "
            "inside the first 72 hours, so the campaign never wastes a "
            "week on them.\n\n"
            "Happy to show the dashboard if you'd like to see how it "
            "reads.\n\n"
            "Jordan"
        ),
    },
    {
        "day_offset": 7,
        "subject_options": [
            "{{firstName}} — quick number",
            "current call CPA?",
        ],
        "body": (
            "{{firstName}} —\n\n"
            "What's {{companyName}}'s blended call CPA running these days, "
            "ballpark? Asking because if you're already at $60 we're "
            "probably not the right fit, and if you're at $120+ this is "
            "almost certainly worth a conversation.\n\n"
            "Jordan"
        ),
    },
    {
        "day_offset": 14,
        "subject_options": [
            "closing the loop",
            "last one",
        ],
        "body": (
            "{{firstName}} —\n\n"
            "Last one from me. If you're happy with where call CPA is, "
            "no worries at all.\n\n"
            "If you're not — 2-minute Loom on how the attribution reads "
            "live: <TBD loom link>\n\n"
            "Good luck regardless.\n\n"
            "Jordan\n"
            "PGAM Media"
        ),
    },
]


SEQUENCES = {
    "brand_awareness": BRAND_AWARENESS_SEQUENCE,
    "performance": PERFORMANCE_SEQUENCE,
}
