"""
HealthNation Automation — Article Generator
Uses Claude API to produce full SEO-optimised health articles.
Returns a structured dict ready for WordPress publishing.
"""
import re
import json
import logging
import anthropic
from config import ANTHROPIC_API_KEY, CLAUDE_MODEL, CLAUDE_MAX_TOKENS

logger = logging.getLogger(__name__)
client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """
You are a senior health and science writer for HealthNation, an evidence-based health publication.
Your writing is informed by deep knowledge of nutritional science, exercise physiology, and medical literature.

WRITING RULES:
- Every health claim is backed by published research — cite studies as: "A 2022 RCT published in [Journal] found..."
- Write in plain, direct English — no wellness jargon, no pseudoscience
- Never make absolute health claims ("cures", "eliminates", "guaranteed to")
- Include both sides of the evidence where it is genuinely contested
- Practical application follows from the science, not the other way around

FORBIDDEN WORDS/PHRASES:
superfoods, toxins, cleanse, boost your immune system, miracle, detox, skyrocket,
game-changer, revolutionary, unlock your potential, amazing, incredible

TONE: Authoritative but accessible. A smart friend who happens to be a doctor.
""".strip()


USER_PROMPT_TEMPLATE = """
Write a comprehensive, SEO-optimised health article with these parameters:

TOPIC: {topic}
TARGET KEYWORD: {keyword}
SECONDARY KEYWORDS: {secondary_keywords}
CATEGORY: {category}
WORD COUNT TARGET: {word_count}

REQUIRED OUTPUT FORMAT — respond with ONLY valid JSON, no markdown fences:

{{
  "meta_title": "<SEO title, 50-60 characters, includes keyword>",
  "meta_description": "<145-155 characters, includes keyword and a clear benefit>",
  "h1": "<Exact article H1 — includes target keyword naturally>",
  "focus_keyword": "{keyword}",
  "reviewer_specialty": "<Recommended medical specialty for reviewer e.g. 'Gastroenterology' or 'Sports Medicine'>",
  "key_takeaways": [
    "<Takeaway 1 — single sentence, most important point>",
    "<Takeaway 2>",
    "<Takeaway 3>",
    "<Takeaway 4>"
  ],
  "html_content": "<Full article body as HTML — NO <html>, <head>, <body> tags. Start from first <h2>. Min {word_count} words. Include: table of contents anchor links, 4+ H2 sections, H3 subsections, at least one comparison table, a key FAQ section with 4 questions, inline [INTERNAL LINK: topic] placeholders for 3 internal links, a Bottom Line H2 section>",
  "excerpt": "<2-3 sentence summary for post excerpt, 50-80 words>",
  "references": [
    "<Citation 1: Author et al. Year. Title. Journal. DOI or PMID.>",
    "<Citation 2>",
    "<Citation 3>"
  ],
  "unsplash_search_query": "<2-4 word Unsplash search query that will return a relevant lifestyle/health photo for this article topic>",
  "estimated_read_time": <integer, minutes>,
  "citation_count": <integer>
}}

ARTICLE STRUCTURE REQUIREMENTS:
1. H1 must contain the exact target keyword
2. First H2: What [Topic] Actually Means (or equivalent clear definition)
3. H2: What the Research Says
   - H3 for each key finding with study citations
4. H2: How to Apply This Practically (step-by-step or protocol)
5. H2: Common Mistakes (4-6 items)
6. H2: Expert Recommendations
7. H2: FAQ (4 questions with concise answers)
8. H2: The Bottom Line (2-3 sentences)
9. Add a Table of Contents as the first element with anchor links to each H2

IMPORTANT: Return ONLY the JSON object. No prose before or after.
""".strip()


def generate_article(topic: dict) -> dict:
    """
    Generate a full article from a topic dict.

    topic = {
        "title":              "How to Improve Your Gut Health",
        "keyword":            "how to improve gut health",
        "secondary_keywords": "gut microbiome improvement, best foods for gut health",
        "category":           "nutrition",
        "word_count":         2200
    }

    Returns enriched dict with all generated fields.
    """
    logger.info(f"Generating article: {topic['title']}")

    prompt = USER_PROMPT_TEMPLATE.format(
        topic             = topic["title"],
        keyword           = topic["keyword"],
        secondary_keywords= topic.get("secondary_keywords", ""),
        category          = topic.get("category", "health"),
        word_count        = topic.get("word_count", 2000),
    )

    response = client.messages.create(
        model      = CLAUDE_MODEL,
        max_tokens = CLAUDE_MAX_TOKENS,
        system     = SYSTEM_PROMPT,
        messages   = [{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()

    # Strip markdown code fences if model adds them anyway
    raw = re.sub(r'^```(?:json)?\s*', '', raw)
    raw = re.sub(r'\s*```$',          '', raw)

    try:
        article = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse failed: {e}\nRaw output (first 500 chars):\n{raw[:500]}")
        raise ValueError("Claude returned invalid JSON. Check CLAUDE_MAX_TOKENS or prompt.") from e

    # Merge original topic data into result
    article["source_topic"]  = topic
    article["category_slug"] = topic.get("category", "health")

    # Process internal link placeholders
    article["html_content"] = _process_internal_links(article["html_content"])

    # Add medical disclaimer to bottom of content
    article["html_content"] += MEDICAL_DISCLAIMER_HTML

    logger.info(f"  ✓ Generated: {article['h1']} ({article.get('estimated_read_time', '?')} min read)")
    return article


def _process_internal_links(html: str) -> str:
    """
    Replace [INTERNAL LINK: topic] placeholders with styled anchor tags.
    These become real links once the site has content — for now styled as pending.
    """
    def replace_link(match):
        topic_text = match.group(1).strip()
        slug = topic_text.lower().replace(" ", "-").replace("/", "-")
        return (
            f'<a href="/#{slug}" class="internal-link" '
            f'data-link-topic="{topic_text}">{topic_text}</a>'
        )
    return re.sub(r'\[INTERNAL LINK:\s*([^\]]+)\]', replace_link, html)


MEDICAL_DISCLAIMER_HTML = """
<div class="medical-disclaimer" role="note">
  <strong>Medical Disclaimer:</strong> This article is for informational purposes only
  and does not constitute medical advice, diagnosis, or treatment. Always consult a
  qualified healthcare provider before making changes to your diet, exercise routine,
  supplement regimen, or any other health-related decisions.
</div>
"""
