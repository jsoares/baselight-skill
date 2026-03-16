# baselight-skill

An [Agent Skill](https://agentskills.io/) that provides access to [Baselight](https://baselight.ai) — a data platform with thousands of queryable public datasets covering crypto/blockchain, finance, demographics, climate, healthcare, sports, and more.

## What it does

When you ask a data question, your agent will search Baselight's catalog, inspect the relevant table schema, and query it directly using DuckDB SQL — returning actual data instead of web search snippets.

Example triggers:
- "Compare GDP across countries"
- "Show me happiness score trends"
- "Query `@owid.happiness.owid_happiness_2`"

## Installation

### npx installation

```
npx skills add jsoares/baselight-skill --skill baselight -g
```

### Manual installation

Copy the skill into your agent's skills directory. Example for Claude Code:

```bash
cp -r skills/baselight ~/.claude/skills/baselight
```

It should pick it up automatically on the next session.

## Setup

You need a Baselight API key:

1. Sign up at [baselight.app](https://baselight.app)
2. Go to **Account Settings → Integrations → Generate New API Key**
3. Save it to `~/.baselight/credentials`:

```bash
mkdir -p ~/.baselight
echo 'BASELIGHT_API_KEY=your-key-here' >> ~/.baselight/credentials
chmod 600 ~/.baselight/credentials
```

## Structure

```
skills/baselight/
├── SKILL.md                    # Skill definition and instructions for Claude
├── scripts/
│   └── baselight.py            # Python MCP client (no extra deps beyond requests)
└── references/
    └── sql-patterns.md         # DuckDB SQL reference and examples
```

## Requirements

Python 3 with `requests`:

```bash
pip install requests
```

## License

MIT
