# Proposal: env var fallback for LLM proxy credentials

## Context

`post_export` allows running scripts after export. Combined with `.env` / `env_file`, we can pass API credentials to `start_app` → `proxy_llm.py` without manual setup.

## Current behavior

`proxy_llm.py` reads credentials **only** from `llm-config.json` (user config file). If missing, the user must call `/set_keys` manually.

## Proposed change

In `proxy_llm.py`, add env var fallback in `load_config()`:

```python
def load_config():
    # 1. Config file (existing — keeps priority)
    config_path = get_config_path()
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")

    # 2. Env var fallback (NEW)
    api_key = os.environ.get("INFOMANIAK_API_KEY")
    product_id = os.environ.get("INFOMANIAK_PRODUCT_ID")
    if api_key and product_id:
        return {"api_key": api_key, "product_id": product_id}

    return None
```

## Usage

```yaml
# catalog.yml
env_file: .env
post_export:
  - generate_links
  - start_app
```

```bash
# .env
INFOMANIAK_API_KEY=xxx
INFOMANIAK_PRODUCT_ID=yyy
```

datannurpy loads `.env` into `os.environ` → `post_export` runs `start_app.py` → launches `proxy_llm.py` → reads env vars → LLM ready, no manual `/set_keys` needed.

## Priority

1. `llm-config.json` (persistent user config — wins if present)
2. `INFOMANIAK_API_KEY` + `INFOMANIAK_PRODUCT_ID` env vars (project config via `.env`)
3. `/set_keys` endpoint (runtime setup — unchanged)

## Notes

- `start_app` blocks (`serve_forever`) — must be the **last** entry in `post_export`
- No changes needed in datannurpy — env vars are inherited by subprocess automatically
- ~5 lines changed in `proxy_llm.py`
