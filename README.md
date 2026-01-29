# Docs Scraper (Genesys Cloud / MediaWiki)

Small crawler + extractor that turns Genesys docs pages into JSONL chunks for downstream search/embedding.

## Quick start

```bash
python scraper.py --base "https://all.docs.genesys.com/System/AllContent"
```

Output defaults to `genesys_chunks.jsonl` in the repo root.

## Authenticated pages (optional)

Some docs require login and return a "Login required" page. You can pass your authenticated cookies:

### 1) Netscape cookies.txt (recommended)

```bash
python scraper.py \
  --base "https://all.docs.genesys.com/System/AllContent" \
  --cookies /path/to/cookies.txt
```

### 2) Raw Cookie header

```bash
python scraper.py \
  --base "https://all.docs.genesys.com/System/AllContent" \
  --cookie-header "key=val; key2=val2"
```

## Notes

- Scope is host-only, so the crawler can follow links across manuals on `all.docs.genesys.com`.
- A simple `keep_url()` filter skips obvious junk like `Special:` and `/extensions/`.
- Extraction falls back to MediaWiki content containers when Readability is too small.
- The crawler grows its queue as it discovers new links (not just the initial seed).

## CLI options

```
python scraper.py --help
```
