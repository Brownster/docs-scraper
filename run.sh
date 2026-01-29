#bin bash
python scraper.py \
  --base "https://all.docs.genesys.com/GenesysCloud/" \
  --out genesys_chunks.jsonl \
  --delay 1.0 \
  --max-pages 8000
