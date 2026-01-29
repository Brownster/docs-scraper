#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from readability import Document
from tqdm import tqdm


@dataclass
class Chunk:
    chunk_id: str
    text: str
    metadata: dict


def norm_url(u: str) -> str:
    # strip fragments, keep query (docs sometimes use it)
    p = urlparse(u)
    return p._replace(fragment="").geturl()


def same_scope(url: str, base: str) -> bool:
    u = urlparse(url)
    b = urlparse(base)
    return (u.scheme, u.netloc) == (b.scheme, b.netloc)


def keep_url(u: str) -> bool:
    # Keep MediaWiki + common manuals, drop obvious junk
    p = urlparse(u)
    if "Special:" in p.path:
        return False
    if p.path.startswith("/extensions/"):
        return False
    return True


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def try_fetch_sitemap(client: httpx.Client, base: str) -> List[str]:
    """
    Try common sitemap locations. Return URLs if found, else [].
    """
    candidates = [
        urljoin(base, "/sitemap.xml"),
        urljoin(base, "/sitemap_index.xml"),
    ]
    urls: List[str] = []

    for sm in candidates:
        r = client.get(sm, follow_redirects=True, timeout=30)
        if r.status_code != 200 or "xml" not in r.headers.get("content-type", ""):
            continue

        soup = BeautifulSoup(r.text, "xml")
        locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]

        # sitemap index -> fetch child sitemaps
        if soup.find("sitemapindex"):
            for child in locs:
                cr = client.get(child, follow_redirects=True, timeout=30)
                if cr.status_code == 200:
                    cs = BeautifulSoup(cr.text, "xml")
                    urls.extend([loc.get_text(strip=True) for loc in cs.find_all("loc")])
            return sorted(set(urls))

        # regular sitemap
        if soup.find("urlset"):
            return sorted(set(locs))

    return []


def extract_main_markdown(html: str) -> Tuple[str, str]:
    """
    Return (title, markdown) from HTML.
    """
    doc = Document(html)
    title = (doc.short_title() or "").strip()

    # Readability gives main content as HTML
    main_html = doc.summary(html_partial=True)
    soup = BeautifulSoup(main_html, "lxml")

    # Drop scripts/styles just in case
    for t in soup(["script", "style", "noscript"]):
        t.decompose()

    clean_html = str(soup)
    markdown = md(clean_html, heading_style="ATX")
    markdown = re.sub(r"\n{3,}", "\n\n", markdown).strip()

    # Fallback for MediaWiki / portal pages if readability is empty/small
    if len(markdown) < 200:
        full = BeautifulSoup(html, "lxml")
        main = (
            full.select_one("#mw-content-text")
            or full.select_one(".mw-parser-output")
            or full.select_one("main")
            or full.body
        )
        if main:
            for t in main(["script", "style", "noscript", "svg"]):
                t.decompose()
            markdown = md(str(main), heading_style="ATX").strip()

    markdown = re.sub(r"\n{3,}", "\n\n", markdown).strip()
    return title, markdown


def split_by_headings(markdown: str) -> List[Tuple[str, str]]:
    """
    Split markdown into sections by headings.
    Returns list of (heading, section_text) where heading may be "" for preface.
    """
    lines = markdown.splitlines()
    sections: List[Tuple[str, List[str]]] = []
    current_heading = ""
    current: List[str] = []

    heading_re = re.compile(r"^(#{1,6})\s+(.*)\s*$")

    for line in lines:
        m = heading_re.match(line)
        if m:
            # flush previous
            if current:
                sections.append((current_heading, current))
            current_heading = m.group(2).strip()
            current = [line]
        else:
            current.append(line)

    if current:
        sections.append((current_heading, current))

    out: List[Tuple[str, str]] = []
    for h, body in sections:
        text = "\n".join(body).strip()
        if text:
            out.append((h, text))
    return out


def approx_tokens(s: str) -> int:
    # cheap estimate: ~4 chars per token average
    return max(1, len(s) // 4)


def chunk_sections(
    sections: List[Tuple[str, str]],
    url: str,
    title: str,
    source: str,
    min_tok: int,
    max_tok: int,
) -> List[Chunk]:
    chunks: List[Chunk] = []
    buf = ""
    buf_path = []

    def flush(buf_text: str, section_path: List[str]):
        buf_text = buf_text.strip()
        if not buf_text:
            return
        cid = sha1(url + "\n" + buf_text)[:16]
        chunks.append(
            Chunk(
                chunk_id=cid,
                text=buf_text,
                metadata={
                    "source": source,
                    "url": url,
                    "title": title,
                    "section_path": " > ".join([p for p in section_path if p]),
                },
            )
        )

    for heading, sec in sections:
        sec_tok = approx_tokens(sec)

        # Keep a simple section path: page title + current heading
        section_path = [title] if title else []
        if heading:
            section_path.append(heading)

        if not buf:
            buf = sec
            buf_path = section_path
            continue

        # If adding would exceed max, flush first
        if approx_tokens(buf) + sec_tok > max_tok:
            # If buf is tiny, still flush it; better small than giant
            flush(buf, buf_path)
            buf = sec
            buf_path = section_path
        else:
            buf = (buf + "\n\n" + sec).strip()
            buf_path = buf_path or section_path

        # If buffer is comfortably large, flush to keep chunks tidy
        if approx_tokens(buf) >= min_tok:
            flush(buf, buf_path)
            buf = ""
            buf_path = []

    if buf:
        flush(buf, buf_path)

    return chunks


def extract_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        u = norm_url(urljoin(base_url, href))
        links.append(u)
    return links


def is_html_response(resp: httpx.Response) -> bool:
    ct = resp.headers.get("content-type", "")
    return "text/html" in ct or "application/xhtml+xml" in ct


def load_cookies_txt(path: str) -> httpx.Cookies:
    """
    Load a Netscape cookie file into an httpx.Cookies jar.
    """
    jar = httpx.Cookies()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            domain, include_subdomains, path, secure, expires, name, value = parts[:7]
            try:
                jar.set(name, value, domain=domain, path=path)
            except Exception:
                continue
    return jar


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://all.docs.genesys.com/GenesysCloud/", help="Base URL scope")
    ap.add_argument("--out", default="genesys_chunks.jsonl", help="Output JSONL file")
    ap.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    ap.add_argument("--max-pages", type=int, default=5000, help="Safety cap")
    ap.add_argument("--user-agent", default="InternalDocsCrawler/1.0", help="User-Agent string")
    ap.add_argument("--cookies", help="Path to Netscape cookies.txt file")
    ap.add_argument("--cookie-header", help="Raw Cookie header value")
    ap.add_argument("--min-tokens", type=int, default=250, help="Min chunk size (approx tokens)")
    ap.add_argument("--max-tokens", type=int, default=900, help="Max chunk size (approx tokens)")
    args = ap.parse_args()

    base = norm_url(args.base)
    source = "GenesysCloud"

    headers = {"User-Agent": args.user_agent}
    if args.cookie_header:
        headers["Cookie"] = args.cookie_header

    cookies = None
    if args.cookies:
        cookies = load_cookies_txt(args.cookies)

    seen = set()
    queue: List[str] = []
    urls_from_sitemap: List[str] = []

    with httpx.Client(headers=headers, cookies=cookies) as client:
        # Prefer sitemap
        urls_from_sitemap = try_fetch_sitemap(client, base)
        if urls_from_sitemap:
            queue = [u for u in urls_from_sitemap if same_scope(u, base) and keep_url(u)]
        else:
            queue = [base]

        total_written = 0

        with open(args.out, "w", encoding="utf-8") as f:
            pbar = tqdm(total=min(len(queue), args.max_pages), desc="Crawling (sitemap or seed)")
            i = 0
            while i < len(queue) and i < args.max_pages:
                url = norm_url(queue[i])
                i += 1
                pbar.update(1)
                if url in seen:
                    continue
                seen.add(url)

                try:
                    r = client.get(url, follow_redirects=True, timeout=30)
                except Exception:
                    continue

                print(
                    f"Fetched {url} status={r.status_code} ct={r.headers.get('content-type')} len={len(r.text)}"
                )
                if r.status_code != 200 or not is_html_response(r):
                    time.sleep(args.delay)
                    continue

                html = r.text

                # If we didn't have a sitemap, discover more links as we go
                if not urls_from_sitemap:
                    for link in extract_links(html, url):
                        if same_scope(link, base) and keep_url(link) and link not in seen:
                            queue.append(link)
                    if len(queue) > args.max_pages:
                        queue = queue[: args.max_pages]
                    if pbar.total < min(len(queue), args.max_pages):
                        pbar.total = min(len(queue), args.max_pages)
                        pbar.refresh()

                title, markdown = extract_main_markdown(html)
                print(f"Extracted markdown chars={len(markdown)} title={title!r}")
                if not markdown or len(markdown) < 200:
                    time.sleep(args.delay)
                    continue

                sections = split_by_headings(markdown)
                chunks = chunk_sections(
                    sections=sections,
                    url=url,
                    title=title,
                    source=source,
                    min_tok=args.min_tokens,
                    max_tok=args.max_tokens,
                )

                for c in chunks:
                    f.write(json.dumps({"id": c.chunk_id, "text": c.text, "metadata": c.metadata}, ensure_ascii=False) + "\n")
                    total_written += 1

                time.sleep(args.delay)

            pbar.close()

    print(f"Done. Wrote {total_written} chunks to {args.out}")


if __name__ == "__main__":
    main()
