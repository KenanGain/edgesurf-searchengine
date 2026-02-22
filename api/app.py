"""
Ultimate Search Engine — Unified API Gateway
Aggregates 21+ search engines, crawlers & extractors into one REST API.
All free. All self-hosted. Zero SaaS fees.
"""

import asyncio
import os
import time
import hashlib
import json
import zlib
import orjson
import redis.asyncio as redis
from typing import Optional
from contextlib import asynccontextmanager
from collections import OrderedDict
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import datetime
import uuid
import ipaddress
import socket
import time

import httpx
from fastapi import FastAPI, Query, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from duckduckgo_search import DDGS
from bs4 import BeautifulSoup
from fastembed import TextEmbedding
from sentence_transformers import CrossEncoder
from prometheus_client import make_asgi_app, Counter, Histogram
import copy

# ─────────────────────────────────────────
#  DATA MODELS
# ─────────────────────────────────────────
class DocumentChunk(BaseModel):
    chunk_id: str
    text: str
    index: int

class UnifiedDocument(BaseModel):
    doc_id: str
    url: str
    canonical_url: str
    title: str = ""
    content: str = ""
    content_chunks: List[DocumentChunk] = Field(default_factory=list)
    lang: str = "en"
    site: str = ""
    domain: str = ""
    path: str = ""
    published_at: Optional[str] = None
    crawled_at: str = Field(default_factory=lambda: datetime.datetime.utcnow().isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.datetime.utcnow().isoformat())
    content_type: str = ""
    filetype: str = ""
    headers: Dict[str, str] = Field(default_factory=dict)
    meta: Dict[str, Any] = Field(default_factory=dict)
    links_out: List[str] = Field(default_factory=list)
    links_in_count: int = 0
    source: str = "manual"

class CrawlProfile(BaseModel):
    job_name: str
    seeds: list[str]
    depth: int = 2
    max_pages: int = 100
    crawler_engine: str = "nutch"
    allow_domains: list[str] = Field(default_factory=list)
    block_patterns: list[str] = Field(default_factory=lambda: [".jpg", ".png", ".css", ".js"])


# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
SEARXNG_URL = os.getenv("SEARXNG_URL", "http://searxng:8080")
WHOOGLE_URL = os.getenv("WHOOGLE_URL", "http://whoogle:5000")
MEILISEARCH_URL = os.getenv("MEILISEARCH_URL", "http://meilisearch:7700")
MEILI_MASTER_KEY = os.getenv("MEILI_MASTER_KEY", "master-key-change-me-12345")
TYPESENSE_URL = os.getenv("TYPESENSE_URL", "http://typesense:8108")
TYPESENSE_API_KEY = os.getenv("TYPESENSE_API_KEY", "typesense-key-change-me")
QUICKWIT_URL = os.getenv("QUICKWIT_URL", "http://quickwit:7280")
OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
MANTICORE_URL = os.getenv("MANTICORE_URL", "http://manticore:9308")
SOLR_URL = os.getenv("SOLR_URL", "http://solr:8983")
ZINCSEARCH_URL = os.getenv("ZINCSEARCH_URL", "http://zincsearch:4080")
ZINC_USER = os.getenv("ZINC_USER", "admin")
ZINC_PASSWORD = os.getenv("ZINC_PASSWORD", "ComplexPass123!")
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://weaviate:8080")
REDISEARCH_HOST = os.getenv("REDISEARCH_HOST", "localhost")
REDISEARCH_PORT = int(os.getenv("REDISEARCH_PORT", "6379"))
YACY_URL = os.getenv("YACY_URL", "http://yacy:8090")

# New engines
FESS_URL = os.getenv("FESS_URL", "http://fess:8080")
VESPA_URL = os.getenv("VESPA_URL", "http://vespa:8080")
VESPA_CONFIG_URL = os.getenv("VESPA_CONFIG_URL", "http://vespa:19071")
TIKA_URL = os.getenv("TIKA_URL", "http://tika:9998")

# Crawlers
HERITRIX_URL = os.getenv("HERITRIX_URL", "https://heritrix:8443")
HERITRIX_USER = os.getenv("HERITRIX_USER", "admin")
HERITRIX_PASSWORD = os.getenv("HERITRIX_PASSWORD", "admin123")
NUTCH_URL = os.getenv("NUTCH_URL", "http://nutch:8084")

REQUEST_TIMEOUT = 10.0
API_KEY = os.getenv("API_KEY", "")

ENGINE_TIMEOUTS = {
    "duckduckgo": 6.0,
    "searxng": 5.0,
    "whoogle": 7.0,
    "yacy": 15.0,
    "meilisearch": 3.0,
    "typesense": 3.0,
    "opensearch": 5.0,
    "manticore": 3.0,
    "solr": 5.0,
    "zincsearch": 5.0,
    "quickwit": 3.0,
    "weaviate": 6.0,
    "qdrant": 4.0,
    "redisearch": 3.0,
    "fess": 8.0,
    "vespa": 5.0,
}

ENGINE_SEMAPHORE = asyncio.Semaphore(12)  # prevents 1000 concurrent outbound calls

TRACKING_PARAMS_PREFIX = ("utm_",)
TRACKING_PARAMS_EXACT = {"gclid", "fbclid", "msclkid"}


def require_key(x_api_key: str | None):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Invalid API key")


# ─────────────────────────────────────────
#  L1 / L2 CACHE
# ─────────────────────────────────────────
class LRUCache:
    def __init__(self, maxsize=500, ttl=300):
        self.cache = OrderedDict()
        self.maxsize = maxsize
        self.ttl = ttl

    def _hash(self, key: str) -> str:
        return hashlib.md5(key.encode()).hexdigest()

    def get(self, key: str):
        h = self._hash(key)
        if h in self.cache:
            val, ts = self.cache[h]
            if time.time() - ts < self.ttl:
                self.cache.move_to_end(h)
                return copy.deepcopy(val)
            del self.cache[h]
        return None

    def set(self, key: str, value):
        h = self._hash(key)
        self.cache[h] = (copy.deepcopy(value), time.time())
        if len(self.cache) > self.maxsize:
            self.cache.popitem(last=False)

cache = LRUCache(maxsize=500, ttl=300)

async def cache_get(key: str):
    # L1 Cache hit
    v = cache.get(key)
    if v is not None:
        return v
    # L2 Redis hit
    raw = None
    if app.state.redis:
        try:
            raw = await app.state.redis.get(f"search:{key}")
        except Exception:
            pass
    if not raw:
        return None
    data = orjson.loads(zlib.decompress(raw))
    cache.set(key, data)
    return data

async def cache_set(key: str, value: dict, ttl: int = 300):
    cache.set(key, value)
    if app.state.redis:
        try:
            raw = zlib.compress(orjson.dumps(value))
            await app.state.redis.setex(f"search:{key}", ttl, raw)
        except Exception:
            pass


# ─────────────────────────────────────────
#  LIFESPAN — shared HTTP client & Redis & Embedder
# ─────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connections pooling
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=40)
    timeout = httpx.Timeout(connect=3.0, read=REQUEST_TIMEOUT, write=10.0, pool=3.0)
    app.state.http = httpx.AsyncClient(timeout=timeout, follow_redirects=True, limits=limits)
    
    # Text Embedding model instantiation
    try:
        app.state.embedder = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
    except Exception as e:
        print(f"Warning: Failed to load TextEmbedding: {e}")
        app.state.embedder = None
        
    # CrossEncoder Reranker instantiation
    try:
        app.state.reranker = CrossEncoder("BAAI/bge-reranker-base")
    except Exception as e:
        print(f"Warning: Failed to load CrossEncoder reranker: {e}")
        app.state.reranker = None
        
    # Redis Integration 
    try:
         app.state.redis = redis.Redis(host=REDISEARCH_HOST, port=REDISEARCH_PORT, decode_responses=False)
    except Exception as e:
         print(f"Warning: Failed to connect to Redis: {e}")
         app.state.redis = None

    yield

    await app.state.http.aclose()
    if app.state.redis:
         await app.state.redis.aclose()


app = FastAPI(
    title="Ultimate Search Engine API",
    description="Unified gateway to 21+ search engines, crawlers & extractors — all free, all local",
    version="3.0.0",
    lifespan=lifespan,
)

# ─────────────────────────────────────────
#  OBSERVABILITY METRICS
# ─────────────────────────────────────────
SEARCH_REQUESTS = Counter("search_requests_total", "Total search requests")
SEARCH_LATENCY = Histogram("search_latency_seconds", "Search latency in seconds")
ENGINE_ERRORS = Counter("engine_errors_total", "Total engine errors", ["engine"])

app.mount("/metrics", make_asgi_app())

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.url.path.startswith("/search") or request.url.path.startswith("/index") or request.url.path.startswith("/ingest") or request.url.path.startswith("/crawl"):
        x_api_key = request.headers.get("x-api-key")
        if getattr(request.app.state, "redis", None) and x_api_key:
            current = int(time.time())
            window = current - (current % 60)
            k = f"rl:{x_api_key}:{window}"
            try:
                count = await request.app.state.redis.incr(k)
                if count == 1:
                    await request.app.state.redis.expire(k, 60)
                if count > 200:
                    return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded (200 req/min)"})
            except Exception:
                pass
    return await call_next(request)

app.add_middleware(
    CORSMiddleware,
    # Using specific domains in production since credentials might be True
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000", "http://127.0.0.1:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────
def http() -> httpx.AsyncClient:
    return app.state.http


def embed(text: str) -> list[float]:
    if not app.state.embedder:
         return []
    try:
        vec = next(app.state.embedder.embed([text]))
        return [float(x) for x in vec]
    except Exception:
        return []

def is_safe_url(url: str) -> bool:
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https"):
        return False
    try:
        ip = socket.gethostbyname(parsed.hostname)
        ip_obj = ipaddress.ip_address(ip)
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
            return False
    except Exception:
        return False
    return True

def normalize_result(title: str, url: str, snippet: str, source: str, **extra) -> dict:
    r = {
        "title": (title or "").strip(),
        "url": (url or "").strip(),
        "snippet": (snippet or "").strip(),
        "source": source,
        "img_src": extra.pop("img_src", ""),
        "thumbnail": extra.pop("thumbnail", ""),
        "filetype": extra.pop("filetype", ""),
        "category": extra.pop("category", ""),
        "engine": extra.pop("engine", ""),
    }
    r.update(extra)
    return r


def canonical_url(u: str) -> str:
    """Strip ubiquitous tracking params from URLs without altering search integrity."""
    u = (u or "").strip()
    if not u:
        return ""
    parts = urlsplit(u)
    qs = []
    for k, v in parse_qsl(parts.query, keep_blank_values=True):
        kl = k.lower()
        if kl in TRACKING_PARAMS_EXACT or kl.startswith(TRACKING_PARAMS_PREFIX):
            continue
        qs.append((k, v))
    clean_query = urlencode(qs, doseq=True)
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), clean_query, ""))
    return clean


async def run_engine(name: str, coro) -> list[dict]:
    """Execute search engine coroutine with semaphore and timeout limits."""
    timeout = ENGINE_TIMEOUTS.get(name, REQUEST_TIMEOUT)
    try:
        async with ENGINE_SEMAPHORE:
            return await asyncio.wait_for(coro, timeout=timeout)
    except Exception as e:
        print(f"WARN: Engine {name} failed: {e}")
        return []


# ─────────────────────────────────────────
#  ENGINE ADAPTERS
# ─────────────────────────────────────────

REGION_MAP = {
    "US": ("us-en", "en-US"),
    "UK": ("uk-en", "en-GB"),
    "CA": ("ca-en", "en-CA"),
    "AU": ("au-en", "en-AU"),
    "IN": ("in-en", "en-IN"),
    "DE": ("de-de", "de-DE"),
    "FR": ("fr-fr", "fr-FR"),
    "JP": ("jp-jp", "ja-JP"),
    "BR": ("br-pt", "pt-BR"),
    "MX": ("mx-es", "es-MX"),
    "IT": ("it-it", "it-IT"),
    "ES": ("es-es", "es-ES"),
    "RU": ("ru-ru", "ru-RU"),
    "ZA": ("za-en", "en-ZA"),
    "KR": ("kr-kr", "ko-KR"),
    "NL": ("nl-nl", "nl-NL"),
}

async def search_duckduckgo(q: str, max_results: int, categories: str = "general", safe: str = "moderate", region: str = "") -> list[dict]:
    out = []
    try:
        loop = asyncio.get_running_loop()
        def _do_search():
            ddg_region = REGION_MAP.get(region, ("wt-wt", "en"))[0] if region else "wt-wt"
            with DDGS() as ddgs:
                try:
                    if categories == "images":
                        return list(ddgs.images(q, max_results=max_results, safesearch=safe, region=ddg_region))
                    elif categories == "news":
                        return list(ddgs.news(q, max_results=max_results, safesearch=safe, region=ddg_region))
                    elif categories == "videos":
                        return list(ddgs.videos(q, max_results=max_results, safesearch=safe, region=ddg_region))
                    else:
                        return list(ddgs.text(q, max_results=max_results, safesearch=safe, region=ddg_region))
                except Exception:
                    # Fallback to HTML backend if API ratelimits us
                    return list(ddgs.text(q, max_results=max_results, backend="html", region=ddg_region))
        
        results = await loop.run_in_executor(None, _do_search)
        
        for r in results:
            title = r.get("title", "")
            url = r.get("href", r.get("url", r.get("image", "")))
            snippet = r.get("body", r.get("source", ""))
            out.append(normalize_result(
                title,
                url,
                snippet,
                "duckduckgo",
                score=max(0.1, 3.0 - (len(out) * 0.05))
            ))
        return out
    except Exception as e:
        print(f"DDG Error: {e}")
        return []


async def search_whoogle(q: str, max_results: int) -> list[dict]:
    out = []
    # Google/Whoogle supports num (up to 100 per page) and start for pagination
    per_page = min(max_results, 100)
    max_pages = max(1, (max_results + per_page - 1) // per_page)
    max_pages = min(max_pages, 3)  # cap at 3 pages to avoid overloading

    for page_idx in range(max_pages):
        if len(out) >= max_results:
            break
        try:
            resp = await http().get(
                f"{WHOOGLE_URL}/search",
                params={"q": q, "num": per_page, "start": page_idx * per_page},
                headers={"Accept": "text/html"},
            )
            resp.raise_for_status()
            text = resp.text

            soup = BeautifulSoup(text, "html.parser")
            page_results = 0

            # Strategy 1: Find result containers (div.g is Google's standard result block)
            result_divs = soup.select("div.g")
            if not result_divs:
                # Fallback: find h3 tags directly
                result_divs = []

            for div in result_divs:
                if len(out) >= max_results:
                    break
                # Find the title (h3) and its anchor
                h3 = div.find("h3")
                if not h3:
                    continue
                anchor = h3.find_parent("a") or div.find("a")
                if not anchor:
                    continue
                href = anchor.get("href", "")
                if not href or href.startswith("/search") or href.startswith("#"):
                    continue
                if "/url?q=" in href:
                    href = href.split("/url?q=")[1].split("&")[0]
                if not href.startswith("http"):
                    continue

                title = h3.get_text(strip=True)
                if not title:
                    continue

                snippet = _extract_whoogle_snippet(div, title)
                out.append(normalize_result(title, href, snippet, "whoogle", score=max(0.1, 5.0 - (len(out) * 0.1))))
                page_results += 1

            # Strategy 2: If div.g didn't work, fall back to h3-based extraction
            if page_results == 0:
                for h3 in soup.find_all("h3"):
                    if len(out) >= max_results:
                        break
                    anchor = h3.find_parent("a")
                    if not anchor:
                        # Try h3's child or sibling anchor
                        anchor = h3.find("a") or h3.find_next_sibling("a")
                    if not anchor:
                        continue
                    href = anchor.get("href", "")
                    if not href or href.startswith("/search") or href.startswith("#"):
                        continue
                    if "/url?q=" in href:
                        href = href.split("/url?q=")[1].split("&")[0]
                    if not href.startswith("http"):
                        continue

                    title = h3.get_text(strip=True)
                    if not title:
                        continue

                    parent = anchor.find_parent(["div", "li"])
                    snippet = _extract_whoogle_snippet(parent, title) if parent else ""
                    out.append(normalize_result(title, href, snippet, "whoogle", score=max(0.1, 5.0 - (len(out) * 0.1))))
                    page_results += 1

            # If we got no results on this page, stop paginating
            if page_results == 0:
                break
        except Exception:
            break

    return out


def _extract_whoogle_snippet(container, title: str) -> str:
    """Extract snippet text from a Whoogle/Google result container."""
    if not container:
        return ""
    # Try known Google snippet class selectors
    for sel in [".VwiC3b", ".IsZvec", ".lEBKkf", ".s", ".st",
                "[data-sncf]", "[data-content-feature]", ".fLtXsc", ".lyLwlc"]:
        el = container.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if text and text != title:
                return text[:500]
    # Fallback: find any text block that looks like a snippet
    for elem in container.find_all(["span", "div", "em"]):
        txt = elem.get_text(strip=True)
        if txt and txt != title and len(txt) > 30:
            return txt[:500]
    return ""


async def search_searxng(q: str, max_results: int, categories: str = "general", safe: int = 1, time_range: str = "", region: str = "") -> list[dict]:
    out = []
    page = 1
    max_pages = 10 if max_results > 10 else 3
    
    try:
        sxng_lang = REGION_MAP.get(region, ("wt-wt", "en"))[1] if region else "en"
        params = {
            "q": q, "format": "json", "categories": categories,
            "pageno": page, "language": sxng_lang,
            "safesearch": str(safe),
        }

        if time_range:
            params["time_range"] = time_range
            
        while len(out) < max_results and page <= max_pages:
            params["pageno"] = page
            resp = await http().get(f"{SEARXNG_URL}/search", params=params)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if not results:
                break
                
            for r in results:
                if len(out) >= max_results:
                    break
                
                url = r.get("url", "")
                filetype = ""
                for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".csv", ".txt", ".zip", ".rar"]:
                    if ext in url.lower():
                        filetype = ext.replace(".", "").upper()
                        break
                
                out.append(normalize_result(
                    r.get("title", ""),
                    url,
                    r.get("content", ""),
                    "searxng",
                    engine=r.get("engine", ""),
                    category=r.get("category", ""),
                    score=float(r.get("score") or max(0.1, 2.0 - (len(out) * 0.05))),
                    img_src=r.get("img_src", ""),
                    thumbnail=r.get("thumbnail", r.get("thumbnail_src", "")),
                    filetype=filetype or r.get("filetype", ""),
                ))
            page += 1
        return out
    except Exception:
        return out


async def search_meilisearch(q: str, index: str, max_results: int) -> list[dict]:
    try:
        resp = await http().post(
            f"{MEILISEARCH_URL}/indexes/{index}/search",
            json={"q": q, "limit": max_results},
            headers={"Authorization": f"Bearer {MEILI_MASTER_KEY}"},
        )
        resp.raise_for_status()
        out = []
        for hit in resp.json().get("hits", []):
            out.append(normalize_result(
                hit.get("title", hit.get("name", "")),
                hit.get("url", ""),
                hit.get("description", hit.get("content", str(hit)[:200])),
                "meilisearch",
            ))
        return out
    except Exception:
        return []


async def search_typesense(q: str, collection: str, max_results: int) -> list[dict]:
    try:
        resp = await http().get(
            f"{TYPESENSE_URL}/collections/{collection}/documents/search",
            params={"q": q, "query_by": "title,content", "per_page": max_results},
            headers={"X-TYPESENSE-API-KEY": TYPESENSE_API_KEY},
        )
        resp.raise_for_status()
        out = []
        for hit in resp.json().get("hits", []):
            doc = hit.get("document", {})
            out.append(normalize_result(
                doc.get("title", ""),
                doc.get("url", ""),
                doc.get("content", doc.get("description", ""))[:300],
                "typesense",
                score=hit.get("text_match", 0),
            ))
        return out
    except Exception:
        return []


async def search_opensearch(q: str, index: str, max_results: int) -> list[dict]:
    try:
        resp = await http().post(
            f"{OPENSEARCH_URL}/{index}/_search",
            json={
                "query": {"multi_match": {"query": q, "fields": ["title^2", "content", "description"]}},
                "size": max_results,
            },
        )
        resp.raise_for_status()
        out = []
        for hit in resp.json().get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            out.append(normalize_result(
                src.get("title", ""),
                src.get("url", ""),
                src.get("content", src.get("description", ""))[:300],
                "opensearch",
                score=hit.get("_score", 0),
            ))
        return out
    except Exception:
        return []


async def search_manticore(q: str, index: str, max_results: int) -> list[dict]:
    try:
        resp = await http().post(
            f"{MANTICORE_URL}/search",
            json={
                "index": index,
                "query": {"query_string": q},
                "limit": max_results,
            },
        )
        resp.raise_for_status()
        out = []
        for hit in resp.json().get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            out.append(normalize_result(
                src.get("title", ""),
                src.get("url", ""),
                src.get("content", str(src)[:200]),
                "manticore",
                score=hit.get("_score", 0),
            ))
        return out
    except Exception:
        return []


async def search_solr(q: str, collection: str, max_results: int) -> list[dict]:
    try:
        resp = await http().get(
            f"{SOLR_URL}/solr/{collection}/select",
            params={"q": q, "rows": max_results, "wt": "json"},
        )
        resp.raise_for_status()
        out = []
        for doc in resp.json().get("response", {}).get("docs", []):
            out.append(normalize_result(
                doc.get("title", [doc.get("id", "")])[0] if isinstance(doc.get("title"), list) else doc.get("title", doc.get("id", "")),
                doc.get("url", [""])[0] if isinstance(doc.get("url"), list) else doc.get("url", ""),
                str(doc.get("content", doc.get("text", "")))[:300],
                "solr",
            ))
        return out
    except Exception:
        return []


async def search_zincsearch(q: str, index: str, max_results: int) -> list[dict]:
    try:
        resp = await http().post(
            f"{ZINCSEARCH_URL}/api/{index}/_search",
            json={
                "search_type": "querystring",
                "query": {"term": q},
                "max_results": max_results,
            },
            auth=(ZINC_USER, ZINC_PASSWORD),
        )
        resp.raise_for_status()
        out = []
        for hit in resp.json().get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            out.append(normalize_result(
                src.get("title", ""),
                src.get("url", ""),
                src.get("content", str(src)[:200]),
                "zincsearch",
                score=hit.get("_score", 0),
            ))
        return out
    except Exception:
        return []


async def search_quickwit(q: str, index: str, max_results: int) -> list[dict]:
    try:
        resp = await http().get(
            f"{QUICKWIT_URL}/api/v1/{index}/search",
            params={"query": q, "max_hits": max_results},
        )
        resp.raise_for_status()
        out = []
        for hit in resp.json().get("hits", []):
            out.append(normalize_result(
                hit.get("title", ""),
                hit.get("url", ""),
                hit.get("body", hit.get("content", str(hit)[:200])),
                "quickwit",
            ))
        return out
    except Exception:
        return []


async def search_qdrant(q: str, collection: str, vector: list[float], max_results: int) -> list[dict]:
    try:
        resp = await http().post(
            f"{QDRANT_URL}/collections/{collection}/points/search",
            json={
                "vector": vector,
                "limit": max_results,
                "with_payload": True,
            },
        )
        resp.raise_for_status()
        out = []
        for r in resp.json().get("result", []):
            payload = r.get("payload", {})
            out.append(normalize_result(
                payload.get("title", ""),
                payload.get("url", ""),
                payload.get("content", payload.get("text", ""))[:300],
                "qdrant",
                score=r.get("score", 0),
            ))
        return out
    except Exception:
        return []


async def search_weaviate(q: str, class_name: str, max_results: int) -> list[dict]:
    try:
        gql = {
            "query": f"""{{
                Get {{
                    {class_name}(
                        bm25: {{ query: "{q}" }}
                        limit: {max_results}
                    ) {{
                        title
                        url
                        content
                        _additional {{ score }}
                    }}
                }}
            }}"""
        }
        resp = await http().post(f"{WEAVIATE_URL}/v1/graphql", json=gql)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("Get", {}).get(class_name, [])
        out = []
        for r in data:
            out.append(normalize_result(
                r.get("title", ""),
                r.get("url", ""),
                r.get("content", "")[:300],
                "weaviate",
                score=r.get("_additional", {}).get("score", 0),
            ))
        return out
    except Exception:
        return []


async def search_yacy(q: str, max_results: int) -> list[dict]:
    """YaCy — decentralized peer-to-peer web search. Using shared client with custom timeout override."""
    try:
        resp = await http().get(
            f"{YACY_URL}/yacysearch.json",
            params={
                "query": q,
                "maximumRecords": max_results,
                "resource": "global",
                "urlmaskfilter": ".*",
                "prefermaskfilter": "",
                "nav": "none",
            },
            timeout=15.0, # Override the default shared connection timeout per request
        )
        resp.raise_for_status()
        channels = resp.json().get("channels", [])
        items = channels[0].get("items", []) if channels else []
        out = []
        for r in items[:max_results]:
            title = (r.get("title") or "").strip()
            link = (r.get("link") or "").strip()
            desc = (r.get("description") or "").strip()
            if title and link:
                out.append(normalize_result(title, link, desc, "yacy"))
        return out
    except Exception:
        return []


async def search_redisearch(q: str, index: str, max_results: int) -> list[dict]:
    """RediSearch Full-Text adapter via FT.SEARCH"""
    try:
        r = app.state.redis
        if not r:
             return []
        query = q.replace('"', '\\"')
        res = await r.execute_command("FT.SEARCH", index, query, "LIMIT", 0, max_results)
        
        out = []
        if not res or len(res) < 2:
            return out
        for i in range(1, len(res), 2):
            fields = res[i+1]
            data = dict(zip(fields[::2], fields[1::2]))
            out.append(normalize_result(
                data.get("title", ""),
                data.get("url", ""),
                (data.get("content", "") or "")[:300],
                "redisearch",
            ))
        return out
    except Exception:
        return []


async def search_fess(q: str, max_results: int) -> list[dict]:
    """Fess — all-in-one search engine with built-in crawler."""
    try:
        resp = await http().get(
            f"{FESS_URL}/api/v1/search",
            params={"q": q, "num": max_results, "lang": "en"},
        )
        resp.raise_for_status()
        data = resp.json()
        out = []
        for r in data.get("response", {}).get("result", []):
            out.append(normalize_result(
                r.get("title", ""),
                r.get("url_link", r.get("url", "")),
                r.get("content_description", r.get("digest", "")),
                "fess",
            ))
        return out
    except Exception:
        return []


async def search_vespa(q: str, max_results: int) -> list[dict]:
    """Vespa — advanced hybrid search (text + vector + structured)."""
    try:
        resp = await http().get(
            f"{VESPA_URL}/search/",
            params={
                "yql": "select * from document where userQuery()",
                "query": q,
                "hits": max_results,
                "ranking": "default",
            },
        )
        resp.raise_for_status()
        out = []
        root = resp.json().get("root", {})
        for hit in root.get("children", []):
            fields = hit.get("fields", {})
            out.append(normalize_result(
                fields.get("title", ""),
                fields.get("url", ""),
                fields.get("content", fields.get("description", ""))[:300],
                "vespa",
                score=hit.get("relevance", 0),
            ))
        return out
    except Exception:
        return []


# ─────────────────────────────────────────
#  RESULT RANKING — merge + deduplicate
# ─────────────────────────────────────────

def deduplicate(results: list[dict]) -> list[dict]:
    """Remove duplicate URLs, keep highest-scored version. Uses canonical_url to prevent tracking query loops."""
    seen = {}
    for r in results:
        url = canonical_url(r.get("url", "")) # Strips utm_, gclid, etc. for better matching 
        if not url or url not in seen:
            key = url or f"_no_url_{len(seen)}"
            seen[key] = r
        else:
            w_new = ENGINE_WEIGHT.get(r.get("source", ""), 1.0)
            w_old = ENGINE_WEIGHT.get(seen[url].get("source", ""), 1.0)
            if w_new > w_old:
                seen[url] = r
            elif w_new == w_old and r.get("score", 0) > seen[url].get("score", 0):
                seen[url] = r
    return list(seen.values())


def filter_relevant(query: str, results: list[dict]) -> list[dict]:
    """
    Light relevance filter. Only removes results that have zero overlap with the query.
    Uses substring matching on the full term AND on individual sub-parts (to handle
    search engine auto-corrections like "Cyberviewn8" → "Cyberview 8").

    Results from engines that do their own relevance ranking (whoogle, duckduckgo,
    searxng, fess) are always kept since those engines already filtered for relevance.
    """
    import re

    # Engines that already apply their own relevance ranking
    TRUSTED_ENGINES = {"whoogle", "duckduckgo", "searxng"}

    terms = [t.lower() for t in query.split() if len(t) >= 2]
    if not terms:
        return results

    # Also generate sub-parts: "cyberviewn8" → ["cyber", "view", "n8"] via camelCase/digit splits
    sub_parts = set()
    for t in terms:
        # Split on digit/letter boundaries: "cyberviewn8" → ["cyberviewn", "8"]
        parts = re.findall(r'[a-z]+|[0-9]+', t)
        for p in parts:
            if len(p) >= 2:
                sub_parts.add(p)
        # Also add contiguous alpha portion for partial matching
        alpha_only = re.sub(r'[^a-z]', '', t)
        if len(alpha_only) >= 3:
            sub_parts.add(alpha_only)

    kept = []
    for r in results:
        # Trust results from engines that do their own relevance ranking
        source = r.get("source", "")
        if source in TRUSTED_ENGINES:
            kept.append(r)
            continue

        raw = f"{r.get('title', '')} {r.get('url', '')} {r.get('snippet', '')}".lower()
        stripped = re.sub(r'[^a-z0-9]', '', raw)

        # Check full terms
        if any(t in raw or t in stripped for t in terms):
            kept.append(r)
            continue

        # Check sub-parts (handles auto-corrections)
        matching_parts = sum(1 for p in sub_parts if p in raw or p in stripped)
        if sub_parts and matching_parts >= max(1, len(sub_parts) // 2):
            kept.append(r)
            continue

    return kept if kept else results


ENGINE_WEIGHT = {
    "whoogle": 3.0,
    "duckduckgo": 2.0,
    "searxng": 1.5,
    "yacy": 0.5,
    "meilisearch": 1.0,
    "typesense": 1.0,
    "opensearch": 1.0,
    "solr": 1.0,
    "quickwit": 1.0,
    "qdrant": 1.0,
    "weaviate": 1.0,
    "redisearch": 1.0,
    "fess": 0.5,
    "vespa": 1.0,
}

def rank_results(query: str, results: list[dict], rank_mode: str = "fast") -> list[dict]:
    url_counts = {}
    for r in results:
        url = canonical_url(r.get("url", ""))
        if url:
            url_counts[url] = url_counts.get(url, 0) + 1

    deduped = deduplicate(results)
    for r in deduped:
        url = canonical_url(r.get("url", ""))
        engine_agreement = url_counts.get(url, 1)
        engine_score = float(r.get("score", 0))
        if engine_score == 0:
            engine_score = 1.0
            
        snippet_len = len(r.get("snippet", ""))
        
        # Enhanced weighting for fast/hybrid mode
        w = ENGINE_WEIGHT.get(r.get("source", ""), 1.0)
        
        # New Scoring Formula: Multiplicative scoring logic
        base_score = engine_score * w
        agreement_multiplier = 1.0 + ((engine_agreement - 1) * 0.5)
        
        r["_rank_score"] = (base_score * agreement_multiplier) + (snippet_len * 0.001)
        
        # Hard Priority Tiers per User Request
        source = r.get("source", "")
        if source == "whoogle":
            r["_rank_score"] += 1000000
        elif source == "duckduckgo":
            r["_rank_score"] += 500000
        elif source == "searxng" and r.get("category", "") == "images":
            r["_rank_score"] += 250000
        elif source == "searxng":
            r["_rank_score"] += 100000
            
        r["_seen_in_engines"] = engine_agreement

    deduped.sort(key=lambda x: x.get("_rank_score", 0), reverse=True)

    # Cross-encoder Reranking
    if rank_mode == "rerank" and getattr(app.state, "reranker", None):
        top_k = deduped[:60]
        # Build query-document pairs for the cross-encoder
        pairs = [[query, r.get("snippet", r.get("title", ""))] for r in top_k]
        if pairs:
            try:
                scores = app.state.reranker.predict(pairs)
                for i, r in enumerate(top_k):
                    r["_rank_score"] = float(scores[i])
                top_k.sort(key=lambda x: x.get("_rank_score", 0), reverse=True)
                deduped = top_k + deduped[60:]
            except Exception as e:
                print(f"Reranking failed: {e}")

    # Re-apply absolute engine priority over any AI/hybrid scores
    for r in deduped:
        source = r.get("source", "")
        # Remove any existing offsets before recalculating to ensure clean sorting
        r["_rank_score"] = r.get("_rank_score", 0) % 100000
        
        if source == "whoogle":
            r["_rank_score"] += 1000000
        elif source == "duckduckgo":
            r["_rank_score"] += 500000
        elif source == "searxng" and r.get("category", "") == "images":
            r["_rank_score"] += 250000
        elif source == "searxng":
            r["_rank_score"] += 100000
            
    deduped.sort(key=lambda x: x.get("_rank_score", 0), reverse=True)

    return deduped


# ─────────────────────────────────────────
#  AVAILABLE ENGINE REGISTRY
# ─────────────────────────────────────────
ALL_WEB_ENGINES = {"duckduckgo", "searxng", "whoogle", "yacy", "fess"}
ALL_INDEX_ENGINES = {
    "meilisearch", "typesense", "opensearch", "manticore",
    "solr", "zincsearch", "quickwit", "redisearch", "vespa",
}
ALL_VECTOR_ENGINES = {"qdrant", "weaviate"}
ALL_ENGINES = ALL_WEB_ENGINES | ALL_INDEX_ENGINES | ALL_VECTOR_ENGINES


# ─────────────────────────────────────────
#  API ENDPOINTS
# ─────────────────────────────────────────

@app.get("/health")
async def health():
    return {"ok": True, "engines": sorted(ALL_ENGINES), "total": len(ALL_ENGINES)}


@app.get("/engines")
async def list_engines():
    return {
        "web_search": sorted(ALL_WEB_ENGINES),
        "full_text_index": sorted(ALL_INDEX_ENGINES),
        "vector_semantic": sorted(ALL_VECTOR_ENGINES),
        "all": sorted(ALL_ENGINES),
    }


@app.get("/search")
async def unified_search(
    q: str = Query(..., min_length=1, description="Search query"),
    engines: str = Query(
        "web",
        description="Comma-separated engine names, or 'all', 'web', 'index', 'vector'",
    ),
    max_results: int = Query(50, ge=1, le=500, description="Max results per engine"),
    categories: str = Query("general", description="SearXNG categories"),
    index: str = Query("documents", description="Index/collection name for index engines"),
    class_name: str = Query("Document", description="Weaviate class name"),
    dedupe: bool = Query(True, description="Deduplicate results by URL"),
    rank: bool = Query(True, description="Apply cross-engine ranking"),
    rank_mode: str = Query("fast", description="Ranking strategy: fast|hybrid|rerank"),
    safe: int = Query(1, description="0=strict, 1=moderate, 2=off"),
    site: Optional[str] = Query(None, description="Filter by site/domain"),
    filetype: Optional[str] = Query(None, description="Filter by file extension"),
    time_range: str = Query("", description="Time range (day, month, year)"),
    region: str = Query("", description="Region format e.g., US, UK, DE"),
    strategy: str = Query("exhaustive", description="fast, balanced, exhaustive"),
    use_cache: bool = Query(True, description="Use result cache"),
    x_api_key: Optional[str] = Header(None, description="API Key header for auth"),
):
    require_key(x_api_key)
    
    if site:
        q += f" site:{site}"
    if filetype:
        q += f" ext:{filetype}"
        
    engine_set = set()
    for e in engines.split(","):
        e = e.strip().lower()
        if e == "all":
            engine_set = ALL_ENGINES.copy()
            break
        elif e == "web":
            engine_set |= ALL_WEB_ENGINES
        elif e == "index":
            engine_set |= ALL_INDEX_ENGINES
        elif e == "vector":
            engine_set |= ALL_VECTOR_ENGINES
        elif e in ALL_ENGINES:
            engine_set.add(e)

    if strategy == "fast":
        if len(engine_set & ALL_WEB_ENGINES) > 1:
            engine_set = (engine_set - ALL_WEB_ENGINES) | {"searxng"}
    elif strategy == "balanced":
        if len(engine_set & ALL_WEB_ENGINES) > 3:
            engine_set = (engine_set - ALL_WEB_ENGINES) | {"searxng", "duckduckgo", "whoogle"}

    if not engine_set:
        raise HTTPException(400, f"No valid engines. Available: {sorted(ALL_ENGINES)}")

    cache_key = f"{q}|{','.join(sorted(engine_set))}|{max_results}|{categories}|{index}|{rank_mode}|{safe}|{time_range}"
    if use_cache:
        cached = await cache_get(cache_key)
        if cached is not None:
            cached["_cached"] = True
            return cached

    # Embed query if using Qdrant vector search
    vector = None
    if "qdrant" in engine_set:
        vector = embed(q)

    ddg_safe = "off" if safe == 0 else "moderate" if safe == 1 else "on"
    searxng_safe = 0 if safe == 0 else 1 if safe == 1 else 2

    # Fire all engines concurrently via run_engine semaphore wrappers
    tasks = {}
    if "duckduckgo" in engine_set:
        tasks["duckduckgo"] = run_engine("duckduckgo", search_duckduckgo(q, max_results, categories=categories, safe=ddg_safe, region=region))
    if "searxng" in engine_set:
        tasks["searxng"] = run_engine("searxng", search_searxng(q, max_results, categories, safe=searxng_safe, time_range=time_range, region=region))
    if "whoogle" in engine_set:
        tasks["whoogle"] = run_engine("whoogle", search_whoogle(q, max_results))
    if "yacy" in engine_set:
        tasks["yacy"] = run_engine("yacy", search_yacy(q, max_results))
    if "meilisearch" in engine_set:
        tasks["meilisearch"] = run_engine("meilisearch", search_meilisearch(q, index, max_results))
    if "typesense" in engine_set:
        tasks["typesense"] = run_engine("typesense", search_typesense(q, index, max_results))
    if "opensearch" in engine_set:
        tasks["opensearch"] = run_engine("opensearch", search_opensearch(q, index, max_results))
    if "manticore" in engine_set:
        tasks["manticore"] = run_engine("manticore", search_manticore(q, index, max_results))
    if "solr" in engine_set:
        tasks["solr"] = run_engine("solr", search_solr(q, index, max_results))
    if "zincsearch" in engine_set:
        tasks["zincsearch"] = run_engine("zincsearch", search_zincsearch(q, index, max_results))
    if "quickwit" in engine_set:
        tasks["quickwit"] = run_engine("quickwit", search_quickwit(q, index, max_results))
    if "redisearch" in engine_set:
        tasks["redisearch"] = run_engine("redisearch", search_redisearch(q, index, max_results))
    if "weaviate" in engine_set:
        tasks["weaviate"] = run_engine("weaviate", search_weaviate(q, class_name, max_results))
    if "fess" in engine_set:
        tasks["fess"] = run_engine("fess", search_fess(q, max_results))
    if "vespa" in engine_set:
        tasks["vespa"] = run_engine("vespa", search_vespa(q, max_results))

    if "qdrant" in engine_set and vector:
        tasks["qdrant"] = run_engine("qdrant", search_qdrant(q, index, vector, max_results))

    start = time.time()
    gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)
    elapsed = round(time.time() - start, 3)

    SEARCH_REQUESTS.inc()
    SEARCH_LATENCY.observe(elapsed)

    all_results = []
    engine_stats = {}
    for name, result in zip(tasks.keys(), gathered):
        if isinstance(result, Exception):
            ENGINE_ERRORS.labels(engine=name).inc()
            engine_stats[name] = {"status": "error", "error": str(result), "count": 0}
        else:
            engine_stats[name] = {"status": "ok", "count": len(result)}
            all_results.extend(result)

    all_results = filter_relevant(q, all_results)
    if rank:
        all_results = rank_results(q, all_results, rank_mode)
    elif dedupe:
        all_results = deduplicate(all_results)

    response = {
        "query": q,
        "total_results": len(all_results),
        "elapsed_seconds": elapsed,
        "engines_queried": sorted(engine_set),
        "engine_stats": engine_stats,
        "results": all_results,
        "_cached": False,
    }

    if use_cache:
        await cache_set(cache_key, response)

    return response

class HybridSearchRequest(BaseModel):
    query: str
    engines: str = "web,index,vector"
    max_results: int = 50
    categories: str = "general"
    index: str = "documents"
    class_name: str = "Document"
    rank_mode: str = "rerank"

@app.post("/search/hybrid")
async def explicit_hybrid_search(
    req: HybridSearchRequest,
    x_api_key: Optional[str] = Header(None),
):
    """Explicit endpoint for hybrid search/reranking."""
    return await unified_search(
        q=req.query,
        engines=req.engines,
        max_results=req.max_results,
        categories=req.categories,
        index=req.index,
        class_name=req.class_name,
        rank_mode=req.rank_mode,
        x_api_key=x_api_key,
    )

@app.get("/search/web")
async def web_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(100, ge=1, le=500),
    categories: str = Query("general"),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(
        q=q, engines="web", max_results=max_results, categories=categories, x_api_key=x_api_key
    )

@app.get("/search/images")
async def image_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(50, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(q=q, engines="searxng", max_results=max_results, categories="images", x_api_key=x_api_key)

@app.get("/search/news")
async def news_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(50, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(q=q, engines="searxng", max_results=max_results, categories="news", x_api_key=x_api_key)

@app.get("/search/videos")
async def video_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(50, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(q=q, engines="searxng", max_results=max_results, categories="videos", x_api_key=x_api_key)

@app.get("/search/science")
async def science_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(20, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(q=q, engines="searxng", max_results=max_results, categories="science", x_api_key=x_api_key)

@app.get("/search/code")
async def code_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(20, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(q=q, engines="searxng", max_results=max_results, categories="it", x_api_key=x_api_key)

@app.get("/search/social")
async def social_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(20, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(q=q, engines="searxng", max_results=max_results, categories="social media", x_api_key=x_api_key)

@app.get("/search/files")
async def files_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(20, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(q=q, engines="searxng", max_results=max_results, categories="files", x_api_key=x_api_key)

@app.get("/search/pdfs")
async def pdf_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(20, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    # SearXNG handles "ext:pdf" logic when passing categories="files" or "general". 
    # But usually, it's safer to just inject it into the query if the user wants PDFs.
    # SearXNG supports categories="files", but to guarantee PDFs we modify the query slightly, or just use categories="files".
    # Here, we append "ext:pdf" to the query if not present, and use 'general' or 'files'
    pdf_q = q if "ext:pdf" in q.lower() or "filetype:pdf" in q.lower() else f"{q} ext:pdf"
    return await unified_search(q=pdf_q, engines="searxng", max_results=max_results, categories="general", x_api_key=x_api_key)

@app.get("/search/music")
async def music_search(
    q: str = Query(..., min_length=1),
    max_results: int = Query(20, ge=1, le=500),
    x_api_key: Optional[str] = Header(None),
):
    return await unified_search(q=q, engines="searxng", max_results=max_results, categories="music", x_api_key=x_api_key)


# ─────────────────────────────────────────
#  INDEX MANAGEMENT ENDPOINTS
# ─────────────────────────────────────────

@app.post("/index/meilisearch/{index_name}")
async def index_meilisearch(index_name: str, documents: list[UnifiedDocument], x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    payload = [d.model_dump() for d in documents]
    resp = await http().post(
        f"{MEILISEARCH_URL}/indexes/{index_name}/documents",
        json=payload,
        headers={"Authorization": f"Bearer {MEILI_MASTER_KEY}"},
    )
    return resp.json()

@app.post("/index/opensearch/{index_name}")
async def index_opensearch(index_name: str, documents: list[UnifiedDocument], x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    lines = []
    for doc in documents:
        lines.append(json.dumps({"index": {"_index": index_name}}))
        lines.append(json.dumps(doc.model_dump()))
    body = "\n".join(lines) + "\n"
    resp = await http().post(
        f"{OPENSEARCH_URL}/_bulk",
        content=body,
        headers={"Content-Type": "application/x-ndjson"},
    )
    return resp.json()

@app.post("/index/typesense/{collection}")
async def index_typesense(collection: str, documents: list[UnifiedDocument], x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    results = []
    for doc in documents:
        resp = await http().post(
            f"{TYPESENSE_URL}/collections/{collection}/documents",
            json=doc.model_dump(),
            headers={"X-TYPESENSE-API-KEY": TYPESENSE_API_KEY},
        )
        results.append(resp.json())
    return {"indexed": len(results), "results": results}

@app.post("/index/zincsearch/{index_name}")
async def index_zincsearch(index_name: str, documents: list[UnifiedDocument], x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    payload = [d.model_dump() for d in documents]
    resp = await http().post(
        f"{ZINCSEARCH_URL}/api/{index_name}/_multi",
        json={"records": payload},
        auth=(ZINC_USER, ZINC_PASSWORD),
    )
    return resp.json()

@app.post("/index/qdrant/{collection}")
async def index_qdrant(collection: str, points: list[dict], x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    resp = await http().put(
        f"{QDRANT_URL}/collections/{collection}/points",
        json={"points": points},
    )
    return resp.json()


# ─────────────────────────────────────────
#  ENGINE STATUS
# ─────────────────────────────────────────

@app.get("/status")
async def engine_status():
    checks = {
        "searxng": f"{SEARXNG_URL}/healthz",
        "whoogle": f"{WHOOGLE_URL}/",
        "meilisearch": f"{MEILISEARCH_URL}/health",
        "typesense": f"{TYPESENSE_URL}/health",
        "quickwit": f"{QUICKWIT_URL}/health/readyz",
        "opensearch": f"{OPENSEARCH_URL}/_cluster/health",
        "solr": f"{SOLR_URL}/solr/admin/info/system?wt=json",
        "zincsearch": f"{ZINCSEARCH_URL}/healthz",
        "qdrant": f"{QDRANT_URL}/healthz",
        "weaviate": f"{WEAVIATE_URL}/v1/.well-known/ready",
        "manticore": f"{MANTICORE_URL}/sql?mode=raw",
        "yacy": f"{YACY_URL}/Status.html",
        "fess": f"{FESS_URL}/api/v1/health",
        "vespa": f"{VESPA_URL}/status.html",
        "tika": f"{TIKA_URL}/tika",
    }

    async def check(name, url):
        try:
            r = await http().get(url, timeout=3.0)
            return name, {"status": "up", "code": r.status_code}
        except Exception as e:
            return name, {"status": "down", "error": str(e)}

    results = await asyncio.gather(*[check(n, u) for n, u in checks.items()])
    status = {name: info for name, info in results}

    status["duckduckgo"] = {"status": "up", "note": "external API via langchain"}
    try:
        r = await http().get(f"http://{REDISEARCH_HOST}:8001/", timeout=3.0)
        status["redisearch"] = {"status": "up", "code": r.status_code}
    except Exception:
        status["redisearch"] = {"status": "down"}

    up = sum(1 for v in status.values() if v.get("status") == "up")
    return {"engines": status, "up": up, "total": len(status)}


# ─────────────────────────────────────────
#  CACHE MANAGEMENT
# ─────────────────────────────────────────

@app.delete("/cache")
async def clear_cache(x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    cache.cache.clear()
    if app.state.redis:
         await app.state.redis.flushdb()
    return {"ok": True, "message": "Cache cleared"}


@app.get("/cache/stats")
async def cache_stats():
    redis_keys = 0
    if app.state.redis:
        redis_keys = await app.state.redis.dbsize()
    return {"l1_entries": len(cache.cache), "l1_max": cache.maxsize, "l1_ttl": cache.ttl, "l2_redis_entries": redis_keys}


# ─────────────────────────────────────────
#  DOCUMENT EXTRACTION (Apache Tika)
# ─────────────────────────────────────────

@app.post("/extract")
async def extract_text(
    url: str = Query(..., description="URL of document to extract text from"),
    x_api_key: Optional[str] = Header(None),
):
    """Extract text from a document (PDF, DOCX, PPT, etc.) using Apache Tika."""
    require_key(x_api_key)
    try:
        doc_resp = await http().get(url, timeout=30.0)
        doc_resp.raise_for_status()

        tika_resp = await http().put(
            f"{TIKA_URL}/tika",
            content=doc_resp.content,
            headers={
                "Content-Type": doc_resp.headers.get("content-type", "application/octet-stream"),
                "Accept": "text/plain",
            },
            timeout=60.0,
        )
        tika_resp.raise_for_status()

        return {
            "url": url,
            "text": tika_resp.text,
            "content_type": doc_resp.headers.get("content-type", ""),
            "length": len(tika_resp.text),
        }
    except Exception as e:
        raise HTTPException(500, f"Extraction failed: {str(e)}")


@app.post("/extract/metadata")
async def extract_metadata(
    url: str = Query(..., description="URL of document to extract metadata from"),
    x_api_key: Optional[str] = Header(None),
):
    """Extract metadata from a document using Apache Tika."""
    require_key(x_api_key)
    try:
        doc_resp = await http().get(url, timeout=30.0)
        doc_resp.raise_for_status()

        tika_resp = await http().put(
            f"{TIKA_URL}/meta",
            content=doc_resp.content,
            headers={
                "Content-Type": doc_resp.headers.get("content-type", "application/octet-stream"),
                "Accept": "application/json",
            },
            timeout=60.0,
        )
        tika_resp.raise_for_status()

        return {"url": url, "metadata": tika_resp.json()}
    except Exception as e:
        raise HTTPException(500, f"Metadata extraction failed: {str(e)}")


@app.get("/extract/supported")
async def tika_supported_types():
    """List document types supported by Apache Tika."""
    try:
        resp = await http().get(f"{TIKA_URL}/mime-types", headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────
#  VESPA APPLICATION DEPLOYMENT
# ─────────────────────────────────────────

@app.post("/vespa/deploy")
async def vespa_deploy_app(x_api_key: Optional[str] = Header(None)):
    """Deploy the Vespa application package (schemas + services config).
    Call this once after Vespa container starts to enable search."""
    require_key(x_api_key)
    import io
    import zipfile

    vespa_app_files = {
        "services.xml": """<?xml version="1.0" encoding="utf-8" ?>
<services version="1.0">
  <container id="default" version="1.0">
    <search /><document-api />
    <nodes><node hostalias="node1" /></nodes>
  </container>
  <content id="content" version="1.0">
    <redundancy>1</redundancy>
    <documents><document type="document" mode="index" /></documents>
    <nodes><node hostalias="node1" distribution-key="0" /></nodes>
  </content>
</services>""",
        "hosts.xml": """<?xml version="1.0" encoding="utf-8" ?>
<hosts><host name="localhost"><alias>node1</alias></host></hosts>""",
        "schemas/document.sd": """schema document {
  document document {
    field title type string { indexing: index | summary  index: enable-bm25 }
    field url type string { indexing: summary | attribute }
    field content type string { indexing: index | summary  index: enable-bm25 }
    field description type string { indexing: index | summary  index: enable-bm25 }
    field timestamp type long { indexing: summary | attribute }
  }
  fieldset default { fields: title, content, description }
  rank-profile default inherits default {
    first-phase { expression: bm25(title)*2 + bm25(content) + bm25(description) }
  }
}""",
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path, content in vespa_app_files.items():
            zf.writestr(f"application/{path}", content)
    buf.seek(0)

    try:
        resp = await http().post(
            f"{VESPA_CONFIG_URL}/application/v2/tenant/default/prepareandactivate",
            content=buf.read(),
            headers={"Content-Type": "application/zip"},
            timeout=30.0,
        )
        resp.raise_for_status()
        return {"ok": True, "response": resp.json()}
    except Exception as e:
        raise HTTPException(500, f"Vespa deploy failed: {str(e)}")


@app.post("/vespa/feed")
async def vespa_feed_document(
    doc_id: str = Query(..., description="Document ID"),
    document: dict = {},
    x_api_key: Optional[str] = Header(None),
):
    """Feed a single document into Vespa."""
    require_key(x_api_key)
    try:
        resp = await http().post(
            f"{VESPA_URL}/document/v1/default/document/docid/{doc_id}",
            json={"fields": document},
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────
#  CRAWLER MANAGEMENT
# ─────────────────────────────────────────

@app.post("/crawl/job")
async def create_crawl_job(profile: CrawlProfile, x_api_key: Optional[str] = Header(None)):
    """Create a new crawl job mapped to a defined profile and store state in Redis."""
    require_key(x_api_key)
    job_id = f"job_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    
    job_data = profile.model_dump()
    job_data["status"] = "starting"
    job_data["job_id"] = job_id
    job_data["created_at"] = datetime.datetime.utcnow().isoformat()
    
    if app.state.redis:
        try:
            await app.state.redis.hset("crawler_jobs", job_id, json.dumps(job_data))
        except Exception as e:
            print(f"Warning: Failed to save job to redis: {e}")

    # Fire off to actual crawler
    if profile.crawler_engine == "nutch":
        try:
            await http().post(f"{NUTCH_URL}/crawl", json={"seeds": profile.seeds, "depth": profile.depth}, timeout=10.0)
            job_data["status"] = "running_nutch"
        except Exception as e:
            job_data["status"] = "error"
            job_data["error"] = str(e)
            
    elif profile.crawler_engine == "heritrix":
        try:
            await http().post(
                f"{HERITRIX_URL}/engine",
                data=f"action=create&createpath={profile.job_name}",
                auth=(HERITRIX_USER, HERITRIX_PASSWORD),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10.0,
            )
            job_data["status"] = "running_heritrix"
        except Exception as e:
            job_data["status"] = "error"
            job_data["error"] = str(e)
            
    if app.state.redis:
        try:
            await app.state.redis.hset("crawler_jobs", job_id, json.dumps(job_data))
        except Exception:
            pass

    return {"job_id": job_id, "status": job_data["status"]}

@app.get("/crawl/jobs")
async def list_crawl_jobs(x_api_key: Optional[str] = Header(None)):
    """List all crawler jobs from Redis."""
    require_key(x_api_key)
    if not app.state.redis:
        raise HTTPException(500, "Redis is not configured")
    try:
        jobs_raw = await app.state.redis.hgetall("crawler_jobs")
        jobs = [json.loads(v) for v in jobs_raw.values()]
        return {"jobs": jobs}
    except Exception as e:
        raise HTTPException(500, f"Failed to list jobs: {str(e)}")


@app.get("/crawl/fess/status")
async def fess_crawler_status(x_api_key: Optional[str] = Header(None)):
    """Get Fess crawler status and running jobs."""
    require_key(x_api_key)
    try:
        resp = await http().get(
            f"{FESS_URL}/api/v1/admin/scheduler/jobs",
            timeout=10.0,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(500, f"Fess crawler status failed: {str(e)}")


@app.post("/crawl/fess/start")
async def fess_start_crawl(
    job_name: str = Query("default_crawler", description="Fess crawl job name"),
    x_api_key: Optional[str] = Header(None),
):
    """Start a Fess crawl job."""
    require_key(x_api_key)
    try:
        resp = await http().post(
            f"{FESS_URL}/api/v1/admin/scheduler/{job_name}/start",
            timeout=10.0,
        )
        resp.raise_for_status()
        return {"ok": True, "job": job_name, "response": resp.json()}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/crawl/nutch/crawl")
async def nutch_start_crawl(
    seeds: list[str],
    depth: int = Query(2, ge=1, le=10, description="Crawl depth"),
    x_api_key: Optional[str] = Header(None),
):
    """Start a Nutch crawl job. Results are indexed into Solr.
    Requires: docker compose --profile crawlers up"""
    require_key(x_api_key)
    try:
        resp = await http().post(
            f"{NUTCH_URL}/crawl",
            json={"seeds": seeds, "depth": depth},
            timeout=10.0,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(500, f"Nutch crawl failed: {str(e)}")


@app.get("/crawl/nutch/jobs")
async def nutch_list_jobs(x_api_key: Optional[str] = Header(None)):
    """List Nutch crawl jobs and statuses."""
    require_key(x_api_key)
    try:
        resp = await http().get(f"{NUTCH_URL}/jobs", timeout=5.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/crawl/heritrix/status")
async def heritrix_status(x_api_key: Optional[str] = Header(None)):
    """Get Heritrix engine status.
    Requires: docker compose --profile crawlers up"""
    require_key(x_api_key)
    try:
        resp = await http().get(
            f"{HERITRIX_URL}/engine",
            auth=(HERITRIX_USER, HERITRIX_PASSWORD),
            headers={"Accept": "application/xml"},
            timeout=5.0,
        )
        resp.raise_for_status()
        return {"status": "up", "code": resp.status_code}
    except Exception as e:
        raise HTTPException(500, f"Heritrix status failed: {str(e)}")


@app.post("/crawl/heritrix/job")
async def heritrix_create_job(
    job_name: str = Query(..., description="Name for the new crawl job"),
    x_api_key: Optional[str] = Header(None),
):
    """Create a new Heritrix crawl job."""
    require_key(x_api_key)
    try:
        resp = await http().post(
            f"{HERITRIX_URL}/engine",
            data=f"action=create&createpath={job_name}",
            auth=(HERITRIX_USER, HERITRIX_PASSWORD),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10.0,
        )
        return {"ok": True, "job": job_name, "code": resp.status_code}
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────
#  INGEST PIPELINE
# ─────────────────────────────────────────

def split_text_into_chunks(text: str, chunk_size: int = 3000) -> list[str]:
    # A simple char-level chunker (~600-800 tokens)
    chunks = []
    for i in range(0, len(text), chunk_size):
        chunks.append(text[i:i+chunk_size])
    return chunks

async def process_and_ingest_url(url: str, source: str = "manual") -> UnifiedDocument:
    # 1. Fetch & SSRF & Size limit
    if not is_safe_url(url):
        raise ValueError(f"URL failed SSRF safety check: {url}")
        
    doc_content = b""
    content_type = ""
    async with app.state.http.stream("GET", url, timeout=30.0) as resp:
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "application/octet-stream")
        async for chunk in resp.aiter_bytes():
            doc_content += chunk
            if len(doc_content) > 20 * 1024 * 1024:
                raise ValueError("File exceeds 20MB limit")

    # 2. Extract
    tika_resp = await http().put(
        f"{TIKA_URL}/tika",
        content=doc_content,
        headers={
            "Content-Type": content_type,
            "Accept": "text/plain",
        },
        timeout=60.0,
    )
    text = (tika_resp.text or "").strip()

    # Metadata
    meta_resp = await http().put(
        f"{TIKA_URL}/meta",
        content=doc_content,
        headers={"Accept": "application/json"},
        timeout=60.0,
    )
    metadata = meta_resp.json() if meta_resp.status_code == 200 else {}

    title = ""
    if isinstance(metadata, dict):
        title = metadata.get("dc:title", metadata.get("title", ""))
    if isinstance(title, list) and title:
        title = title[0]
    if not title:
        title = url.rsplit("/", 1)[-1]

    doc_id = hashlib.md5(canonical_url(url).encode()).hexdigest()

    # 3. Chunk
    raw_chunks = split_text_into_chunks(text, 3000)
    content_chunks = []
    for i, c in enumerate(raw_chunks):
        chunk_hash = hashlib.md5(f"{doc_id}_{i}".encode()).hexdigest()
        chunk_uuid = str(uuid.UUID(chunk_hash))
        content_chunks.append(
            DocumentChunk(chunk_id=chunk_uuid, text=c, index=i)
        )

    filetype = ""
    for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".csv", ".txt"]:
        if ext in url.lower():
            filetype = ext.replace(".", "").upper()
            break

    doc = UnifiedDocument(
        doc_id=doc_id,
        url=url,
        canonical_url=canonical_url(url),
        title=title,
        content=text[:50000],
        content_chunks=content_chunks,
        content_type=content_type,
        filetype=filetype,
        meta=metadata if isinstance(metadata, dict) else {},
        source=source,
    )

    # 4. Index doc to OpenSearch (Upsert/Replace)
    await http().put(
        f"{OPENSEARCH_URL}/documents/_doc/{doc_id}",
        json=doc.model_dump(),
    )

    # 5. Index chunks to Qdrant
    if app.state.embedder and content_chunks:
        points = []
        for c in content_chunks:
            vec = embed(c.text)
            if vec:
                points.append({
                    "id": c.chunk_id,
                    "payload": {
                        "doc_id": doc_id,
                        "url": url,
                        "title": title,
                        "text": c.text,
                        "chunk_index": c.index,
                    },
                    "vector": vec,
                })
        if points:
            # Upsert into qdrant
            await http().put(
                f"{QDRANT_URL}/collections/documents/points",
                json={"points": points},
            )

    return doc


@app.post("/ingest/url")
async def ingest_url(url: str = Query(..., description="URL to ingest"), x_api_key: Optional[str] = Header(None)):
    """Extract and index a single URL."""
    require_key(x_api_key)
    try:
        doc = await process_and_ingest_url(url)
        return {"status": "ok", "doc_id": doc.doc_id, "chunks": len(doc.content_chunks)}
    except Exception as e:
        raise HTTPException(500, f"Ingest failed: {str(e)}")

@app.post("/ingest/batch")
async def ingest_batch(urls: list[str], x_api_key: Optional[str] = Header(None)):
    """Batch ingest multiple URLs."""
    require_key(x_api_key)
    results = []
    for u in urls:
        try:
            doc = await process_and_ingest_url(u)
            results.append({"url": u, "status": "ok", "doc_id": doc.doc_id})
        except Exception as e:
            results.append({"url": u, "status": "error", "error": str(e)})
    return {"total": len(urls), "results": results}

@app.post("/ingest/crawl-output")
async def ingest_crawl_output(job_data: dict, source: str = Query("crawler"), x_api_key: Optional[str] = Header(None)):
    """Accept incoming webhook items from Nutch/Heritrix/etc."""
    require_key(x_api_key)
    # E.g. job_data = {"items": ["http://...", "http://..."]}
    urls = job_data.get("items", [])
    results = []
    for u in urls:
        try:
            doc = await process_and_ingest_url(u, source=source)
            results.append({"url": u, "status": "ok", "doc_id": doc.doc_id})
        except Exception as e:
            results.append({"url": u, "status": "error", "error": str(e)})
    return {"total": len(urls), "results": results}

@app.get("/docs/{doc_id}")
async def get_document(doc_id: str, x_api_key: Optional[str] = Header(None)):
    """Retrieve an ingested document from OpenSearch."""
    require_key(x_api_key)
    try:
        resp = await http().get(f"{OPENSEARCH_URL}/documents/_doc/{doc_id}")
        resp.raise_for_status()
        data = resp.json()
        return data.get("_source", {})
    except Exception as e:
        raise HTTPException(404, f"Document not found or error: {str(e)}")




# ─────────────────────────────────────────
#  CRAWL STATUS (all crawlers)
# ─────────────────────────────────────────

@app.get("/crawl/status")
async def all_crawler_status(x_api_key: Optional[str] = Header(None)):
    """Get status of all available crawler services."""
    require_key(x_api_key)
    status = {}

    async def _check(name, url, **kwargs):
        try:
            r = await http().get(url, timeout=5.0, **kwargs)
            return name, {"status": "up", "code": r.status_code}
        except Exception:
            return name, {"status": "down"}

    checks = await asyncio.gather(
        _check("fess", f"{FESS_URL}/api/v1/health"),
        _check("nutch", f"{NUTCH_URL}/health"),
        _check("heritrix", f"{HERITRIX_URL}/engine",
               auth=(HERITRIX_USER, HERITRIX_PASSWORD)),
        _check("tika", f"{TIKA_URL}/tika"),
        return_exceptions=True,
    )

    for result in checks:
        if isinstance(result, tuple):
            status[result[0]] = result[1]

    return {"crawlers": status}
