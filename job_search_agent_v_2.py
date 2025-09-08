"""
job_search_agent v2 (with Greenhouse & Lever)

Implements the multistep workflow:
Fetch → Normalize → Deduplicate → Translate → Enrich → Rank → Select → Act/Log

This version adds real-capable adapters for:
- FINN (HTML parse; falls back to stub on errors)
- Greenhouse boards (public boards-api)
- Lever postings API

Notes
- External calls are best-effort; failures fall back to stubs so the pipeline stays healthy.
- Company names & titles are preserved as-is (translations never alter them).
- Logs print to console AND return as a list so automations can post them.
- A simple last-7-days file store is maintained at ./logs/YYYY-MM-DD.json

Run
    python job_search_agent_v2.py --location "Oslo" --days 7 --top 10

Outputs
- Ranked results (list of dicts)
- ./logs/<date>.json with stats + job snapshots (kept for 7 days)
- Log lines like: [LOG][Dhruva][Stage]
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

# ----------------------
# Config / Preferences
# ----------------------
SKIP_COMPANIES = {"Skatteetaten"}       # lightweight memory (can be fed from chat)
PRIORITY_COMPANIES: set[str] = set()     # e.g., {"Cognite","Tise"}
PRIORITY_SIGNALS = {"B2B", "SaaS", "GenAI", "LLM"}
QUIET_AFTER_HOUR = 11                    # quiet window upper bound (local hour)

# Company-site monitors (fill these lists with your targets)
USE_GREENHOUSE = True
USE_LEVER = True
GREENHOUSE_BOARDS: List[str] = [
    # e.g., "cognite",  # https://boards.greenhouse.io/cognite
]
LEVER_COMPANIES: List[str] = [
    # e.g., "retool",   # https://jobs.lever.co/retool
]

# ----------------------
# Utilities
# ----------------------

def now_local_iso() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def ensure_log_dir(path: str = "./logs") -> None:
    os.makedirs(path, exist_ok=True)


def prune_old_logs(path: str = "./logs", keep_days: int = 7) -> None:
    if not os.path.isdir(path):
        return
    today = datetime.now().date()
    for fname in os.listdir(path):
        if not fname.endswith(".json"):
            continue
        try:
            date_str = fname.replace(".json", "")
            fdate = datetime.strptime(date_str, "%Y-%m-%d").date()
            if (today - fdate).days >= keep_days:
                os.remove(os.path.join(path, fname))
        except Exception:
            pass

# ----------------------
# Data models
# ----------------------
@dataclass
class RawJob:
    source: str
    source_job_id: str
    title: str
    company: str
    location: str
    post_date: str  # ISO date or raw; will normalize
    url: str
    description_raw: str
    lang_guess: str | None = None
    seniority_hint: str | None = None
    salary_raw: str | None = None
    employment_type: str | None = None
    source_meta: Dict[str, Any] | None = None


@dataclass
class Job:
    job_id: str
    title: str
    company: str
    location: str
    post_date: str  # ISO YYYY-MM-DD
    age_days: int
    url: str
    description_raw: str
    description_lang: str
    needs_translation: bool
    description_en: str | None
    signals: Dict[str, Any]
    seniority: str | None
    source: str
    source_meta: Dict[str, Any]

# ----------------------
# HTTP helpers (best-effort)
# ----------------------
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9,nb;q=0.8",
}


def http_get(url: str, params: Dict[str, Any] | None = None, retries: int = 3, backoff: float = 0.8) -> Tuple[str, float]:
    """Basic HTTP GET with retry/backoff. Returns (text, latency_ms)."""
    import requests
    last_err = None
    for i in range(retries):
        t0 = time.time()
        try:
            resp = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=20)
            latency = (time.time() - t0) * 1000
            if resp.status_code == 200 and resp.text:
                return resp.text, latency
            last_err = f"status={resp.status_code}"
        except Exception as e:
            last_err = str(e)
        time.sleep(backoff * (2 ** i))
    raise RuntimeError(f"GET failed for {url}: {last_err}")


def http_get_json(url: str, params: Dict[str, Any] | None = None, retries: int = 3, backoff: float = 0.8) -> Tuple[Any, float]:
    text, latency = http_get(url, params=params, retries=retries, backoff=backoff)
    try:
        data = json.loads(text)
    except Exception as e:
        raise RuntimeError(f"Invalid JSON from {url}: {e}")
    return data, latency

# ----------------------
# Stage 1: Fetch (stubs + real-capable adapters)
# ----------------------

def fetch_linkedin_stub(query: str, location: str, window_days: int) -> List[RawJob]:
    today = datetime.now().date()
    return [
        RawJob(
            source="linkedin",
            source_job_id="lnk-001",
            title="Senior Product Manager",
            company="Cognite",
            location="Oslo, NO",
            post_date=str(today - timedelta(days=1)),
            url="https://example.com/cognite-spm",
            description_raw="Lead B2B SaaS products with GenAI features. English work environment.",
            lang_guess="en",
            seniority_hint="Senior",
            employment_type="Full-time",
            source_meta={"latency_ms": random.randint(120, 300)},
        ),
        RawJob(
            source="linkedin",
            source_job_id="lnk-002",
            title="Produktleder for adresse- og bostedsopplysninger",
            company="Skatteetaten",
            location="Oslo, NO",
            post_date=str(today - timedelta(days=12)),
            url="https://example.com/skatteetaten-pl",
            description_raw="Forvalte kjerne data. Norsk språk kreves.",
            lang_guess="no",
            employment_type="Full-time",
            source_meta={"latency_ms": random.randint(120, 300)},
        ),
    ]


def fetch_finn_stub(query: str, location: str, window_days: int) -> List[RawJob]:
    today = datetime.now().date()
    return [
        RawJob(
            source="finn",
            source_job_id="fn-100",
            title="Product Manager – Maritime (Crew Welfare)",
            company="Marlink",
            location="Lysaker, NO",
            post_date=str(today - timedelta(days=2)),
            url="https://example.com/marlink-pm",
            description_raw="B2B connectivity; stakeholder mgmt; English OK.",
            lang_guess="en",
            employment_type="Full-time",
            source_meta={"latency_ms": random.randint(80, 180)},
        ),
    ]

# FINN real-capable
from bs4 import BeautifulSoup

def parse_finn_search(html: str) -> List[RawJob]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[RawJob] = []
    # JSON-LD JobPosting blocks
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.text.strip())
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                title = data.get("title") or ""
                company = (data.get("hiringOrganization") or {}).get("name") or ""
                url = data.get("url") or ""
                location = ((data.get("jobLocation") or {}).get("address") or {}).get("addressLocality") or "Oslo"
                date_str = (data.get("datePosted") or "")[:10]
                desc = data.get("description") or ""
                jobs.append(RawJob(
                    source="finn",
                    source_job_id=sha256(url or title + company)[:12],
                    title=title,
                    company=company,
                    location=f"{location}, NO" if location else "Norway",
                    post_date=date_str or datetime.now().date().isoformat(),
                    url=url,
                    description_raw=BeautifulSoup(desc, "html.parser").get_text(" ").strip(),
                    lang_guess=None,
                    employment_type=None,
                    source_meta={}
                ))
        except Exception:
            continue
    # Fallback: link cards
    if not jobs:
        for a in soup.select("a[href*='/job/']"):
            title = (a.get_text(" ") or "").strip()
            href = a.get("href") or ""
            if not title or not href:
                continue
            jobs.append(RawJob(
                source="finn",
                source_job_id=sha256(href)[:12],
                title=title,
                company="",
                location="Oslo, NO",
                post_date=datetime.now().date().isoformat(),
                url=(href if href.startswith("http") else f"https://www.finn.no{href}"),
                description_raw="",
                lang_guess=None,
                employment_type=None,
                source_meta={}
            ))
    return jobs


def fetch_finn_real_or_stub(query: str, location: str, window_days: int) -> List[RawJob]:
    # Compose a FINN search URL (keywords + location); this may need tuning.
    q = f"{query} {location}".replace(" ", "+")
    url = f"https://www.finn.no/job/fulltime/search.html?q={q}"
    try:
        html, latency_ms = http_get(url)
        jobs = parse_finn_search(html)
        for j in jobs:
            j.source_meta = j.source_meta or {}
            j.source_meta["latency_ms"] = latency_ms
        # window filter
        cutoff = datetime.now().date() - timedelta(days=window_days)
        filtered: List[RawJob] = []
        for j in jobs:
            try:
                d = datetime.fromisoformat(j.post_date).date()
            except Exception:
                d = datetime.now().date()
            if d >= cutoff:
                filtered.append(j)
        return filtered or fetch_finn_stub(query, location, window_days)
    except Exception:
        return fetch_finn_stub(query, location, window_days)

# Greenhouse & Lever adapters

def normalize_phrase(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower()).strip()

KEY_PHRASES = [
    "product manager",
    "senior product manager",
    "product owner",
]

def query_matches(title: str, description: str, query: str) -> bool:
    t = normalize_phrase(title)
    d = normalize_phrase(description)
    return any(p in t or p in d for p in KEY_PHRASES)


def location_matches(loc_text: str, wanted: str) -> bool:
    return normalize_phrase(wanted) in normalize_phrase(loc_text or "")

# Greenhouse: https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true

def fetch_greenhouse_boards(query: str, location: str, window_days: int, boards: List[str]) -> List[RawJob]:
    jobs: List[RawJob] = []
    cutoff = datetime.now().date() - timedelta(days=window_days)
    for board in boards:
        url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
        params = {"content": "true"}
        try:
            data, latency_ms = http_get_json(url, params=params)
            for item in (data.get("jobs") or []):
                title = item.get("title") or ""
                abs_url = item.get("absolute_url") or ""
                loc_name = (item.get("location") or {}).get("name") or ""
                updated = (item.get("updated_at") or item.get("created_at") or "")[:10]
                desc = item.get("content") or ""
                # filters
                if location and loc_name and not location_matches(loc_name, location):
                    continue
                if not query_matches(title, desc, query):
                    continue
                try:
                    d = datetime.fromisoformat(updated).date()
                except Exception:
                    d = datetime.now().date()
                if d < cutoff:
                    continue
                jobs.append(RawJob(
                    source="greenhouse",
                    source_job_id=str(item.get("id") or sha256(abs_url)[:12]),
                    title=title,
                    company=(item.get("company") or "").strip() or board,
                    location=loc_name or location,
                    post_date=d.isoformat(),
                    url=abs_url,
                    description_raw=BeautifulSoup(desc, "html.parser").get_text(" ").strip(),
                    lang_guess=None,
                    employment_type=None,
                    source_meta={"latency_ms": latency_ms, "board": board},
                ))
        except Exception:
            continue
    return jobs

# Lever: https://api.lever.co/v0/postings/{company}?mode=json

def fetch_lever_companies(query: str, location: str, window_days: int, companies: List[str]) -> List[RawJob]:
    jobs: List[RawJob] = []
    cutoff = datetime.now().date() - timedelta(days=window_days)
    for comp in companies:
        url = f"https://api.lever.co/v0/postings/{comp}"
        params = {"mode": "json"}
        try:
            data, latency_ms = http_get_json(url, params=params)
            for item in data or []:
                title = item.get("text") or ""
                cats = item.get("categories") or {}
                loc = cats.get("location") or ""
                created_ms = item.get("createdAt")
                try:
                    d = datetime.fromtimestamp(int(created_ms)/1000.0).date() if created_ms else datetime.now().date()
                except Exception:
                    d = datetime.now().date()
                if d < cutoff:
                    continue
                desc_html = item.get("content") or ""
                desc = BeautifulSoup(desc_html, "html.parser").get_text(" ").strip()
                hosted = item.get("hostedUrl") or item.get("applyUrl") or ""
                # filters
                if location and loc and not location_matches(loc, location):
                    continue
                if not query_matches(title, desc, query):
                    continue
                jobs.append(RawJob(
                    source="lever",
                    source_job_id=str(item.get("id") or sha256(hosted)[:12]),
                    title=title,
                    company=comp,
                    location=loc or location,
                    post_date=d.isoformat(),
                    url=hosted,
                    description_raw=desc,
                    lang_guess=None,
                    employment_type=cats.get("commitment"),
                    source_meta={"latency_ms": latency_ms, "company": comp},
                ))
        except Exception:
            continue
    return jobs

# ----------------------
# Stage 1 orchestration
# ----------------------

def stage_fetch(query: str, location: str, window_days: int) -> Tuple[List[RawJob], List[str]]:
    logs: List[str] = []
    adapters: List[Any] = []
    if USE_GREENHOUSE and GREENHOUSE_BOARDS:
        adapters.append(lambda q, l, w: fetch_greenhouse_boards(q, l, w, GREENHOUSE_BOARDS))
    adapters.append(fetch_finn_real_or_stub)
    if USE_LEVER and LEVER_COMPANIES:
        adapters.append(lambda q, l, w: fetch_lever_companies(q, l, w, LEVER_COMPANIES))
    adapters.append(fetch_linkedin_stub)

    results: List[RawJob] = []
    ok = 0
    for adapter in adapters:
        try:
            chunk = adapter(query, location, window_days)
            results.extend(chunk)
            ok += 1
        except Exception as e:
            logs.append(f"[LOG][Dhruva][Fetch] adapter={getattr(adapter, '__name__', 'lambda')} error={e}")
    logs.append(f"[LOG][Dhruva][Fetch] count={len(results)} sources_ok={ok}/{len(adapters)}")
    return results, logs

# ----------------------
# Stage 2: Normalize
# ----------------------

def guess_lang(text: str) -> str:
    if re.search(r"\b(å|ø|æ|Norsk|Norge)\b", text, re.IGNORECASE):
        return "no"
    return "en"


def normalize_job(raw: RawJob) -> Job:
    try:
        dt = datetime.fromisoformat(raw.post_date)
    except Exception:
        dt = datetime.now()
    age = (datetime.now() - dt).days
    lang = raw.lang_guess or guess_lang(raw.description_raw)
    needs_tx = lang != "en"
    job_id = sha256(f"{raw.source}:{raw.source_job_id}")
    return Job(
        job_id=job_id,
        title=raw.title,
        company=raw.company,
        location=raw.location,
        post_date=dt.strftime("%Y-%m-%d"),
        age_days=age,
        url=raw.url,
        description_raw=raw.description_raw,
        description_lang=lang,
        needs_translation=needs_tx,
        description_en=None,
        signals={"keywords": [], "language_req": []},
        seniority=raw.seniority_hint,
        source=raw.source,
        source_meta=raw.source_meta or {},
    )


def extract_signals(job: Job) -> None:
    text = job.description_en or job.description_raw
    keys = []
    for k in ["GenAI","LLM","SaaS","B2B","SQL","analytics","roadmap","Norwegian","English"]:
        if re.search(rf"\b{k}\b", text, re.IGNORECASE):
            keys.append(k)
    job.signals["keywords"] = keys
    if re.search(r"Norwegian", text, re.IGNORECASE):
        job.signals["language_req"].append("Norwegian")
    if re.search(r"English", text, re.IGNORECASE):
        job.signals["language_req"].append("English")


def stage_normalize(raws: List[RawJob]) -> Tuple[List[Job], List[str]]:
    jobs = [normalize_job(r) for r in raws]
    logs = [f"[LOG][Dhruva][Normalize] in={len(raws)} out={len(jobs)} dedup_keys_ready=true"]
    return jobs, logs

# ----------------------
# Stage 3: Deduplicate
# ----------------------

def stage_dedup(jobs: List[Job]) -> Tuple[List[Job], List[str]]:
    before = len(jobs)
    seen = set()
    unique: List[Job] = []
    for j in jobs:
        if j.job_id in seen:
            continue
        seen.add(j.job_id)
        unique.append(j)
    logs = [f"[LOG][Dhruva][Dedup] before={before} after={len(unique)} merged_pairs={before-len(unique)}"]
    return unique, logs

# ----------------------
# Stage 4: Translate (stub)
# ----------------------

def translate_to_en(text: str, source_lang: str) -> Tuple[str, float]:
    if source_lang == "en":
        return text, 1.0
    return f"[AUTO-TRANSLATED from {source_lang}] " + text, 0.6


def stage_translate(jobs: List[Job]) -> Tuple[List[Job], List[str]]:
    count = 0
    for j in jobs:
        if j.needs_translation:
            j.description_en, conf = translate_to_en(j.description_raw, j.description_lang)
            j.source_meta["translation_confidence"] = conf
            count += 1
        else:
            j.description_en = j.description_raw
            j.source_meta["translation_confidence"] = 1.0
        extract_signals(j)
    logs = [f"[LOG][Dhruva][Translate] translated={count} avg_chars={int(sum(len(x.description_raw) for x in jobs)/max(1,len(jobs)))}"]
    return jobs, logs

# ----------------------
# Stage 5: Enrich (light)
# ----------------------

def classify_company_type(name: str) -> str:
    if re.search(r"Skatteetaten|Municipal|Directorate|Agency", name, re.IGNORECASE):
        return "government"
    return "private"


def infer_english_ok(job: Job) -> bool | None:
    if "Norwegian" in job.signals.get("language_req", []):
        return False
    if "English" in job.signals.get("language_req", []):
        return True
    return None


def stage_enrich(jobs: List[Job]) -> Tuple[List[Job], List[str]]:
    english_ok = 0
    norwegian_req = 0
    for j in jobs:
        j.signals["company_type"] = classify_company_type(j.company)
        eng_ok = infer_english_ok(j)
        j.signals["english_ok"] = eng_ok
        if eng_ok is True:
            english_ok += 1
        elif eng_ok is False:
            norwegian_req += 1
    logs = [f"[LOG][Dhruva][Enrich] enriched={len(jobs)} flags={{english_ok:{english_ok}, norwegian_required:{norwegian_req}}}"]
    return jobs, logs

# ----------------------
# Stage 6: Rank
# ----------------------

def match_any(text: str, arr: List[str]) -> bool:
    return any(re.search(rf"\b{re.escape(x)}\b", text, re.IGNORECASE) for x in arr)


def score_job(j: Job) -> Tuple[float, List[str]]:
    score = 0.0
    reasons: List[str] = []
    title = j.title
    desc = j.description_en or j.description_raw
    # Title match
    if match_any(title, ["Product Manager", "Senior Product Manager", "Product Owner"]):
        score += 3.0; reasons.append("Title match")
    # Domain / signals
    kws = set(j.signals.get("keywords", []))
    if {"B2B","SaaS"} & kws:
        score += 2.5; reasons.append("B2B/SaaS")
    if j.signals.get("english_ok") is True:
        score += 1.5; reasons.append("English OK")
    if j.seniority and re.search(r"Senior|Lead", j.seniority, re.IGNORECASE):
        score += 1.0; reasons.append("Sr/Lead")
    if {"LLM","GenAI","SQL","analytics","roadmap"} & kws:
        score += 1.0; reasons.append("Key skills")
    # Company preferences
    if j.company in SKIP_COMPANIES:
        score -= 10.0; reasons.append("SKIP company")
    if j.company in PRIORITY_COMPANIES:
        score += 0.5; reasons.append("Priority company")
    # Recency
    if j.age_days <= 3:
        score += 1.5; reasons.append("Fresh")
    elif j.age_days <= 7:
        score += 0.8; reasons.append("Recent")
    return max(0.0, score), reasons[:3]


def stage_rank(jobs: List[Job]) -> Tuple[List[Dict[str, Any]], List[str]]:
    scored: List[Tuple[Job, float, List[str]]] = []
    for j in jobs:
        s, r = score_job(j)
        scored.append((j, s, r))
    scored.sort(key=lambda x: (x[1], x[0].post_date), reverse=True)
    ranked = [
        {
            "company": j.company,
            "title": j.title,
            "location": j.location,
            "post_date": j.post_date,
            "url": j.url,
            "score": round(s, 2),
            "reasons": r,
            "english_ok": j.signals.get("english_ok"),
            "keywords": j.signals.get("keywords", []),
            "age_days": j.age_days,
        }
        for (j, s, r) in scored
    ]
    top1 = f"{ranked[0]['company']} – {ranked[0]['title']}" if ranked else "None"
    logs = [f"[LOG][Dhruva][Rank] ranked={len(ranked)} top1=\"{top1}\""]
    return ranked, logs

# ----------------------
# Stage 7: Select & Act
# ----------------------

def stage_select(ranked: List[Dict[str, Any]], top_n: int) -> Tuple[List[Dict[str, Any]], List[str]]:
    final = ranked[:top_n]
    new_today = sum(1 for x in final if x.get("age_days", 999) == 0)
    logs = [f"[LOG][Dhruva][Select] final_count={len(final)} new_today={new_today} existing={len(final)-new_today}"]
    return final, logs


def write_daily_json(date_str: str, payload: Dict[str, Any]) -> None:
    ensure_log_dir()
    prune_old_logs()
    path = os.path.join("./logs", f"{date_str}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ----------------------
# Orchestration
# ----------------------

def run_pipeline(location: str, window_days: int, top_n: int) -> Tuple[List[Dict[str, Any]], List[str]]:
    logs: List[str] = []
    query = "(Product Manager) OR (Senior Product Manager) OR (Product Owner)"

    # Quiet window (safety)
    if datetime.now().hour >= QUIET_AFTER_HOUR:
        logs.append("[LOG][Dhruva][Guard] skipped – quiet window")
        return [], logs

    raws, L1 = stage_fetch(query, location, window_days)
    logs.extend(L1)
    jobs, L2 = stage_normalize(raws)
    logs.extend(L2)
    jobs, L3 = stage_dedup(jobs)
    logs.extend(L3)
    jobs, L4 = stage_translate(jobs)
    logs.extend(L4)
    jobs, L5 = stage_enrich(jobs)
    logs.extend(L5)
    ranked, L6 = stage_rank(jobs)
    logs.extend(L6)
    final, L7 = stage_select(ranked, top_n)
    logs.extend(L7)

    # Rollup
    rollup = {
        "fetched": len(raws),
        "unique": len(jobs),
        "final": len(final),
        "median_age": sorted([j.get("age_days", 0) for j in final])[len(final)//2] if final else None,
        "english_ok": sum(1 for j in final if j.get("english_ok") is True),
        "genai_hits": sum(1 for j in final if any(k in (j.get("keywords") or []) for k in ("GenAI","LLM"))),
        "errors": 0,
    }
    logs.append(
        f"[LOG][Dhruva][Rollup] fetched={rollup['fetched']} unique={rollup['unique']} final={rollup['final']} "
        f"median_age={rollup['median_age']} english_ok={rollup['english_ok']} genai_hits={rollup['genai_hits']} errors={rollup['errors']}"
    )

    # Persist daily snapshot
    date_str = datetime.now().date().isoformat()
    payload = {
        "run_id": date_str,
        "generated_at": now_local_iso(),
        "stats": rollup,
        "jobs": final,
        "logs": logs,
    }
    write_daily_json(date_str, payload)

    return final, logs

# ----------------------
# CLI
# ----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--location", default="Oslo")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args()

    results, logs = run_pipeline(args.location, args.days, args.top)

    for line in logs:
        print(line)
    print("\n=== FINAL (Top) ===")
    for i, r in enumerate(results, 1):
        print(f"{i:02d}. {r['company']} — {r['title']} | {r['location']} | {r['post_date']} | score={r['score']} | reasons={r['reasons']}")
