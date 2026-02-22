"""
Apache Nutch REST Wrapper — Exposes crawl management via HTTP.
Nutch crawls feed into Solr for indexing.
"""

import asyncio
import os
import uuid
import time
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException

app = FastAPI(title="Nutch Crawler Manager", version="1.0.0")

NUTCH_HOME = os.getenv("NUTCH_HOME", "/opt/nutch")
DATA_DIR = os.getenv("NUTCH_DATA", "/opt/nutch/data")
SOLR_URL = os.getenv("SOLR_URL", "http://solr:8983/solr/gettingstarted")

jobs: dict = {}


async def _run_nutch_crawl(job_id: str, seed_dir: str, crawl_dir: str, depth: int):
    """Run a Nutch crawl pipeline in the background."""
    try:
        jobs[job_id]["status"] = "running"
        cmd = (
            f"{NUTCH_HOME}/bin/crawl "
            f"-i -D solr.server.url={SOLR_URL} "
            f"{seed_dir} {crawl_dir} {depth}"
        )
        jobs[job_id]["command"] = cmd

        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        jobs[job_id]["status"] = "completed" if proc.returncode == 0 else "failed"
        jobs[job_id]["exit_code"] = proc.returncode
        jobs[job_id]["stdout"] = stdout.decode()[-2000:]
        jobs[job_id]["stderr"] = stderr.decode()[-2000:]
        jobs[job_id]["finished_at"] = time.time()
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)


@app.get("/health")
async def health():
    nutch_bin = Path(f"{NUTCH_HOME}/bin/nutch")
    return {"ok": nutch_bin.exists(), "nutch_home": NUTCH_HOME, "jobs": len(jobs)}


@app.post("/crawl")
async def start_crawl(
    seeds: list[str],
    depth: int = Query(2, ge=1, le=10, description="Crawl depth"),
):
    """Start a new Nutch crawl job with given seed URLs."""
    job_id = str(uuid.uuid4())[:8]
    job_dir = f"{DATA_DIR}/{job_id}"
    seed_dir = f"{job_dir}/seeds"
    crawl_dir = f"{job_dir}/crawldb"

    os.makedirs(seed_dir, exist_ok=True)
    with open(f"{seed_dir}/seed.txt", "w") as f:
        for url in seeds:
            f.write(url.strip() + "\n")

    jobs[job_id] = {
        "status": "queued",
        "depth": depth,
        "seeds": seeds,
        "started_at": time.time(),
    }

    asyncio.create_task(_run_nutch_crawl(job_id, seed_dir, crawl_dir, depth))

    return {"job_id": job_id, "status": "started", "depth": depth, "seeds": len(seeds)}


@app.get("/jobs")
async def list_jobs():
    """List all crawl jobs and their statuses."""
    return {"total": len(jobs), "jobs": jobs}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Get status of a specific crawl job."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    return jobs[job_id]


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """Remove a completed job from the list."""
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    if jobs[job_id]["status"] == "running":
        raise HTTPException(400, "Cannot delete a running job")
    del jobs[job_id]
    return {"ok": True, "deleted": job_id}
