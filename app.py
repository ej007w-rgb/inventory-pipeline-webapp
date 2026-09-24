"""
Inventory Pipeline - web service.

Upload feed CSVs (+ optional Customer_PO.xlsx) in the browser; the
three pipeline steps run here on the server in a background thread, so
the user's own computer stays free. Poll /api/jobs/<id> for progress,
then download everything as one zip.

Run locally:
    pip install -r requirements.txt
    python app.py            # http://localhost:8000

Production (Docker / Hugging Face Spaces):
    gunicorn -w 1 --threads 8 --timeout 600 -b 0.0.0.0:7860 app:app
Single worker is deliberate: job state lives in this process.
"""
import io
import os
import re
import shutil
import threading
import time
import uuid
import zipfile

from flask import Flask, jsonify, render_template, request, send_file

import inventory_core as core

JOBS_DIR = os.environ.get("JOBS_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs"))
JOB_TTL_SECONDS = int(os.environ.get("JOB_TTL_SECONDS", str(24 * 3600)))
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(2 * 1024 ** 3)))  # 2 GB

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES

os.makedirs(JOBS_DIR, exist_ok=True)

_jobs = {}          # job_id -> job dict
_jobs_lock = threading.Lock()


def _new_job():
    job_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    job = {
        "id": job_id,
        "dir": job_dir,
        "status": "queued",          # queued -> running -> done | failed
        "phase": "",
        "done": 0,
        "total": 1,
        "label": "Waiting to start…",
        "log": [],
        "error": "",
        "files": [],                 # result file names for the UI
        "zip_name": "",
        "created_at": time.time(),
    }
    with _jobs_lock:
        _jobs[job_id] = job
    return job


def _progress_cb(job_id, phase):
    def cb(done, total, label):
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job is not None:
                job["done"] = done
                job["total"] = max(total, 1)
                job["label"] = f"{phase}: {label}"
    return cb


def _run_pipeline(job_id, pricing):
    """Background worker: steps 1-3, then zips the deliverables."""
    with _jobs_lock:
        job = _jobs[job_id]
    job_dir = job["dir"]

    def log(msg):
        with _jobs_lock:
            job["log"].append(str(msg))
            job["log"] = job["log"][-200:]

    try:
        with _jobs_lock:
            job["status"] = "running"

        ok = core.run_step1(
            job_dir, log=log,
            progress=_progress_cb(job_id, "Step 1/3 - processing feeds"),
            announce_completion=False, pricing_config=pricing,
        )
        if not ok:
            raise RuntimeError("Step 1 failed - see log.")

        ok = core.run_step2(
            job_dir, log=log,
            progress=_progress_cb(job_id, "Step 2/3 - building final files"),
            announce_completion=False,
        )
        if not ok:
            raise RuntimeError("Step 2 failed - see log.")

        po_path = os.path.join(job_dir, core.CUSTOMER_PO_FILENAME)
        if os.path.exists(po_path):
            ok = core.run_step3(
                job_dir, log=log,
                progress=_progress_cb(job_id, "Step 3/3 - matching PO"),
                announce_completion=False,
            )
            if not ok:
                raise RuntimeError("Step 3 failed - see log.")

        # Collect deliverables.
        final_dir = os.path.join(job_dir, core.STEP2_OUTPUT_FOLDER)
        result_files = []
        if os.path.isdir(final_dir):
            for name in sorted(os.listdir(final_dir)):
                if name.lower().endswith(".csv"):
                    result_files.append(os.path.join(final_dir, name))
        for pool in core.POOLS.values():
            p = os.path.join(job_dir, pool["filename"])
            if os.path.exists(p):
                result_files.append(p)
        if not result_files:
            raise RuntimeError("Pipeline finished but produced no output files.")

        zip_name = f"inventory_results_{job_id}.zip"
        zip_path = os.path.join(job_dir, zip_name)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for fp in result_files:
                zf.write(fp, arcname=os.path.basename(fp))

        with _jobs_lock:
            job["status"] = "done"
            job["phase"] = ""
            job["label"] = f"Done - {len(result_files)} file(s) ready."
            job["done"] = 1
            job["total"] = 1
            job["files"] = [os.path.basename(fp) for fp in result_files]
            job["zip_name"] = zip_name
    except Exception as e:  # noqa: BLE001 - surfaced to the UI
        with _jobs_lock:
            job["status"] = "failed"
            job["error"] = str(e)
            job["label"] = "Failed."


def _sweep_old_jobs():
    """Delete job folders older than JOB_TTL_SECONDS. Runs forever."""
    while True:
        time.sleep(3600)
        cutoff = time.time() - JOB_TTL_SECONDS
        with _jobs_lock:
            stale = [jid for jid, j in _jobs.items() if j["created_at"] < cutoff]
            for jid in stale:
                shutil.rmtree(_jobs[jid]["dir"], ignore_errors=True)
                del _jobs[jid]


threading.Thread(target=_sweep_old_jobs, daemon=True).start()


@app.get("/")
def index():
    return render_template(
        "index.html",
        defaults={
            "us_rate": core.DEFAULT_US_RATE,
            "uk_rate": core.DEFAULT_UK_RATE,
            "lc_adder": core.DEFAULT_LC_ADDER,
            "margin_pct": core.DEFAULT_MARGIN_PCT,
        },
    )


def _parse_pricing(form):
    def num(key, default):
        try:
            return float(form.get(key, default))
        except (TypeError, ValueError):
            return float(default)
    return core.build_pricing_config(
        us_rate=num("us_rate", core.DEFAULT_US_RATE),
        uk_rate=num("uk_rate", core.DEFAULT_UK_RATE),
        lc_adder=num("lc_adder", core.DEFAULT_LC_ADDER),
        margin_pct=num("margin_pct", core.DEFAULT_MARGIN_PCT),
    )


@app.post("/api/jobs")
def create_job():
    feeds = request.files.getlist("feeds")
    feeds = [f for f in feeds if f and f.filename]
    if not feeds:
        return jsonify({"error": "Upload at least one feed CSV file."}), 400
    for f in feeds:
        if not f.filename.lower().endswith(".csv"):
            return jsonify({"error": f"'{f.filename}' is not a .csv file."}), 400

    po = request.files.get("po")
    if po and po.filename and not po.filename.lower().endswith((".xlsx", ".xls")):
        return jsonify({"error": "Customer PO must be an Excel (.xlsx) file."}), 400

    try:
        pricing = _parse_pricing(request.form)
    except Exception as e:  # noqa: BLE001
        return jsonify({"error": f"Bad pricing input: {e}"}), 400

    job = _new_job()
    # Save feeds under the names the pipeline expects.
    for i, f in enumerate(feeds, start=1):
        f.save(os.path.join(job["dir"], f"ibo_data_{i}.csv"))
    if po and po.filename:
        po.save(os.path.join(job["dir"], core.CUSTOMER_PO_FILENAME))

    threading.Thread(target=_run_pipeline, args=(job["id"], pricing), daemon=True).start()
    return jsonify({"job_id": job["id"]})


@app.get("/api/jobs/<job_id>")
def job_status(job_id):
    if not re.fullmatch(r"[0-9a-f]{12}", job_id or ""):
        return jsonify({"error": "Unknown job."}), 404
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            return jsonify({"error": "Unknown job."}), 404
        return jsonify({
            "job_id": job["id"],
            "status": job["status"],
            "label": job["label"],
            "done": job["done"],
            "total": job["total"],
            "error": job["error"],
            "files": job["files"],
            "log": job["log"][-50:],
        })


@app.get("/api/jobs/<job_id>/download")
def job_download(job_id):
    if not re.fullmatch(r"[0-9a-f]{12}", job_id or ""):
        return jsonify({"error": "Unknown job."}), 404
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None or job["status"] != "done":
            return jsonify({"error": "Results not ready."}), 404
        zip_path = os.path.join(job["dir"], job["zip_name"])
    return send_file(zip_path, as_attachment=True, download_name=job["zip_name"])


@app.get("/health")
def health():
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), threaded=True)
