# Inventory Pipeline - Web App

Upload feed CSVs (and optionally `Customer_PO.xlsx`) in the browser. The
three pipeline steps run **on the server in a background thread**, so your
own computer stays free. Close the tab and come back - the job keeps
running; recent jobs are remembered in the browser.

## Run it locally (your own machine does the work)

```bash
pip install -r requirements.txt
python app.py
# open http://localhost:8000
```

## Put it on the internet (recommended - a free host does the work)

The easiest free option is **Hugging Face Spaces** (free, no credit card,
2 vCPU / 16 GB RAM - plenty for this pipeline):

1. Create a free account at https://huggingface.co/join
2. Create a new Space: choose **Docker** as the SDK, name it e.g.
   `inventory-pipeline`, and set visibility to **Private** (only you can
   open it).
3. Upload every file from this folder into the Space (drag & drop on the
   Space's *Files* tab works).
4. The Space builds and starts automatically - open it, upload your feed
   CSVs + Customer PO, press **Run pipeline**, download the zip when done.

Same steps work on any host that runs Docker (Render, Railway, a VPS…):
build the Dockerfile and expose the app's port.

## How it works

- `POST /api/jobs` - multipart upload: `feeds` (one or more .csv),
  `po` (optional .xlsx), plus `us_rate`, `uk_rate`, `lc_adder`,
  `margin_pct`. Returns `{job_id}` and starts the pipeline in a
  background thread.
- `GET /api/jobs/<job_id>` - `{status, label, done, total, error, files,
  log}`. Poll this for the progress bar.
- `GET /api/jobs/<job_id>/download` - zip with the `.FINAL OUTPUT`
  CSVs and the two `Customer PO- INVxPrice checked` workbooks (when a PO
  was uploaded).
- Jobs and their files are deleted automatically after 24 hours
  (`JOB_TTL_SECONDS`).

The pipeline logic itself is untouched `inventory_core.py` - the exact
same code as the desktop v2 suite, so results are identical.
