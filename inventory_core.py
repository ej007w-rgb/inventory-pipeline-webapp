"""
inventory_core.py  (performance rewrite for large datasets)
-------------------------------------------------------------
Shared logic for the Inventory Pipeline Suite. This file does the actual
work; you don't run this one directly - run either:
    inventory_suite_cli.py   (menu you type numbers into, with a progress bar)
    inventory_suite_gui.py   (popup window with buttons and a progress bar)

SETUP (once):
    pip install pandas pyarrow xlsxwriter openpyxl
    (optional, speeds up reading very large Customer_PO.xlsx files:
     pip install python-calamine  - needs pandas 2.2 or newer to take effect)

FOLDER LAYOUT (everything lives in one folder now):

    Your Main Folder/
        inventory_core.py
        inventory_suite_cli.py
        inventory_suite_gui.py
        ibo_data_1.csv               <- raw feeds you drop in
        ibo_data_2.csv
        Customer_PO.xlsx            <- drop in whenever you need Step 3
        Processed_Output/                              <- made by Step 1 (*.parquet)
        .FINAL OUTPUT/                                  <- made by Step 2 (*.csv - nothing else)
        Customer PO- INVxPrice checked - US.xlsx        <- made by Step 3
        Customer PO- INVxPrice checked - UK.xlsx        <- made by Step 3

Step 1 -> Step 2 -> Step 3 each read the previous step's output
automatically, so you just drop files into the main folder and run.

WHAT CHANGED vs the original (same results, much faster on big files):
- The pricing math is done in bulk on whole columns at once instead of
  row-by-row in Python - same formulas, same rounding.
- Step 1 now saves its intermediate files as Parquet instead of CSV:
  typed, compressed, and far quicker to read back. Step 2's output is
  unchanged from the original: plain customer-facing CSVs in
  .FINAL OUTPUT/, and any stray .parquet files there are deleted once
  Step 2 finishes, so that folder ends up holding only CSVs.
- Step 3 only loads the ~8 columns it actually needs for matching, and
  builds each pool's lookup separately - the whole inventory is never
  held in memory as text anymore.
- Step 3's Excel output is written with xlsxwriter: header/column
  formatting is applied per column in one call each, and column widths
  are measured from the data directly instead of scanning every cell.
- CSV reads use bigger chunks (100k rows) and the progress-bar row count
  uses a fast block scan instead of a slow line-by-line count.

PROGRESS REPORTING
Every run_stepN() / run_all() function takes an optional `progress`
callback: progress(done, total, label). It's called repeatedly while
large files are being read/written (in chunks) so a progress bar can
move smoothly even on a single big file, not just jump 0 -> 100 once
per file. Pass progress=None (the default) to skip this.
"""

import os
import re
import glob

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Rows processed per chunk. Larger chunks mean fewer trips through the
# Python loop and faster parsing; 100k is a good balance for big files
# (progress still updates regularly, memory stays bounded).
CHUNK_SIZE = 100_000

# Intermediate files are Parquet now (typed + compressed + fast).
PARQUET_EXT = ".parquet"


def _noop_progress(done, total, label):
    pass


def _count_data_rows(filepath):
    """Fast approximate row count (minus header) used only to size the
    progress bar - it's fine if it's slightly off for files with
    newlines inside quoted text fields. Counts newlines in 1MB blocks
    (~GB/s) instead of iterating line by line in Python."""
    try:
        count = 0
        with open(filepath, "rb") as f:
            while True:
                block = f.read(1024 * 1024)
                if not block:
                    break
                count += block.count(b"\n")
        return max(count - 1, 0)
    except OSError:
        return 0


# =======================================================================
# STEP 1 - process_inventory: raw feed -> calculated columns
# =======================================================================

# --- Dynamic pricing inputs -------------------------------------------
# These four values used to be hard-coded. They're now adjustable per
# run (CLI prompts for them, GUI has entry boxes) and get bundled into
# a "pricing config" that flows through Step 1. Defaults match the
# original formula exactly (verified against Inventory_feed_part_1-
# Execute.xlsx: US rate 96.03, UK rate 129, LC adder 320, OFFER divisor
# 0.78 <-> 22% margin).
DEFAULT_US_RATE = 96.03
DEFAULT_UK_RATE = 129
DEFAULT_LC_ADDER = 320
DEFAULT_MARGIN_PCT = 22  # 22% margin -> 0.78 divisor

# The "Final Quoted Price" divisor now tracks whatever rate is entered:
# divisor = rate - 1 (e.g. UK rate 129 -> divisor 128). No separate
# input needed - it's derived automatically from the US/UK rate above.


def _rate_to_fqp_divisor(rate):
    """Final Quoted Price divisor = conversion rate - 1."""
    divisor = rate - 1
    if divisor <= 0:
        raise ValueError("Conversion rate must be greater than 1 (so the FQP divisor stays positive).")
    return divisor


def margin_pct_to_offer_divisor(margin_pct):
    """Converts a margin % into the OFFER divisor the formula actually
    uses (divisor = 1 - margin/100). 22% -> 0.78, 25% -> 0.75, etc."""
    margin_pct = float(margin_pct)
    if not (0 <= margin_pct < 100):
        raise ValueError("Margin % must be between 0 and 100 (exclusive of 100).")
    return round(1 - (margin_pct / 100.0), 10)


def build_pricing_config(us_rate=DEFAULT_US_RATE, uk_rate=DEFAULT_UK_RATE,
                         lc_adder=DEFAULT_LC_ADDER, margin_pct=DEFAULT_MARGIN_PCT):
    """Bundles the user-adjustable pricing inputs into one config dict.
    margin_pct is converted to the OFFER divisor once, here, so the rest
    of Step 1 just uses a plain divisor and never has to know about
    "margin" as a concept. Likewise, each rate's FQP divisor (rate - 1)
    is derived here rather than being a separate input."""
    us_rate = float(us_rate)
    uk_rate = float(uk_rate)
    lc_adder = float(lc_adder)
    offer_divisor = margin_pct_to_offer_divisor(margin_pct)
    us_fqp_divisor = _rate_to_fqp_divisor(us_rate)
    uk_fqp_divisor = _rate_to_fqp_divisor(uk_rate)

    return {
        "lc_adder": lc_adder,
        "offer_divisor": offer_divisor,
        "margin_pct": float(margin_pct),
        # "IPS" is an alternate US-side source code - treated identically
        # to US everywhere in this pipeline (same rate/divisor/currency,
        # same pool), so it always mirrors whatever the US rate is set to.
        "source_rules": {
            "US":  {"rate": us_rate, "fqp_divisor": us_fqp_divisor, "currency": "USD"},
            "IPS": {"rate": us_rate, "fqp_divisor": us_fqp_divisor, "currency": "USD"},
            "UK":  {"rate": uk_rate, "fqp_divisor": uk_fqp_divisor, "currency": "GBP"},
        },
    }


# Used whenever a caller doesn't supply its own pricing_config - keeps
# the original hard-coded behaviour as the default.
DEFAULT_PRICING_CONFIG = build_pricing_config()

DISCOUNT_CODE_MAP = {"NET": 0, "LOW": 20, "REG": 40}
# Feed filenames the pipeline will pick up. Add a pattern here if a new
# source ships files under a different name.
FEED_FILE_PATTERNS = ["ibo_data_*.csv", "batch_*.csv", "merged_part_*.csv"]
STEP1_OUTPUT_FOLDER = "Processed_Output"


def _get_discount_percent(discount_value):
    """Plain-Python discount parser, kept for the sense-check's
    independent recompute (deliberately not vectorized)."""
    s = str(discount_value).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return float(s)
    except ValueError:
        pass
    code = s.upper()
    if code in DISCOUNT_CODE_MAP:
        return DISCOUNT_CODE_MAP[code]
    return None


def _parse_discount_series(s):
    """Vectorized discount parsing: numbers stay numbers, NET/LOW/REG
    map to 0/20/40, anything else (blank, 'nan', junk) becomes NaN.
    Same outcomes as _get_discount_percent, on the whole column at
    once."""
    s = s.fillna("").astype("string").str.strip()
    numeric = pd.to_numeric(s, errors="coerce")
    codes = pd.to_numeric(s.str.upper().map(DISCOUNT_CODE_MAP), errors="coerce")
    return numeric.fillna(codes).astype("float64")


def _format_percent_series(pct):
    """Vectorized '20%' / '22.5%' formatting. NaN -> '' (same as the old
    per-row version, which also had to cope with pandas turning the
    mixed None/float column into NaN)."""
    out = pd.Series("", index=pct.index, dtype="string")
    m = pct.notna()
    if not m.any():
        return out
    vals = pct[m]
    str_vals = vals.astype("string")
    whole = vals % 1 == 0
    # Whole-number floats print without the ".0" ("20%"), like before.
    str_vals[whole] = vals[whole].round().astype("int64").astype("string")
    out[m] = str_vals + "%"
    return out


def _normalize_int_like_series(s):
    """Vectorized version of the old _format_ean / _format_whole_number /
    _normalize_isbn helpers: '53.0' -> '53', '9.78E+12' -> '978...',
    blanks and 'nan' -> '', anything else left exactly as-is."""
    s = s.fillna("").astype("string").str.strip()
    num = pd.to_numeric(s, errors="coerce")
    out = s.copy()
    m = num.notna()
    # int64 fast path for realistic magnitudes; plain-Python fallback
    # for absurdly large values so nothing silently overflows.
    safe = m & (num.abs() < 9e15)
    out[safe] = num[safe].round().astype("int64").astype("string")
    rest = m & ~safe
    if rest.any():
        out[rest] = num[rest].map(lambda x: str(int(round(x)))).astype("string")
    out[(s == "") | (s.str.lower() == "nan")] = ""
    return out


# --- Raw feed column aliases ------------------------------------------
# Different data sources ship the same fields under different header
# names (and in different column orders - order doesn't matter, these
# are matched by name). Everything downstream uses the canonical name on
# the right, so add a line here rather than renaming code if a new feed
# turns up. Alias keys are matched case-insensitively.
#   "ean" / "isbn"                  -> isbn          (identifier)
#   "quantity" / "qty"              -> qty           (stock quantity)
#   "fditemtitle" / "title"         -> title
#   "fdpublishername" / "publisher" -> publisher
#   "fdbisaccode" / "bisac_codes"   -> bisac_codes
# Files in any of these styles can sit in the folder together.
COLUMN_ALIASES = {
    "ean": "isbn",
    "quantity": "qty",
    "fditemtitle": "title",
    "fdpublishername": "publisher",
    "fdbisaccode": "bisac_codes",
}


def _normalize_feed_columns(chunk):
    """Renames known alias columns to their canonical names and strips
    stray whitespace/BOM from headers. Matching is case-insensitive.
    Idempotent and safe to call more than once. If a file already has the
    canonical column, the alias is left alone rather than overwriting
    real data."""
    chunk.columns = [str(c).strip().lstrip("\ufeff") for c in chunk.columns]
    present = {c.lower() for c in chunk.columns}
    rename = {
        c: COLUMN_ALIASES[c.lower()]
        for c in chunk.columns
        if c.lower() in COLUMN_ALIASES
        and COLUMN_ALIASES[c.lower()] not in present
    }
    if rename:
        chunk = chunk.rename(columns=rename)
    return chunk


def _compute_step1_columns(chunk, pricing_config):
    """Add the calculated columns to one chunk - same math as before,
    just done on whole columns at once (vectorized) instead of one row
    at a time, and driven by a pricing_config dict so the rates/adder/
    divisor can change per run.

    Returns (chunk, diag) where diag counts rows whose Final Quoted
    Price came out blank and why, so bad input data shows up in the log
    instead of passing silently."""
    chunk = _normalize_feed_columns(chunk)
    source_rules = pricing_config["source_rules"]
    lc_adder = pricing_config["lc_adder"]
    offer_divisor = pricing_config["offer_divisor"]
    known_sources = set(source_rules)

    price = pd.to_numeric(chunk["price"], errors="coerce").astype("float64")
    discount_pct = _parse_discount_series(chunk["discount"])
    discount_frac = discount_pct / 100.0
    source = chunk["inventory_source"].fillna("").astype("string").str.strip()

    # One dict-map per attribute instead of a Python lambda per row.
    rate = source.map({k: v["rate"] for k, v in source_rules.items()}).astype("float64")
    fqp_divisor = source.map({k: v["fqp_divisor"] for k, v in source_rules.items()}).astype("float64")
    currency = source.map({k: v["currency"] for k, v in source_rules.items()}).fillna("").astype("string")

    srp = price
    dis_bp = srp - (discount_frac * srp)
    bp_inr = dis_bp * rate
    lc = bp_inr + lc_adder
    mrp = srp * rate
    offer = lc / offer_divisor
    final_quoted_price = (offer / fqp_divisor).round(2)

    chunk["Disc %"] = _format_percent_series(discount_pct)
    chunk["SRP"] = srp
    chunk["DIS BP$"] = dis_bp
    chunk["BP INR"] = bp_inr
    chunk["LC"] = lc
    chunk["MRP"] = mrp
    chunk["OFFER"] = offer
    chunk["Final Quoted Price"] = final_quoted_price
    chunk["Currency"] = currency

    # Canonical ISBN column (fed from "isbn" or "ean" - see COLUMN_ALIASES)
    # still gets the scientific-notation cleanup.
    if "isbn" in chunk.columns:
        chunk["isbn"] = _normalize_int_like_series(chunk["isbn"])

    # Stock quantity arrives as "53.0" from some sources and "53" from
    # others - normalize so the final output is consistent.
    if "qty" in chunk.columns:
        chunk["qty"] = _normalize_int_like_series(chunk["qty"])

    # Any row whose Final Quoted Price came out blank (NaN) is a row
    # the customer-facing output can't price - counted here from the
    # series we already computed, no extra passes over the data.
    blank_mask = final_quoted_price.isna()
    diag = {
        "blank": int(blank_mask.sum()),
        "bad_discount": int((blank_mask & discount_pct.isna()).sum()),
        "unknown_source": int((blank_mask & ~source.isin(known_sources)).sum()),
        "bad_price": int((blank_mask & price.isna()).sum()),
    }
    return chunk, diag


def _process_one_feed_file(filepath, output_dir, log, progress, file_label, pricing_config):
    filename = os.path.basename(filepath)
    total_rows = _count_data_rows(filepath)
    out_path = os.path.join(output_dir, os.path.splitext(filename)[0] + PARQUET_EXT)

    try:
        reader = pd.read_csv(
            filepath, dtype=str, keep_default_na=False, encoding="utf-8-sig",
            chunksize=CHUNK_SIZE
        )
    except Exception as e:
        log(f"  !! Couldn't open {filename}: {e}")
        return False

    # Checked against canonical names, so a feed using either header
    # style ("isbn"/"ean", "qty"/"quantity") passes.
    required = {"price", "discount", "inventory_source", "isbn", "qty"}
    rows_done = 0
    first_chunk = True
    totals = {"blank": 0, "bad_discount": 0, "unknown_source": 0, "bad_price": 0}
    writer = None

    try:
        for chunk in reader:
            if len(chunk) == 0:
                continue
            raw_columns = {str(c).strip().lstrip("\ufeff") for c in chunk.columns}
            raw_lower = {c.lower() for c in raw_columns}
            chunk = _normalize_feed_columns(chunk)

            if first_chunk:
                missing = required - set(chunk.columns)
                if missing:
                    log(f"  !! Skipping {filename} - missing column(s): {', '.join(sorted(missing))}")
                    return False
                applied = [
                    f"{c}->{COLUMN_ALIASES[c.lower()]}"
                    for c in sorted(raw_columns)
                    if c.lower() in COLUMN_ALIASES
                    and COLUMN_ALIASES[c.lower()] not in raw_lower
                ]
                if applied:
                    log(f"     (header aliases applied: {', '.join(applied)})")

            chunk, diag = _compute_step1_columns(chunk, pricing_config)
            for k in totals:
                totals[k] += diag[k]

            # Parquet keeps real types (no string round-trip) and is far
            # quicker to read back in Steps 2/3. ParquetWriter streams
            # chunk-by-chunk so memory stays bounded on huge files.
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema)
            writer.write_table(table)

            first_chunk = False
            rows_done += len(chunk)
            progress(rows_done, max(total_rows, rows_done, 1), f"{file_label} - {rows_done:,} rows")
    finally:
        if writer is not None:
            writer.close()

    log(f"  -> {filename} processed ({rows_done:,} rows)")
    if totals["blank"]:
        log(f"     ⚠️ {totals['blank']:,} row(s) got no Final Quoted Price "
            f"(unreadable discount: {totals['bad_discount']:,}, "
            f"unknown inventory_source: {totals['unknown_source']:,}, "
            f"unreadable price: {totals['bad_price']:,})")
    return True


def sense_check_step1(base_dir, pricing_config, log=print, sample_size=25):
    """Independently re-derives Final Quoted Price / Currency for a
    sample of rows using plain Python (no pandas vectorization) and the
    SAME pricing_config just used to run Step 1, then compares against
    what actually landed in Processed_Output/. This isn't a before/after
    diff - it's a from-scratch recompute, so it catches any place where
    a changed rate/adder/margin failed to reach the real calculation.
    Runs automatically at the end of every Step 1 (and therefore every
    Run All). Only the first batch of each file is read, so this stays
    fast even on huge outputs."""
    output_dir = os.path.join(base_dir, STEP1_OUTPUT_FOLDER)
    files = sorted(glob.glob(os.path.join(output_dir, "*" + PARQUET_EXT)))
    if not files:
        log("\nSense check skipped - no Step 1 output found.")
        return False

    source_rules = pricing_config["source_rules"]
    lc_adder = pricing_config["lc_adder"]
    offer_divisor = pricing_config["offer_divisor"]
    needed = ["inventory_source", "price", "discount", "Final Quoted Price", "Currency"]

    checked = 0
    mismatches = []

    for f in files:
        try:
            pf = pq.ParquetFile(f)
        except Exception:
            continue
        available = [c for c in needed if c in pf.schema.names]
        try:
            batch = next(pf.iter_batches(batch_size=sample_size, columns=available))
        except StopIteration:
            continue
        df = batch.to_pandas()
        for i, row in df.iterrows():
            source = str(row["inventory_source"]).strip()
            rule = source_rules.get(source)
            if rule is None:
                continue  # unrecognized source - nothing to sense-check against

            try:
                price = float(row["price"])
            except (ValueError, TypeError):
                continue
            if pd.isna(price):
                continue
            disc_pct = _get_discount_percent(row["discount"])
            if disc_pct is None:
                continue

            disc_frac = disc_pct / 100.0
            srp = price
            dis_bp = srp - (disc_frac * srp)
            bp_inr = dis_bp * rule["rate"]
            lc = bp_inr + lc_adder
            offer = lc / offer_divisor
            expected_fqp = round(offer / rule["fqp_divisor"], 2)
            expected_currency = rule["currency"]

            checked += 1
            try:
                actual_fqp = float(row["Final Quoted Price"])
            except (ValueError, TypeError):
                actual_fqp = None
            if actual_fqp is not None and pd.isna(actual_fqp):
                actual_fqp = None
            actual_currency = row.get("Currency", "")
            if pd.isna(actual_currency):
                actual_currency = ""

            if (actual_fqp is None or abs(expected_fqp - actual_fqp) > 0.01
                    or actual_currency != expected_currency):
                mismatches.append((os.path.basename(f), i, expected_fqp, actual_fqp, expected_currency, actual_currency))

    if checked == 0:
        log("\nSense check skipped - no recognizable rows to verify.")
        return False

    if mismatches:
        log(f"\n⚠️ SENSE CHECK FAILED: {len(mismatches)}/{checked} sampled row(s) disagree with an "
            f"independent recompute of the same formula.")
        for fname, i, exp_fqp, act_fqp, exp_cur, act_cur in mismatches[:5]:
            log(f"   {fname} row {i}: expected Final Quoted Price {exp_fqp} ({exp_cur}), "
                f"got {act_fqp} ({act_cur})")
        return False

    log(f"\n✅ Sense check passed: {checked} sampled row(s) across {len(files)} file(s) independently "
        f"recomputed and matched Step 1 output.\n"
        f"   US rate={source_rules['US']['rate']} (FQP divisor {source_rules['US']['fqp_divisor']})  "
        f"UK rate={source_rules['UK']['rate']} (FQP divisor {source_rules['UK']['fqp_divisor']})  "
        f"LC adder={lc_adder}  margin={pricing_config['margin_pct']}% -> OFFER divisor={offer_divisor}")
    return True


def run_step1(base_dir, log=print, progress=None, announce_completion=True, pricing_config=None):
    """Process raw ibo_data_*.csv files into Processed_Output/ (Parquet).

    pricing_config (see build_pricing_config()) controls the US/UK
    conversion rate, the LC adder, and the OFFER margin/divisor for
    this run. Defaults to DEFAULT_PRICING_CONFIG (the original
    hard-coded values) if not supplied."""
    progress = progress or _noop_progress
    pricing_config = pricing_config or DEFAULT_PRICING_CONFIG
    output_dir = os.path.join(base_dir, STEP1_OUTPUT_FOLDER)
    os.makedirs(output_dir, exist_ok=True)

    matches = set()
    for pattern in FEED_FILE_PATTERNS:
        matches.update(glob.glob(os.path.join(base_dir, pattern)))
    files = sorted(
        matches,
        key=lambda p: [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", p)]
    )

    if not files:
        log(f"No files matching {' or '.join(FEED_FILE_PATTERNS)} were found in:\n  {base_dir}")
        return False

    log(f"Found {len(files)} feed file(s) to process:")
    log(f"Pricing inputs -> US rate: {pricing_config['source_rules']['US']['rate']} "
        f"(FQP divisor {pricing_config['source_rules']['US']['fqp_divisor']})  "
        f"UK rate: {pricing_config['source_rules']['UK']['rate']} "
        f"(FQP divisor {pricing_config['source_rules']['UK']['fqp_divisor']})  "
        f"LC adder: {pricing_config['lc_adder']}  "
        f"Margin: {pricing_config['margin_pct']}% (OFFER divisor {pricing_config['offer_divisor']})")
    ok = 0
    for i, f in enumerate(files, start=1):
        file_label = f"File {i}/{len(files)}: {os.path.basename(f)}"
        if _process_one_feed_file(f, output_dir, log, progress, file_label, pricing_config):
            ok += 1

    log(f"\nStep 1 done: {ok}/{len(files)} file(s) processed -> {output_dir}")

    if ok > 0:
        sense_check_step1(base_dir, pricing_config, log=log)

    if ok > 0 and announce_completion:
        log("\n✅ Task completed!")
    return ok > 0


# =======================================================================
# STEP 2 - generate_final_output: calculated columns -> customer-facing feed
# =======================================================================

STEP2_OUTPUT_FOLDER = ".FINAL OUTPUT"

FINAL_COLUMNS = {
    "isbn": "ISBN",
    "title": "Title",
    "qty": "quantity",
    "weight": "weight",
    "inventory_source": "inventory_source",
    "Final Quoted Price": "Final Price",
    "Currency": "Currency",
    "publisher": "Publisher",
    "bisac_codes": "Categories",
}


def _build_final_file(filepath, output_dir, log, progress, file_label):
    filename = os.path.basename(filepath)
    out_path = os.path.join(output_dir, os.path.splitext(filename)[0] + ".csv")

    try:
        pf = pq.ParquetFile(filepath)
    except Exception as e:
        log(f"  !! Couldn't open {filename}: {e}")
        return False

    total_rows = pf.metadata.num_rows
    needed = list(FINAL_COLUMNS.keys())
    missing = [c for c in needed if c not in pf.schema.names]
    if missing:
        log(f"  !! Skipping {filename} - missing column(s): {', '.join(missing)}")
        log("     (Step 2 expects the already-processed files from Step 1)")
        return False

    # Write to a temp file first so a failed run never leaves a
    # half-written CSV behind; it is renamed into place when done.
    # The handle stays open across chunks so the BOM is written once.
    tmp_path = out_path + ".tmp"
    rows_done = 0
    try:
        with open(tmp_path, "w", encoding="utf-8-sig", newline="") as fh:
            first = True
            # Only the final columns are ever read - the rest of the
            # feed's columns stay on disk.
            for batch in pf.iter_batches(batch_size=CHUNK_SIZE, columns=needed):
                chunk = batch.to_pandas()
                if len(chunk) == 0:
                    continue

                final_chunk = chunk[needed].rename(columns=FINAL_COLUMNS)
                quantity_num = pd.to_numeric(chunk["qty"], errors="coerce").fillna(0)
                final_chunk["Stock Status"] = np.where(
                    quantity_num > 0, "Stock Available", "No Stock"
                )

                final_chunk.to_csv(fh, index=False, header=first)
                first = False

                rows_done += len(chunk)
                progress(rows_done, max(total_rows, rows_done, 1), f"{file_label} - {rows_done:,} rows")
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise
    os.replace(tmp_path, out_path)

    log(f"  -> {filename} finalized ({rows_done:,} rows)")
    return True


def run_step2(base_dir, log=print, progress=None, announce_completion=True):
    """Build the customer-facing feed from Processed_Output/ into .FINAL OUTPUT/."""
    progress = progress or _noop_progress
    input_dir = os.path.join(base_dir, STEP1_OUTPUT_FOLDER)
    output_dir = os.path.join(base_dir, STEP2_OUTPUT_FOLDER)

    if not os.path.isdir(input_dir):
        log(f"Couldn't find '{STEP1_OUTPUT_FOLDER}' - run Step 1 first.")
        return False

    os.makedirs(output_dir, exist_ok=True)

    files = sorted(
        glob.glob(os.path.join(input_dir, "*" + PARQUET_EXT)),
        key=lambda p: [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", p)]
    )

    if not files:
        log(f"No processed files found in:\n  {input_dir}")
        return False

    log(f"Found {len(files)} file(s) to finalize:")
    ok = 0
    for i, f in enumerate(files, start=1):
        file_label = f"File {i}/{len(files)}: {os.path.basename(f)}"
        if _build_final_file(f, output_dir, log, progress, file_label):
            ok += 1

    log(f"\nStep 2 done: {ok}/{len(files)} file(s) finalized -> {output_dir}")
    if ok > 0:
        # The final folder is customer-facing: it should hold only CSVs.
        # Sweep out any .parquet files left behind by earlier runs.
        for stale in glob.glob(os.path.join(output_dir, "*.parquet")):
            try:
                os.remove(stale)
            except OSError:
                pass
    if ok > 0 and announce_completion:
        log("\n✅ Task completed!")
    return ok > 0


# =======================================================================
# STEP 3 - match_customer_po: customer PO x finalized feed -> US/UK checks
# =======================================================================

CUSTOMER_PO_FILENAME = "Customer_PO.xlsx"
PO_ISBN_COLUMN = "ISBN"
INVENTORY_ISBN_COLUMN = "ISBN"
SOURCE_COLUMN = "inventory_source"

# pool name -> (inventory_source values folded into it, output filename)
# "IPS" is folded into the US pool - it never gets its own file.
POOLS = {
    "US": {"sources": ["US", "IPS"], "filename": "Customer PO- INVxPrice checked - US.xlsx"},
    "UK": {"sources": ["UK"],        "filename": "Customer PO- INVxPrice checked - UK.xlsx"},
}

# The only columns the PO match actually touches - everything else in
# the feed stays on disk instead of being loaded into memory.
LOOKUP_COLUMNS = [
    "ISBN", "Final Price", "Currency", "quantity", "Stock Status",
    "inventory_source", "Categories", "Publisher",
]
# Subset of LOOKUP_COLUMNS carried into the merge. "ISBN" is deliberately
# left out: the output keeps the PO's own ISBN column (as the original
# version did) instead of gaining a second suffixed one.
MERGE_COLUMNS = [
    "Final Price", "Currency", "quantity", "Stock Status",
    "inventory_source", "Categories", "Publisher",
]


def _empty_lookup():
    cols = ["_isbn_key"] + LOOKUP_COLUMNS
    return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})


def _dedupe_to_lookup(combined):
    """One row per ISBN, preferring an in-stock row over an out-of-stock
    one. A single stable sort + drop_duplicates (no Python-level group
    loop), so ties keep their original file/row order."""
    combined["_isbn_key"] = _normalize_int_like_series(combined["ISBN"])
    working = combined[combined["_isbn_key"] != ""].copy()
    if working.empty:
        return _empty_lookup()
    # 0 = in stock, 1 = not - sorting ascending puts in-stock rows
    # first. kind="mergesort" is stable, so ties keep their original
    # file/row order, matching the old "first matching row" behaviour.
    working["_stock_rank"] = (working["Stock Status"] != "Stock Available").astype("int8")
    working = working.sort_values(["_isbn_key", "_stock_rank"], kind="mergesort")
    return working.drop_duplicates(subset="_isbn_key", keep="first")


def _load_pool_lookups(final_output_dir, pools, log, progress):
    """Builds the one-row-per-ISBN lookup table for every pool in a
    single pass over the files.

    Only the columns the match needs are ever read, and each file is
    read exactly once no matter how many pools there are - the old
    version loaded every column of every file as text and concatenated
    it all, which is what blew up memory on large inventories."""
    files = sorted(
        glob.glob(os.path.join(final_output_dir, "*.csv")),
        key=lambda p: [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", p)]
    )
    if not files:
        raise FileNotFoundError(f"No finalized CSV files found inside '{final_output_dir}'.")

    needed = set(LOOKUP_COLUMNS)
    pool_frames = {name: [] for name in pools}
    for i, f in enumerate(files, start=1):
        try:
            # usecols keeps the read narrow: the parser skips every
            # column the match doesn't need. dtype=str + no NA parsing
            # keeps ISBNs and quantities exactly as Step 2 wrote them.
            df = pd.read_csv(
                f,
                usecols=lambda c: c in needed,
                dtype=str,
                keep_default_na=False,
                encoding="utf-8-sig",
            )
        except Exception as e:
            log(f"  !! Skipping {os.path.basename(f)} - couldn't read: {e}")
            continue
        if INVENTORY_ISBN_COLUMN not in df.columns:
            log(f"  !! Skipping {os.path.basename(f)} - no '{INVENTORY_ISBN_COLUMN}' column")
            continue
        for c in LOOKUP_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        # Split this file's rows across the pools up front - one read
        # serves every pool instead of re-reading per pool.
        src = df["inventory_source"].str.strip()
        for name, info in pools.items():
            part = df[src.isin(info["sources"])]
            if len(part):
                pool_frames[name].append(part)
        progress(i, len(files), f"Loading {os.path.basename(f)} ({i}/{len(files)})")

    lookups = {}
    usable = 0
    for name, frames in pool_frames.items():
        if not frames:
            lookups[name] = _empty_lookup()
            continue
        usable += 1
        combined = pd.concat(frames, ignore_index=True)
        lookups[name] = _dedupe_to_lookup(combined)
    if not usable:
        raise ValueError("None of the files inside .FINAL OUTPUT had usable data.")
    return lookups


def _write_matched_po(po, lookup_table, out_path, progress, pool_label):
    progress(0, 100, f"{pool_label} - matching {len(po):,} PO rows")

    po_work = po.copy()
    po_work["_isbn_key"] = _normalize_int_like_series(po_work[PO_ISBN_COLUMN])

    if lookup_table.empty:
        lookup_slim = pd.DataFrame({c: pd.Series(dtype="object") for c in ["_isbn_key"] + MERGE_COLUMNS})
    else:
        lookup_slim = lookup_table[["_isbn_key"] + MERGE_COLUMNS]

    # A single vectorized left-join replaces the old per-row Python
    # loop + dict lookup - this is the main speed-up for large PO files.
    merged = po_work.merge(lookup_slim, on="_isbn_key", how="left")
    merged["Stock Status"] = merged["Stock Status"].fillna("Not Found")
    for col in ("Final Price", "Currency", "quantity", "inventory_source", "Categories", "Publisher"):
        merged[col] = merged[col].fillna("")
    merged = merged.drop(columns=["_isbn_key"])

    matched = int((merged["Stock Status"] != "Not Found").sum())
    not_found = int((merged["Stock Status"] == "Not Found").sum())

    progress(0, 100, f"{pool_label} - writing spreadsheet")

    out = merged.copy()
    out["Final Price"] = pd.to_numeric(out["Final Price"], errors="coerce")
    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce")

    # xlsxwriter applies one format object per column in a single call,
    # instead of the old approach of creating a Font object per cell and
    # looping over every row - that per-cell loop was the single biggest
    # silent delay on large PO files.
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        out.to_excel(writer, index=False, sheet_name="PO", header=False, startrow=1)
        wb = writer.book
        ws = writer.sheets["PO"]

        header_fmt = wb.add_format({"bold": True, "font_name": "Arial", "font_size": 10})
        body_fmt = wb.add_format({"font_name": "Arial", "font_size": 10})
        price_fmt = wb.add_format({"font_name": "Arial", "font_size": 10, "num_format": "#,##0.00"})
        qty_fmt = wb.add_format({"font_name": "Arial", "font_size": 10, "num_format": "#,##0"})

        ws.write_row(0, 0, list(out.columns), header_fmt)
        ws.freeze_panes(1, 0)

        n_cols = len(out.columns)
        for idx, col in enumerate(out.columns):
            # Column widths measured from the data with vectorized string
            # lengths - no need to scan every worksheet cell in Python.
            vals = out[col].dropna()
            longest = int(vals.astype(str).str.len().max()) if len(vals) else 0
            width = min(max(longest, len(str(col))) + 2, 60)
            width = max(width, 10)
            if col == "Final Price":
                fmt = price_fmt
            elif col == "quantity":
                fmt = qty_fmt
            else:
                fmt = body_fmt
            ws.set_column(idx, idx, width, fmt)
            if idx % 5 == 0 or idx == n_cols - 1:
                progress(10 + int(80 * (idx + 1) / max(n_cols, 1)), 100,
                         f"{pool_label} - styling columns")

    progress(100, 100, f"{pool_label} - done")

    return matched, not_found


def run_step3(base_dir, log=print, progress=None, announce_completion=True):
    """Match Customer_PO.xlsx against .FINAL OUTPUT/, split into US/UK checks."""
    progress = progress or _noop_progress
    final_output_dir = os.path.join(base_dir, STEP2_OUTPUT_FOLDER)
    po_path = os.path.join(base_dir, CUSTOMER_PO_FILENAME)

    if not os.path.isdir(final_output_dir):
        log(f"Couldn't find '{STEP2_OUTPUT_FOLDER}' - run Step 2 first.")
        return False
    if not os.path.isfile(po_path):
        log(f"Couldn't find '{CUSTOMER_PO_FILENAME}' in:\n  {base_dir}")
        return False

    log("Loading Customer PO...")
    progress(0, 1, "Loading Customer PO")
    try:
        try:
            # calamine reads large .xlsx files several times faster than
            # openpyxl; fall back gracefully if it isn't installed.
            po = pd.read_excel(po_path, dtype=str, engine="calamine")
        except (ImportError, ValueError):
            po = pd.read_excel(po_path, dtype=str)
    except Exception as e:
        log(f"  !! Couldn't open {CUSTOMER_PO_FILENAME}: {e}")
        return False
    if PO_ISBN_COLUMN not in po.columns:
        log(f"Couldn't find a '{PO_ISBN_COLUMN}' column in {CUSTOMER_PO_FILENAME}.")
        return False

    log("Building ISBN lookups...")
    try:
        lookups = _load_pool_lookups(final_output_dir, POOLS, log, progress)
    except (FileNotFoundError, ValueError) as e:
        log(str(e))
        return False

    any_ok = False
    for pool_name, pool_info in POOLS.items():
        lookup_table = lookups[pool_name]
        out_path = os.path.join(base_dir, pool_info["filename"])
        matched, not_found = _write_matched_po(po, lookup_table, out_path, progress, f"[{pool_name}]")
        log(f"\n[{pool_name}] {len(lookup_table):,} unique ISBN(s) in this pool")
        log(f"[{pool_name}] Matched: {matched}   Not found: {not_found}")
        log(f"[{pool_name}] Saved: {out_path}")
        any_ok = True

    if any_ok and announce_completion:
        log("\n✅ Task completed!")
    return any_ok


# =======================================================================
# RUN ALL
# =======================================================================

def run_all(base_dir, log=print, progress=None, pricing_config=None):
    progress = progress or _noop_progress

    log("=== STEP 1 of 3: Process raw inventory feeds ===")
    if not run_step1(base_dir, log, progress, announce_completion=False, pricing_config=pricing_config):
        log("\nStopped - Step 1 did not complete successfully.")
        return False

    log("\n=== STEP 2 of 3: Generate final customer-facing output ===")
    if not run_step2(base_dir, log, progress, announce_completion=False):
        log("\nStopped - Step 2 did not complete successfully.")
        return False

    log("\n=== STEP 3 of 3: Match against Customer PO ===")
    if not run_step3(base_dir, log, progress, announce_completion=False):
        log("\nStep 3 did not complete (this is fine if you don't have a "
            "Customer_PO.xlsx to check yet - Steps 1 and 2 are done).")
        return False

    log("\nAll 3 steps completed successfully.")
    log("✅ Task completed!")
    return True
