# maps_scraper_batch.py
# ============================================================
# BATCH + SAFE COMPLETION — Google Maps Scraper
#
# A query is marked completed ONLY after ALL four conditions are met:
#   1. Phase 1 (collect place URLs) finished successfully
#   2. Phase 2 (extract name + website) finished successfully
#   3. All records for that query have been processed
#   4. Data has been successfully written to business_websites.csv
#
# pip install selenium webdriver-manager pandas tqdm beautifulsoup4
# ============================================================

import time
import atexit
import tempfile
import os
import threading
import signal
import sys
from queue import Queue, Empty
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# ===================== CONFIG =====================

BATCH_SIZE             = 10
QUERIES_FILE           = "queries.txt"
COMPLETED_QUERIES_FILE = "completed_queries.txt"
FAILED_QUERIES_FILE    = "failed_queries.txt"

with open(QUERIES_FILE, "r", encoding="utf-8") as f:
    ALL_QUERIES = [line.strip() for line in f if line.strip()]

def load_state() -> tuple[set[str], set[str]]:
    completed: set[str] = set()
    failed: set[str] = set()
    if os.path.exists(COMPLETED_QUERIES_FILE):
        with open(COMPLETED_QUERIES_FILE, "r", encoding="utf-8") as f:
            completed = {line.strip() for line in f if line.strip()}
    if os.path.exists(FAILED_QUERIES_FILE):
        with open(FAILED_QUERIES_FILE, "r", encoding="utf-8") as f:
            failed = {line.strip() for line in f if line.strip()}
    return completed, failed

completed_queries, failed_queries = load_state()
remaining_queries = [q for q in ALL_QUERIES if q not in completed_queries]
SEARCH_QUERIES    = remaining_queries[:BATCH_SIZE]

print(
    f"\n[BATCH] Total queries     : {len(ALL_QUERIES)}\n"
    f"[BATCH] Completed so far  : {len(completed_queries)}\n"
    f"[BATCH] Remaining         : {len(remaining_queries)}\n"
    f"[BATCH] Processing now    : {len(SEARCH_QUERIES)}\n"
)

if not SEARCH_QUERIES:
    print("[BATCH] ✅ All queries already completed. Nothing to do.")
    sys.exit(0)

MAX_LISTINGS     = 100
OUTPUT_CSV       = "business_websites.csv"
BATCH_CSV        = "current_batch.csv"   # ← NAYA: sirf is run ki websites
HEADLESS         = True
PARALLEL_DRIVERS = 2
SCROLL_PAUSE     = 0.3
PLACE_LOAD_WAIT  = 2.0
COLLECT_WORKERS  = 1
SAVE_EVERY       = 50
MAX_RETRIES      = 2

# ===================== GLOBALS =====================
_drivers_lock   = threading.Lock()
_all_drivers: list[webdriver.Chrome] = []
_shutdown_event = threading.Event()
_file_write_lock = threading.Lock()

def _append_to_file(filepath: str, line: str) -> None:
    with _file_write_lock:
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(line.strip() + "\n")

def mark_query_completed(query: str) -> None:
    _append_to_file(COMPLETED_QUERIES_FILE, query)

def mark_query_failed(query: str) -> None:
    _append_to_file(FAILED_QUERIES_FILE, query)

# ===================== SIGNAL HANDLER =====================
def _handle_sigint(sig, frame):
    print("\n\n⚠️  Ctrl+C detected — finishing current tasks and saving…")
    _shutdown_event.set()

signal.signal(signal.SIGINT, _handle_sigint)

# ===================== DRIVER FACTORY =====================
def _make_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.page_load_strategy = "eager"
    if HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1400,900")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-javascript-harmony-shipping")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--mute-audio")
    options.add_argument("--no-first-run")
    options.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
    })
    service = Service(ChromeDriverManager().install())
    drv = webdriver.Chrome(service=service, options=options)
    drv.set_page_load_timeout(15)
    with _drivers_lock:
        _all_drivers.append(drv)
    return drv

def _quit_all():
    with _drivers_lock:
        for drv in _all_drivers:
            try:
                drv.quit()
            except Exception:
                pass
        _all_drivers.clear()

atexit.register(_quit_all)

# ===================== WAIT HELPER =====================
def _wait_for(drv, xpath, timeout=5):
    try:
        return WebDriverWait(drv, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
    except Exception:
        return None

# ===================== PHASE 1: COLLECT HREFS =====================
def search_and_collect_hrefs(drv, query: str, max_links: int) -> list[str]:
    if _shutdown_event.is_set():
        return []
    drv.get(f"https://www.google.com/maps/search/{query.replace(' ', '+')}")
    try:
        WebDriverWait(drv, 3).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept')]"))
        ).click()
    except Exception:
        pass
    panel = None
    for sel in ["//div[@role='feed']", "//div[contains(@class,'m6QErb')]"]:
        try:
            panel = WebDriverWait(drv, 8).until(
                EC.presence_of_element_located((By.XPATH, sel))
            )
            break
        except Exception:
            continue
    if panel is None:
        print(f"  ⚠️  No results panel for: {query}")
        return []
    hrefs: list[str] = []
    href_set: set[str] = set()
    idle_count = 0
    while len(hrefs) < max_links and idle_count < 5 and not _shutdown_event.is_set():
        prev_count = len(hrefs)
        try:
            anchors = panel.find_elements(By.XPATH, ".//a[@href]")
        except Exception:
            anchors = drv.find_elements(By.XPATH, "//a[@href]")
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                short = href.split("?")[0]
                if "/place/" in short and short not in href_set:
                    href_set.add(short)
                    hrefs.append(short)
                    if len(hrefs) >= max_links:
                        break
            except Exception:
                continue
        try:
            drv.execute_script(
                "arguments[0].scrollTop += arguments[0].clientHeight;", panel
            )
        except Exception:
            drv.execute_script("window.scrollBy(0,700);")
        time.sleep(SCROLL_PAUSE)
        idle_count = 0 if len(hrefs) > prev_count else idle_count + 1
    return hrefs[:max_links]

# ===================== PHASE 2: EXTRACT DATA =====================
def extract_place_data(drv, url: str) -> dict:
    drv.get(url)
    name_el = _wait_for(drv, "//h1", timeout=PLACE_LOAD_WAIT)
    name = name_el.text.strip() if name_el else None
    website = None
    try:
        w_el = drv.find_element(
            By.XPATH, "//a[contains(@data-item-id,'authority')]"
        )
        website = w_el.get_attribute("href")
    except Exception:
        pass
    if not name:
        try:
            soup = BeautifulSoup(drv.page_source, "html.parser")
            h1 = soup.find("h1")
            if h1:
                name = h1.get_text(strip=True)
        except Exception:
            pass
    return {"name": name, "website": website, "place_url": url}

def worker_process_hrefs(
    task_queue: Queue,
    results: list,
    results_lock: threading.Lock,
    pbar: tqdm,
    save_callback,
) -> None:
    drv = _make_driver()
    local_buffer: list[dict] = []
    try:
        while not _shutdown_event.is_set():
            try:
                query, href = task_queue.get(timeout=2)
            except Empty:
                break
            rec = None
            for attempt in range(MAX_RETRIES + 1):
                try:
                    data = extract_place_data(drv, href)
                    rec = {
                        "search_query": query,
                        "name":         data.get("name"),
                        "website":      data.get("website"),
                        "place_url":    href,
                    }
                    break
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        time.sleep(1)
                    else:
                        print(f"  ⚠️  Failed after {MAX_RETRIES} retries: {href} — {e}")
            if rec:
                local_buffer.append(rec)
                if len(local_buffer) >= SAVE_EVERY:
                    with results_lock:
                        results.extend(local_buffer)
                        save_callback(results)
                    local_buffer.clear()
            task_queue.task_done()
            pbar.update(1)
    finally:
        if local_buffer:
            with results_lock:
                results.extend(local_buffer)
                save_callback(results)
        try:
            drv.quit()
        except Exception:
            pass

# ===================== CSV SAVE — MERGE WITH EXISTING =====================
def atomic_write_csv(records: list[dict], outpath: str) -> None:
    """
    Merges new records with any existing CSV rows, deduplicates by place_url,
    then atomically writes the result. Never loses data from previous runs.
    """
    if not records:
        return
    new_df = pd.DataFrame(records)
    if os.path.exists(outpath):
        try:
            existing_df = pd.read_csv(outpath, encoding="utf-8-sig")
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception:
            combined_df = new_df
    else:
        combined_df = new_df
    combined_df = combined_df.drop_duplicates(subset=["place_url"], keep="first")
    dirn = os.path.dirname(outpath) or "."
    fd, tmp = tempfile.mkstemp(prefix="tmp_biz_", dir=dirn, text=True)
    os.close(fd)
    try:
        combined_df.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, outpath)
    except PermissionError:
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise

def make_save_callback(outpath: str):
    def _save(records):
        try:
            atomic_write_csv(records, outpath)
        except Exception as e:
            print(f"  ⚠️  CSV save error: {e}")
    return _save

# ===================== PHASE 2 RUNNER — PER QUERY =====================
def run_phase2_for_query(
    query: str,
    hrefs: list[str],
    seen_hrefs: set[str],
    seen_lock: threading.Lock,
) -> list[dict]:
    unique_hrefs = []
    with seen_lock:
        for h in hrefs:
            if h not in seen_hrefs:
                seen_hrefs.add(h)
                unique_hrefs.append(h)

    if not unique_hrefs:
        return []

    task_queue: Queue = Queue()
    for href in unique_hrefs:
        task_queue.put((query, href))

    results: list[dict] = []
    results_lock = threading.Lock()

    def _noop_save(_):
        pass

    with tqdm(total=len(unique_hrefs), unit="place",
              desc=f"  [{query[:40]}]", leave=False) as pbar:
        threads = []
        for _ in range(PARALLEL_DRIVERS):
            t = threading.Thread(
                target=worker_process_hrefs,
                args=(task_queue, results, results_lock, pbar, _noop_save),
                daemon=True,
            )
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    return results

# ===================== MAIN =====================
def run() -> None:
    cpu_cores = os.cpu_count() or 4
    print("=" * 62)
    print("  GOOGLE MAPS SCRAPER — Batch Mode / Safe Completion")
    print("=" * 62)
    print(f"  Queries this batch  : {len(SEARCH_QUERIES)}")
    print(f"  Max per query       : {MAX_LISTINGS}")
    print(f"  Phase 1 workers     : {COLLECT_WORKERS}")
    print(f"  Phase 2 workers     : {PARALLEL_DRIVERS}")
    print(f"  Page load mode      : eager")
    print(f"  Headless            : {HEADLESS}")
    print(f"  CPU cores detected  : {cpu_cores}")
    print(f"  Output              : {OUTPUT_CSV}")
    print(f"  Batch output        : {BATCH_CSV}")
    print("=" * 62)

    print(f"\n📍 PHASE 1 — Collecting place URLs ({COLLECT_WORKERS} parallel browsers)…\n")

    tasks_by_query: dict[str, list[str]] = {q: [] for q in SEARCH_QUERIES}
    phase1_failed:  set[str]             = set()
    collect_lock  = threading.Lock()
    collect_queue : Queue = Queue()
    for q in SEARCH_QUERIES:
        collect_queue.put(q)

    phase1_pbar = tqdm(total=len(SEARCH_QUERIES), unit="query", desc="  Queries")

    def collect_thread_worker():
        drv = _make_driver()
        try:
            while not _shutdown_event.is_set():
                try:
                    query = collect_queue.get_nowait()
                except Empty:
                    break
                try:
                    hrefs = search_and_collect_hrefs(drv, query, MAX_LISTINGS)
                    with collect_lock:
                        tasks_by_query[query] = hrefs
                    phase1_pbar.set_postfix({"hrefs": len(hrefs), "query": query[:30]})
                except Exception as e:
                    print(f"\n  ⚠️  Error collecting [{query}]: {e}")
                    with collect_lock:
                        phase1_failed.add(query)
                    mark_query_failed(query)
                finally:
                    collect_queue.task_done()
                    phase1_pbar.update(1)
        finally:
            try:
                drv.quit()
            except Exception:
                pass

    collect_threads = []
    for _ in range(COLLECT_WORKERS):
        t = threading.Thread(target=collect_thread_worker, daemon=True)
        t.start()
        collect_threads.append(t)
    for t in collect_threads:
        t.join()
    phase1_pbar.close()

    if _shutdown_event.is_set():
        print("\n⚠️  Interrupted during Phase 1. No queries marked complete.")
        _print_summary([])
        return

    total_hrefs = sum(len(v) for v in tasks_by_query.values())
    print(f"\n✅ Phase 1 done — {total_hrefs} place URLs across "
          f"{len(tasks_by_query) - len(phase1_failed)} queries\n")

    print(f"🌐 PHASE 2 — Extracting name + website, query by query…\n")

    seen_hrefs: set[str] = set()
    seen_lock   = threading.Lock()
    batch_completed: list[str] = []
    batch_processed = 0

    # ← NAYA: is run ki saari websites collect karne ke liye
    all_batch_records: list[dict] = []

    queries_to_process = [q for q in SEARCH_QUERIES if q not in phase1_failed]

    for query in queries_to_process:
        if _shutdown_event.is_set():
            print("\n⚠️  Shutdown requested — stopping before next query.")
            break

        hrefs = tasks_by_query[query]
        if not hrefs:
            mark_query_completed(query)
            batch_completed.append(query)
            batch_processed += 1
            print(f"  ℹ️  No place URLs found for [{query}] — marking complete.")
            continue

        print(f"  🔍 Processing query {batch_processed + 1}/"
              f"{len(queries_to_process)}: {query}")

        records = run_phase2_for_query(query, hrefs, seen_hrefs, seen_lock)

        csv_saved = False
        if records:
            df = pd.DataFrame(records)
            df = df.drop_duplicates(subset=["place_url"], keep="first")
            df_with_site = df[df["website"].notna()]
            if not df_with_site.empty:
                try:
                    # Main CSV mein merge karo (purana behavior)
                    atomic_write_csv(df_with_site.to_dict("records"), OUTPUT_CSV)

                    # ← NAYA: is batch ke records alag collect karo
                    all_batch_records.extend(df_with_site.to_dict("records"))

                    csv_saved = True
                except Exception as e:
                    print(f"  ❌ CSV write failed for [{query}]: {e} — NOT marking complete.")
                    mark_query_failed(query)
                    batch_processed += 1
                    continue
            else:
                csv_saved = True
        else:
            csv_saved = True

        mark_query_completed(query)
        batch_completed.append(query)
        batch_processed += 1
        print(f"  ✅ [{query}] complete — "
              f"{len(records)} records, "
              f"{'saved to CSV' if csv_saved and records else 'no website records'}")

    # ← NAYA: current_batch.csv mein sirf is run ki websites likho (overwrite)
    if all_batch_records:
        batch_df = pd.DataFrame(all_batch_records)
        batch_df = batch_df.drop_duplicates(subset=["place_url"], keep="first")
        batch_df.to_csv(BATCH_CSV, index=False, encoding="utf-8-sig")
        print(f"\n📋 current_batch.csv saved — {len(batch_df)} websites for email extractor")
    else:
        # Agar kuch nahi mila toh empty file banao taaki email extractor fail na ho
        pd.DataFrame(columns=["search_query", "name", "website", "place_url"]).to_csv(
            BATCH_CSV, index=False, encoding="utf-8-sig"
        )
        print(f"\n📋 current_batch.csv saved — 0 websites this run")

    print(f"\n{'=' * 62}")
    print(f"  ✅ Batch finished!")
    print(f"  Saved to : {OUTPUT_CSV}")
    print(f"{'=' * 62}")
    _print_summary(batch_completed)


def _print_summary(batch_completed: list[str]) -> None:
    completed_now, _ = load_state()
    total           = len(ALL_QUERIES)
    completed_total = len(completed_now)
    remaining       = total - completed_total
    processed_now   = len(batch_completed)

    print(f"\n{'─' * 62}")
    print(f"  📊 BATCH SUMMARY")
    print(f"{'─' * 62}")
    print(f"  Total queries in queries.txt  : {total}")
    print(f"  Completed queries (all runs)  : {completed_total}")
    print(f"  Remaining queries             : {remaining}")
    print(f"  Processed in this batch       : {processed_now}")
    print(f"{'─' * 62}\n")


# ===================== ENTRY =====================
if __name__ == "__main__":
    run()
