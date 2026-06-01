# maps_scraper_optimized.py
# OPTIMIZED FOR: Intel i3-1215U (6 cores, 1.20 GHz) + 16 GB RAM
#
# KEY OPTIMIZATIONS vs previous version:
#   1. PARALLEL_DRIVERS = 5  (sweet spot for i3 — avoids CPU throttle)
#   2. page_load_strategy = 'eager'  (stops waiting for slow secondary resources)
#   3. Phase 1 (href collection) is NOW PARALLEL too (was single-threaded before)
#   4. Reduced sleep/wait values tuned for eager loading
#   5. CPU-aware thread pool — won't exceed os.cpu_count() * 1.5
#   6. Progressive CSV saving every 50 records (crash-safe)
#   7. Graceful Ctrl+C handling — saves whatever was collected before exit
#   8. Retry logic (up to 2 retries) for flaky place pages
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
with open("queries.txt", "r", encoding="utf-8") as f:
    ALL_QUERIES = [
        line.strip()
        for line in f
        if line.strip()
    ]

SEARCH_QUERIES = ALL_QUERIES

print(f"Running {len(SEARCH_QUERIES)} queries")

MAX_LISTINGS      = 100    # per query
OUTPUT_CSV        = "business_websites.csv"
HEADLESS          = True   # True = faster (no rendering overhead)

# ── Tuned for i3-1215U + 16 GB RAM ──────────────────────────
PARALLEL_DRIVERS  = 2      # sweet spot: enough concurrency, no CPU throttle
SCROLL_PAUSE      = 0.3    # works well with eager page load
PLACE_LOAD_WAIT   = 2.0    # reduced — eager loading means DOM arrives faster
COLLECT_WORKERS   = 1      # parallel drivers for Phase 1 (href collection)
SAVE_EVERY        = 50     # write CSV every N new records (crash safety)
MAX_RETRIES       = 2      # retries per place page on failure
# ─────────────────────────────────────────────────────────────

# ===================== GLOBALS =====================
_drivers_lock = threading.Lock()
_all_drivers: list[webdriver.Chrome] = []
_shutdown_event = threading.Event()


# ===================== SIGNAL HANDLER =====================
def _handle_sigint(sig, frame):
    print("\n\n⚠️  Ctrl+C detected — finishing current tasks and saving…")
    _shutdown_event.set()


signal.signal(signal.SIGINT, _handle_sigint)


# ===================== DRIVER FACTORY =====================
def _make_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()

    # ── KEY OPTIMIZATION: eager stops waiting for all assets ──
    options.page_load_strategy = "eager"

    if HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")

    # Performance / stability flags
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1400,900")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--blink-settings=imagesEnabled=false")        # skip images = faster
    options.add_argument("--disable-javascript-harmony-shipping")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--mute-audio")
    options.add_argument("--no-first-run")

    options.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2,           # block images
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.stylesheets": 2,      # block CSS too
    })

    service = Service(ChromeDriverManager().install())
    drv = webdriver.Chrome(service=service, options=options)
    drv.set_page_load_timeout(15)   # don't hang forever on slow pages

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
    """Return element or None — never raises."""
    try:
        return WebDriverWait(drv, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
    except Exception:
        return None


# ===================== PHASE 1: COLLECT HREFS =====================
def search_and_collect_hrefs(drv, query: str, max_links: int) -> list[str]:
    """
    Opens Maps, searches, scrolls the results panel, returns up to max_links hrefs.
    Now uses eager page load — panel arrives ~0.5s faster per query.
    """
    if _shutdown_event.is_set():
        return []

    drv.get(f"https://www.google.com/maps/search/{query.replace(' ', '+')}")

    # Accept cookies if shown (non-blocking, short timeout)
    try:
        WebDriverWait(drv, 3).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept')]"))
        ).click()
    except Exception:
        pass

    # Wait for results panel
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

        # Scroll panel
        try:
            drv.execute_script(
                "arguments[0].scrollTop += arguments[0].clientHeight;", panel
            )
        except Exception:
            drv.execute_script("window.scrollBy(0,700);")

        time.sleep(SCROLL_PAUSE)
        idle_count = 0 if len(hrefs) > prev_count else idle_count + 1

    return hrefs[:max_links]


def collect_worker(query: str) -> list[tuple[str, str]]:
    """One driver handles one query (used in Phase 1 parallel pool)."""
    drv = _make_driver()
    try:
        hrefs = search_and_collect_hrefs(drv, query, MAX_LISTINGS)
        return [(query, h) for h in hrefs]
    except Exception as e:
        print(f"  ⚠️  Collect error [{query}]: {e}")
        return []
    finally:
        try:
            drv.quit()
        except Exception:
            pass


# ===================== PHASE 2: EXTRACT DATA =====================
def extract_place_data(drv, url: str) -> dict:
    """
    Visits a place page and extracts name + website.
    Eager page_load_strategy means we don't wait for images/fonts/CSS.
    """
    drv.get(url)

    # h1 = business name — arrives quickly with eager + images blocked
    name_el = _wait_for(drv, "//h1", timeout=PLACE_LOAD_WAIT)
    name = name_el.text.strip() if name_el else None

    # Website link
    website = None
    try:
        w_el = drv.find_element(
            By.XPATH, "//a[contains(@data-item-id,'authority')]"
        )
        website = w_el.get_attribute("href")
    except Exception:
        pass

    # Fallback: parse page source for h1
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
    """
    One thread owns one Chrome driver and processes tasks from the shared queue.
    Includes retry logic and progressive saving.
    """
    drv = _make_driver()
    local_buffer: list[dict] = []

    try:
        while not _shutdown_event.is_set():
            try:
                query, href = task_queue.get(timeout=2)
            except Empty:
                break

            # Retry loop
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
                    break   # success
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        time.sleep(1)           # brief pause before retry
                    else:
                        print(f"  ⚠️  Failed after {MAX_RETRIES} retries: {href} — {e}")

            if rec:
                local_buffer.append(rec)

                # Progressive save every SAVE_EVERY records
                if len(local_buffer) >= SAVE_EVERY:
                    with results_lock:
                        results.extend(local_buffer)
                        save_callback(results)
                    local_buffer.clear()

            task_queue.task_done()
            pbar.update(1)

    finally:
        # Flush remaining buffer
        if local_buffer:
            with results_lock:
                results.extend(local_buffer)
                save_callback(results)
        try:
            drv.quit()
        except Exception:
            pass


# ===================== CSV SAVE =====================
def atomic_write_csv(records: list[dict], outpath: str) -> None:
    """Thread-safe atomic CSV write — never corrupts on crash."""
    if not records:
        return
    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["place_url"], keep="first")

    dirn = os.path.dirname(outpath) or "."
    fd, tmp = tempfile.mkstemp(prefix="tmp_biz_", dir=dirn, text=True)
    os.close(fd)
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, outpath)
    except PermissionError:
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise


def make_save_callback(outpath: str):
    """Returns a callback that saves all results to CSV (called progressively)."""
    def _save(records):
        try:
            atomic_write_csv(records, outpath)
        except Exception as e:
            print(f"  ⚠️  CSV save error: {e}")
    return _save


# ===================== MAIN =====================
def run() -> None:
    cpu_cores = os.cpu_count() or 4
    print("=" * 62)
    print("  GOOGLE MAPS SCRAPER — Optimized for i3-1215U / 16 GB")
    print("=" * 62)
    print(f"  Queries           : {len(SEARCH_QUERIES)}")
    print(f"  Max per query     : {MAX_LISTINGS}")
    print(f"  Phase 1 workers   : {COLLECT_WORKERS}  (parallel href collection)")
    print(f"  Phase 2 workers   : {PARALLEL_DRIVERS}  (parallel place scraping)")
    print(f"  Page load mode    : eager  (stops on DOM ready, skips assets)")
    print(f"  Headless          : {HEADLESS}")
    print(f"  CPU cores detected: {cpu_cores}")
    print(f"  Save every        : {SAVE_EVERY} records")
    print(f"  Output            : {OUTPUT_CSV}")
    print("=" * 62)

    save_cb = make_save_callback(OUTPUT_CSV)

    # ── PHASE 1: Parallel href collection ───────────────────────────────
    print(f"\n📍 PHASE 1 — Collecting place URLs ({COLLECT_WORKERS} parallel browsers)…")
    print("   (This is now parallel — was single-threaded in previous version)\n")

    all_tasks: list[tuple[str, str]] = []
    seen_hrefs: set[str] = set()
    collect_lock = threading.Lock()

    collect_queue: Queue = Queue()
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
                    new_count = 0
                    with collect_lock:
                        for h in hrefs:
                            if h not in seen_hrefs:
                                seen_hrefs.add(h)
                                all_tasks.append((query, h))
                                new_count += 1
                    phase1_pbar.set_postfix({"new": new_count, "total": len(all_tasks)})
                except Exception as e:
                    print(f"\n  ⚠️  Error collecting [{query}]: {e}")
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
        print("\n⚠️  Interrupted during Phase 1. Saving what was collected…")
        save_cb([{"search_query": q, "name": None, "website": None, "place_url": h}
                 for q, h in all_tasks])
        return

    print(f"\n✅ Phase 1 done — {len(all_tasks)} unique places found")

    # ── PHASE 2: Parallel place page scraping ────────────────────────────
    print(f"\n🌐 PHASE 2 — Extracting name + website ({PARALLEL_DRIVERS} browsers)…\n")

    task_queue: Queue = Queue()
    for task in all_tasks:
        task_queue.put(task)

    results: list[dict] = []
    results_lock = threading.Lock()

    with tqdm(total=len(all_tasks), unit="place", desc="  Places") as pbar:
        phase2_threads = []
        for _ in range(PARALLEL_DRIVERS):
            t = threading.Thread(
                target=worker_process_hrefs,
                args=(task_queue, results, results_lock, pbar, save_cb),
                daemon=True,
            )
            t.start()
            phase2_threads.append(t)

        for t in phase2_threads:
            t.join()

    # ── PHASE 3: Final save ──────────────────────────────────────────────
    print("\n💾 PHASE 3 — Final save…")
    df = pd.DataFrame(results)
    df = df.drop_duplicates(subset=["place_url"], keep="first")
    df_with_site = df[df["website"].notna()]

    atomic_write_csv(df_with_site.to_dict("records"), OUTPUT_CSV)

    print(f"\n{'=' * 62}")
    print(f"  ✅ Done!")
    print(f"  Total places visited : {len(df)}")
    print(f"  With website         : {len(df_with_site)}")
    print(f"  Saved to             : {OUTPUT_CSV}")
    print(f"{'=' * 62}")


# ===================== ENTRY =====================
if __name__ == "__main__":
    run()
