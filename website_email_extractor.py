import re
import time
import os
import pandas as pd
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

# ================= CONFIG =================
INPUT_CSV  = "business_emails.csv"   # ← SAHI HAI
OUTPUT_CSV = "current_batch.csv" 
TIMEOUT    = 3
DELAY      = 0.5

EXCLUDED_DOMAINS = [
    "prada.com", "gucci.com", "zara.com",
    "nike.com", "apple.com", "amazon.com"
]

EMAIL_RE = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.I)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

def normalize_url(url):
    if not url:
        return None
    url = str(url).strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url

def clean_email(email):
    if not email:
        return None
    email = email.strip().lower()
    email = email.replace("mailto:", "")
    email = email.strip(".,:;!?()[]<> ")
    return email

def extract_emails_from_html(html):
    found = set()
    for m in EMAIL_RE.findall(html):
        email = clean_email(m)
        if email:
            found.add(email)
    return list(found)

def fetch_page(url):
    try:
        response = requests.get(
            url, headers=HEADERS, timeout=TIMEOUT,
            allow_redirects=True, verify=False
        )
        if response.status_code == 200:
            return response.text
    except Exception as e:
        print("ERROR:", url, e)
    return None

def extract_website_emails(base_url):
    if not base_url:
        return []
    if any(domain in base_url for domain in EXCLUDED_DOMAINS):
        print("SKIPPED BIG WEBSITE")
        return []

    pages_to_try = ["", "/contact", "/contact-us", "/about", "/about-us", "/support", "/team"]
    all_emails = set()

    for path in pages_to_try:
        try:
            full_url = urljoin(base_url, path)
            html = fetch_page(full_url)
            if not html:
                continue
            for e in extract_emails_from_html(html):
                all_emails.add(e)
            soup = BeautifulSoup(html, "lxml")
            for link in soup.select('a[href^="mailto:"]'):
                href = link.get("href")
                if href:
                    email = clean_email(href)
                    if email:
                        all_emails.add(email)
        except Exception as e:
            print("PAGE ERROR:", e)
            continue

    return list(all_emails)

def run():
    if not os.path.exists(INPUT_CSV):
        print(f"No {INPUT_CSV} found, skipping.")
        return

    df = pd.read_csv(INPUT_CSV)
    df = df[df["website"].notna()]

    if df.empty:
        print("No websites in current batch, skipping.")
        return

    print(f"[INFO] Processing {len(df)} websites from current batch")

    results = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        name    = row.get("name")
        website = normalize_url(row.get("website"))

        if not website:
            continue

        print(f"\nChecking: {website}")
        emails = extract_website_emails(website)
        if emails:
            for email in emails:
                results.append({"name": name, "website": website, "email": email})
                print("FOUND:", email)
        else:
            results.append({"name": name, "website": website, "email": None})
            print("No email found")
        time.sleep(DELAY)

    if not results:
        print("No emails found in this batch.")
        return

    new_df = pd.DataFrame(results)
    if os.path.exists(OUTPUT_CSV):
        try:
            existing_df = pd.read_csv(OUTPUT_CSV, encoding="utf-8-sig")
            new_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception:
            pass

    new_df = new_df.drop_duplicates()
    new_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\nDONE — Saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    run()
