"""
Pinterest Ultra-Reliable Scraper - 50k Pins Edition
===================================================
Ye script maximum reliability aur speed ke liye design ki gayi hai. 
Ye boards aur individual pins dono ko handle karti hai.

Key Features:
- Separate Files: Har profile ka data alag CSV file mein save hota.
- Duplicate Removal: Overall unique titles rakhe jate hain (first occurrence).
- Multi-URL Support: GitHub Actions se multiple URLs handle kar sakti hai.
"""

import asyncio
import csv
import re
import html
from playwright.async_api import async_playwright
from datetime import datetime
import os
import sys

# --- CONFIGURATION ---
DEFAULT_URL = "https://www.pinterest.com/amiller1119/_created"
env_urls = os.getenv("PROFILE_URLS", DEFAULT_URL)
PROFILE_URLS = [url.strip() for url in env_urls.split(",") if url.strip()]

OUTPUT_DIR = "output"

# BATCH SETTINGS
TARGET_COUNT = 50000      # 50k Target
CONCURRENT_TASKS = 40    # High concurrency for speed

# PERFORMANCE SETTINGS
HEADLESS = True
MAX_RETRIES = 3
SCROLL_DELAY = 2.0       # Stable scrolling
MAX_SCROLL_ATTEMPTS = 20000
NO_NEW_PINS_WAIT = 15    # Wait longer for large profiles
# ---------------------

# Global state for current profile
results = []
seen_urls = set()
seen_titles_for_profile = set() # To track unique titles per profile
queue = asyncio.Queue()
processed_count = 0
extraction_done = asyncio.Event()
results_lock = asyncio.Lock()

def get_filename_from_url(url):
    """Extracts a clean filename from the Pinterest URL."""
    clean_url = url.split("?")[0].rstrip("/")
    parts = clean_url.split("/")
    if len(parts) >= 4:
        name = f"{parts[3]}_{parts[4]}" if len(parts) > 4 else parts[3]
        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        return f"pinterest_{name}.csv"
    return "pinterest_data.csv"

async def get_pin_details_worker(browser, semaphore):
    global processed_count, results, seen_urls, seen_titles_for_profile
    while True:
        try:
            url = await asyncio.wait_for(queue.get(), timeout=5)
        except asyncio.TimeoutError:
            if extraction_done.is_set():
                break
            continue
        
        async with semaphore:
            retry_count = 0
            success = False
            while retry_count <= MAX_RETRIES and not success:
                context = None
                try:
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                    page = await context.new_page()
                    await page.route("**/*.{png,jpg,jpeg,gif,svg,css,woff,woff2}", lambda route: route.abort())
                    
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(1)
                    
                    content = await page.content()
                    
                    title = "N/A"
                    
                    # 1. Prioritize og:title
                    og_title_match = re.search(r'<meta property="og:title" content="(.*?)">', content)
                    if og_title_match:
                        extracted_title = og_title_match.group(1).replace(' - Pinterest', '').strip()
                        if extracted_title and not extracted_title.lower().startswith("pin on "):
                            title = extracted_title
                    
                    # 2. Fallback to <meta name="title">
                    if title == "N/A" or title.lower().startswith("pin on "):
                        meta_title_match = re.search(r'<meta name="title" content="(.*?)">', content)
                        if meta_title_match:
                            extracted_title = meta_title_match.group(1).strip()
                            if extracted_title and not extracted_title.lower().startswith("pin on "):
                                title = extracted_title

                    # 3. Fallback to <h1>
                    if title == "N/A" or title.lower().startswith("pin on "):
                        h1_title_match = re.search(r'<h1[^>]*>(.*?)</h1>', content, re.IGNORECASE | re.DOTALL)
                        if h1_title_match:
                            extracted_title = re.sub('<[^<]+?>', '', h1_title_match.group(1)).strip()
                            if extracted_title and not extracted_title.lower().startswith("pin on "):
                                title = extracted_title
                    
                    # 4. Fallback to <title>
                    if title == "N/A" or title.lower().startswith("pin on "):
                        page_title_match = re.search(r'<title>(.*?)</title>', content, re.IGNORECASE | re.DOTALL)
                        if page_title_match:
                            extracted_title = page_title_match.group(1).replace(' - Pinterest', '').strip()
                            if extracted_title and not extracted_title.lower().startswith("pin on "):
                                title = extracted_title

                    # If still a generic title, try to extract something more meaningful from the URL or content
                    if title == "N/A" or title.lower().startswith("pin on "):
                        # Attempt to get a more descriptive title from the URL path if available
                        url_parts = url.split('/')
                        if len(url_parts) > 4 and url_parts[3] == 'pin':
                            potential_title = url_parts[4].replace('-', ' ').replace('_', ' ').strip()
                            if potential_title:
                                title = potential_title
                        
                    # Decode HTML entities (e.g., &amp; -> &)
                    if title:
                        title = html.unescape(title)

                    patterns = [
                        r'"saves"[:\s]+(\d+)',
                        r'"aggregated_pin_data"[^}]*"saves"[:\s]+(\d+)',
                        r'"save_count"[:\s]+(\d+)',
                        r'saves["\s:]+(\d+)',
                        r'(\d+)\s+saves',
                        r'"repinCount"[:\s]+(\d+)',
                    ]
                    saves_count = 0
                    for pattern in patterns:
                        matches = re.findall(pattern, content, re.IGNORECASE)
                        if matches:
                            saves_count = max([int(m) for m in matches])
                            break
                    
                    async with results_lock:
                        # Check for URL uniqueness first
                        if url not in seen_urls:
                            seen_urls.add(url)
                            # Then check for title uniqueness for the current profile
                            if title not in seen_titles_for_profile:
                                seen_titles_for_profile.add(title)
                                results.append({
                                    'url': url,
                                    'title': title[:200] if title else "N/A",
                                    'saves': saves_count,
                                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                })
                                processed_count += 1
                                if processed_count % 5 == 0:
                                    print(f"   ⚡ Processed {processed_count} unique pins... (Queue: {queue.qsize()})", end='\r')
                    
                    success = True
                except Exception:
                    retry_count += 1
                    if retry_count > MAX_RETRIES:
                        pass 
                    else:
                        await asyncio.sleep(2)
                finally:
                    if context: await context.close()
        
        queue.task_done()

async def scrape_single_profile(browser, profile_url, semaphore):
    """Scrapes a single profile and saves its results."""
    global results, seen_urls, processed_count, extraction_done, seen_titles_for_profile
    
    # Reset state for new profile
    results = []
    seen_urls = set()
    seen_titles_for_profile = set() # Reset for each profile
    processed_count = 0
    extraction_done = asyncio.Event()
    while not queue.empty(): queue.get_nowait()

    print(f"\n🚀 Starting Collection for: {profile_url}")
    
    # Start workers for this profile
    workers = [asyncio.create_task(get_pin_details_worker(browser, semaphore)) for _ in range(CONCURRENT_TASKS)]
    
    # URL Collection
    page = await browser.new_page()
    try:
        await page.goto(profile_url, wait_until="networkidle", timeout=60000)
        scrolls = 0
        no_new_items_count = 0
        previous_total = 0
        
        while scrolls < MAX_SCROLL_ATTEMPTS and len(seen_urls) < TARGET_COUNT:
            links = await page.query_selector_all('a[href*="/pin/"]')
            for link in links:
                href = await link.get_attribute('href')
                if href:
                    url = f"https://www.pinterest.com{href.split('?')[0]}" if href.startswith('/') else href.split('?')[0]
                    if url not in seen_urls:
                        await queue.put(url)
            
            if queue.qsize() >= TARGET_COUNT: break
            if queue.qsize() == previous_total:
                no_new_items_count += 1
                if no_new_items_count >= NO_NEW_PINS_WAIT:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(5)
                    if queue.qsize() == previous_total: break
            else:
                no_new_items_count = 0
            
            previous_total = queue.qsize()
            print(f"   🔍 Found {queue.qsize()} pins... (Queue size: {queue.qsize()})", end='\r')
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(SCROLL_DELAY)
            scrolls += 1
    except Exception as e:
        print(f"\n❌ Error collecting from {profile_url}: {e}")
    finally:
        await page.close()
        extraction_done.set()

    # Wait for workers to finish
    await queue.join()
    for w in workers: w.cancel()

    # Save results for this profile
    if results:
        filename = get_filename_from_url(profile_url)
        filepath = os.path.join(OUTPUT_DIR, filename)
        # No sorting by saves, just output as collected (which is effectively by URL first seen)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['url', 'title', 'saves', 'timestamp'])
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✅ Saved {len(results)} unique title pins to {filename}")

async def main():
    start_time = datetime.now()
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print(f"📋 Profiles to scrape: {len(PROFILE_URLS)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        semaphore = asyncio.Semaphore(CONCURRENT_TASKS)
        
        for url in PROFILE_URLS:
            await scrape_single_profile(browser, url, semaphore)
        
        await browser.close()

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n\n{'='*70}")
    print(f"✅ ALL PROFILES COMPLETE!")
    print(f"⏱️  Total Time: {elapsed/60:.1f} minutes")
    print(f"📁 Output Directory: {OUTPUT_DIR}")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    asyncio.run(main())
