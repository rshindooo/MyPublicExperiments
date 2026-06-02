import os
import re
import sys
import time
import queue
import threading
import atexit
from pathlib import Path
from urllib.parse import quote_plus
import psutil
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def load_env_file(env_path):
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export "):].strip()

        key, separator, value = line.partition("=")
        if not separator:
            continue

        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]

        os.environ.setdefault(key, value)


def env_string(name, default):
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def env_int(name, default):
    value = env_string(name, str(default))
    try:
        parsed_value = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None

    if parsed_value < 1:
        raise ValueError(f"{name} must be at least 1, got {parsed_value}")

    return parsed_value


# *** Configuration ***
SCRIPT_DIR = Path(__file__).resolve().parent
load_env_file(SCRIPT_DIR / ".env")

TARGET_BASE_URL = env_string("TARGET_BASE_URL", "https://www.target.com").rstrip("/")
SEARCH_QUERY = env_string("SEARCH_QUERY", "pizza")
NUM_CONSUMER_THREADS = env_int("NUM_CONSUMER_THREADS", 24)
OUTPUT_FILE = SCRIPT_DIR / "results.txt"
WAIT_TIMEOUT = 8
SEARCH_SCROLL_PAUSE = 0.65
PRODUCT_SCROLL_PAUSE = 0.65
PAGINATION_PAUSE = 3
QUEUE_TIMEOUT = 1.5
SAVE_EVERY = 40

# *** Shared Resources ***
url_queue = queue.Queue()
master_mapping = {}
mapping_lock = threading.Lock()
save_lock = threading.Lock()
save_counter_lock = threading.Lock()
results_since_last_save = 0
seen_urls = set()

# Thread-safe progress tracking
tasks_completed = 0
tasks_lock = threading.Lock()
producer_finished = False

shutdown_flag = threading.Event()

# Runtime measurement starts when the first consumer begins scraping a product.
timer_lock = threading.Lock()
timer_start = None
process = psutil.Process()
peak_rss_bytes = process.memory_info().rss
peak_total_tree_rss_bytes = peak_rss_bytes
peak_total_tree_cpu_seconds = sum(process.cpu_times()[:2])
resource_lock = threading.Lock()

def bytes_to_gb(num_bytes):
    return num_bytes / (1024 ** 3)

def format_duration(seconds):
    minutes, secs = divmod(seconds, 60)
    hours, mins = divmod(int(minutes), 60)
    if hours:
        return f"{hours}h {mins}m {secs:.2f}s"
    if mins:
        return f"{mins}m {secs:.2f}s"
    return f"{secs:.2f}s"

def start_timer_if_needed(thread_id):
    global timer_start
    with timer_lock:
        if timer_start is None:
            timer_start = time.perf_counter()
            print(f"[Timer] Started when Thread-{thread_id} began scraping its first product.")

def elapsed_since_first_consumer():
    with timer_lock:
        if timer_start is None:
            return None
        return time.perf_counter() - timer_start

def print_final_timing():
    elapsed = elapsed_since_first_consumer()
    if elapsed is None:
        print("[Timer] No consumer scraped a product, so no timed run was recorded.")
        return
    print(f"[Timer] Final elapsed time since first consumer scrape: {format_duration(elapsed)}")

def sample_resource_usage():
    global peak_rss_bytes, peak_total_tree_rss_bytes, peak_total_tree_cpu_seconds
    with resource_lock:
        current_rss = process.memory_info().rss
        total_tree_rss = current_rss
        total_tree_cpu_seconds = sum(process.cpu_times()[:2])

        for child in process.children(recursive=True):
            try:
                total_tree_rss += child.memory_info().rss
                total_tree_cpu_seconds += sum(child.cpu_times()[:2])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        peak_rss_bytes = max(peak_rss_bytes, current_rss)
        peak_total_tree_rss_bytes = max(peak_total_tree_rss_bytes, total_tree_rss)
        peak_total_tree_cpu_seconds = max(peak_total_tree_cpu_seconds, total_tree_cpu_seconds)

        return (
            current_rss,
            total_tree_rss,
            peak_rss_bytes,
            peak_total_tree_rss_bytes,
            total_tree_cpu_seconds,
            peak_total_tree_cpu_seconds,
        )

def print_resource_summary():
    current_rss, total_tree_rss, peak_rss, peak_tree_rss, tree_cpu_seconds, peak_tree_cpu_seconds = sample_resource_usage()
    elapsed = elapsed_since_first_consumer()
    avg_tree_cpu = (peak_tree_cpu_seconds / elapsed * 100) if elapsed else 0
    cpu_seconds = sum(process.cpu_times()[:2])
    print(
        "[Resources] "
        f"Python RAM current/peak: {bytes_to_gb(current_rss):.2f}/{bytes_to_gb(peak_rss):.2f} GB | "
        f"Process tree RAM current/peak: {bytes_to_gb(total_tree_rss):.2f}/{bytes_to_gb(peak_tree_rss):.2f} GB | "
        f"Python CPU time: {cpu_seconds:.2f}s | "
        f"Process tree CPU time current/peak: {tree_cpu_seconds:.2f}/{peak_tree_cpu_seconds:.2f}s | "
        f"Avg process tree CPU: {avg_tree_cpu:.1f}%"
    )

def print_final_report():
    print_final_timing()
    print_resource_summary()

def get_optimized_driver():
    """Generates a stripped-down Chrome driver built for speed."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    chrome_options.page_load_strategy = 'eager'
    
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.media_stream": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    return webdriver.Chrome(options=chrome_options)

def search_producer(query):
    global producer_finished
    driver = get_optimized_driver()
    search_url = f"{TARGET_BASE_URL}/s?searchTerm={quote_plus(query)}"
    
    try:
        driver.get(search_url)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        page_number = 1
        
        while not shutdown_flag.is_set():
            print(f"\n[Producer] Scanning search page {page_number}...")
            
            try:
                wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/p/')]")))
            except TimeoutException:
                print("[Producer] No products found on this page. Ending pagination.")
                break

            for i in range(1, 8):
                if shutdown_flag.is_set():
                    break
                driver.execute_script(f"window.scrollTo(0, {i * 1200});")
                time.sleep(SEARCH_SCROLL_PAUSE)

            elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/p/')]")
            for element in elements:
                if shutdown_flag.is_set():
                    break
                href = element.get_attribute("href")
                if href and "target.com/p/" in href:
                    clean_url = href.split("#")[0].split("?")[0]
                    
                    if clean_url not in seen_urls:
                        seen_urls.add(clean_url)
                        url_queue.put(clean_url)

            print(f"[Producer] Queued {len(seen_urls)} unique items so far.")

            if shutdown_flag.is_set():
                break

            # Pagination logic
            try:
                next_button_xpath = "/html/body/div[1]/div[2]/main/div/div[2]/div/div/div[3]/div/div/div[9]/div/div/div/button[2]"
                next_button = driver.find_element(By.XPATH, next_button_xpath)
                
                if next_button.get_attribute("disabled"):
                    print("[Producer] Next button is disabled. Reached the last page.")
                    break
                
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(SEARCH_SCROLL_PAUSE)
                driver.execute_script("arguments[0].click();", next_button)
                
                time.sleep(PAGINATION_PAUSE)
                page_number += 1

            except (NoSuchElementException, TimeoutException):
                print("[Producer] Could not find the Next button. Ending pagination.")
                break
            except Exception as e:
                print(f"[Producer] Pagination error: {e}")
                break

    except Exception as e:
        if not shutdown_flag.is_set():
            print(f"[Producer] Critical Error: {e}")
            
    finally:
        producer_finished = True
        driver.quit()
        for _ in range(NUM_CONSUMER_THREADS):
            url_queue.put(None)

def product_consumer(thread_id):
    global tasks_completed, results_since_last_save
    driver = get_optimized_driver()
    xpath_names = [
        "/html/body/div[1]/div[2]/main/div/div[1]/div[2]/div[2]/div/div/div[4]/h1",
        "/html/body/div[1]/div[2]/main/div/div[1]/div[2]/div[2]/div/div/div[3]/h1",
    ]

    try:
        while not shutdown_flag.is_set():
            try:
                url = url_queue.get(timeout=QUEUE_TIMEOUT)
            except queue.Empty:
                continue
            
            if url is None:
                url_queue.task_done()
                break

            start_timer_if_needed(thread_id)
            
            with tasks_lock:
                current_total = len(seen_urls)
                progress_str = f"[{tasks_completed + 1}/{current_total if producer_finished else '?'}]"
            
            print(f"{progress_str} [Thread-{thread_id}] Scraping: {url.split('/')[-1]}")
            
            item_name = "Unknown Product"
            rating_percentage = None
            total_recommendations = None

            try:
                driver.get(url)
                wait = WebDriverWait(driver, WAIT_TIMEOUT)

                # 1. Grab Title
                for xpath_name in xpath_names:
                    try:
                        name_element = wait.until(EC.presence_of_element_located((By.XPATH, xpath_name)))
                        item_name = name_element.text
                        if item_name:
                            break
                    except TimeoutException:
                        continue

                # 2. Scroll to trigger the reviews network request
                for i in range(1, 5):
                    if shutdown_flag.is_set(): break
                    driver.execute_script(f"window.scrollTo(0, {i * 1000});")
                    time.sleep(PRODUCT_SCROLL_PAUSE) 

                if shutdown_flag.is_set(): break

                # 3. Explicitly wait for the reviews to finish loading into the DOM
                try:
                    wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'would recommend')]")))
                except TimeoutException:
                    pass

                # 4. Read visible page text directly from the browser DOM.
                text_dump = driver.execute_script(
                    "return document.body.innerText || document.body.textContent || '';"
                )

                target_pattern = re.compile(r'(\d+)\s*%\s*would recommend.{0,150}?(\d+)\s*recommendations', re.IGNORECASE | re.DOTALL)
                match = target_pattern.search(text_dump)

                if match:
                    rating_percentage = match.group(1)
                    total_recommendations = match.group(2)

                with mapping_lock:
                    master_mapping[item_name] = {
                        "percentage": rating_percentage,
                        "count": total_recommendations
                    }
                sample_resource_usage()
                with save_counter_lock:
                    results_since_last_save += 1
                    should_save = results_since_last_save >= SAVE_EVERY
                    if should_save:
                        results_since_last_save = 0

                if should_save:
                    save_current_results(reason=f"{SAVE_EVERY} results scraped")

            except Exception as e:
                if not shutdown_flag.is_set():
                    print(f"[Error on {url.split('/')[-1]}]: {e}")
            finally:
                with tasks_lock:
                    tasks_completed += 1
                url_queue.task_done()

    finally:
        driver.quit()

def save_current_results(reason="manual"):
    with mapping_lock:
        snapshot = dict(master_mapping)
    sort_and_save_results(snapshot, reason=reason)

def sort_and_save_results(results, reason="manual"):
    def sort_key(item):
        data = item[1]
        rating = data["percentage"]
        if rating is not None and rating.isdigit():
            return int(rating)
        return -1  

    sorted_items = sorted(results.items(), key=sort_key, reverse=True)

    output_lines = [
        "=" * 80,
        f"FINAL RESULTS FOR: '{SEARCH_QUERY}'",
        "=" * 80,
    ]

    for name, data in sorted_items:
        perc = data["percentage"]
        count = data["count"]

        display_rating = f"{perc}%" if perc else "N/A"
        display_count = f"{count} recs" if count else "N/A"

        output_lines.append(f"[{display_rating:>4} | {display_count:>8}] {name}")

    with save_lock:
        with OUTPUT_FILE.open("w", encoding="utf-8") as results_file:
            results_file.write("\n".join(output_lines) + "\n")

    print(f"Results saved to {OUTPUT_FILE} ({reason})")

if __name__ == "__main__":
    atexit.register(print_final_report)
    print(f"Starting Highly Optimized Scraper for: '{SEARCH_QUERY}'")
    
    consumers = []
    for i in range(NUM_CONSUMER_THREADS):
        t = threading.Thread(target=product_consumer, args=(i+1,))
        t.start()
        consumers.append(t)

    producer = threading.Thread(target=search_producer, args=(SEARCH_QUERY,))
    producer.start()

    try:
        while producer.is_alive():
            producer.join(0.5)

        for c in consumers:
            while c.is_alive():
                c.join(0.5)

        if not shutdown_flag.is_set():
            save_current_results(reason="final")

    except KeyboardInterrupt:
        print("\n[!] Ctrl+C detected! Sending kill signal to all threads...")
        shutdown_flag.set()
        
        print("[!] Waiting for browsers to close cleanly...")
        for c in consumers:
            c.join(timeout=3)
        producer.join(timeout=3)
        
        save_current_results(reason="interrupt")
        print("[!] Partial results saved. Exiting script.")
        sys.exit(1)
