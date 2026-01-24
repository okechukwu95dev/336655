import requests
import json
import time
import concurrent.futures
import os
import argparse

# Config
# In GitHub Actions, cookies come from the SOFA_COOKIES secret, which we dump to a file or read from env
OUTPUT_FILE = "sofascore_leagues.json"

def load_session(cookie_source):
    """
    Loads cookies from:
    1. A JSON file path (if arg provided)
    2. An Environment Variable (SOFA_COOKIES)
    """
    cookies = {}
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
    
    # Priority 1: File Path
    if cookie_source and os.path.exists(cookie_source):
        print(f"--- [Worker] Loading cookies from file: {cookie_source}")
        with open(cookie_source, 'r') as f:
            data = json.load(f)
            cookies = data.get('cookies', {})
            user_agent = data.get('user_agent', user_agent)
            
    # Priority 2: Env Var (GitHub Secrets)
    elif os.environ.get("SOFA_COOKIES"):
        print("--- [Worker] Loading cookies from ENV 'SOFA_COOKIES'")
        try:
            env_data = json.loads(os.environ["SOFA_COOKIES"])
            cookies = env_data.get('cookies', {})
            user_agent = env_data.get('user_agent', user_agent)
        except json.JSONDecodeError:
            print("--- [Worker] Error: SOFA_COOKIES env var is not valid JSON.")
            return None, None
    else:
        print("--- [Worker] Error: No cookies found. Provide --cookies FILE or set SOFA_COOKIES env.")
        return None, None
        
    return cookies, user_agent

def fetch_url(url, cookies, user_agent):
    """Generic fetcher with error handling."""
    headers = {
        "User-Agent": user_agent,
        "Accept": "*/*",
        "Referer": "https://www.sofascore.com/",
        "Connection": "keep-alive"
    }
    
    try:
        resp = requests.get(url, headers=headers, cookies=cookies, timeout=15)
        return resp
    except Exception as e:
        print(f"--- [Worker] Network Error: {e}")
        return None

def scrape_category(category, cookies, user_agent):
    """Worker task: Scrape a single category (Country)."""
    cat_name = category.get('name')
    cat_id = category.get('id')
    
    url = f"https://www.sofascore.com/api/v1/category/{cat_id}/unique-tournaments"
    resp = fetch_url(url, cookies, user_agent)
    
    leagues_data = []
    
    if resp and resp.status_code == 200:
        data = resp.json()
        
        # Combined parsing logic (Direct + Groups)
        raw_list = data.get('uniqueTournaments', [])
        if 'groups' in data:
            for g in data['groups']:
                raw_list.extend(g.get('uniqueTournaments', []))
                
        # Simplify data for output
        for l in raw_list:
            leagues_data.append({
                "id": l.get('id'),
                "name": l.get('name'),
                "slug": l.get('slug'),
                "userCount": l.get('userCount'),
                "uniqueTournamentId": l.get('id'), # Explicit for mapping
                "country": {
                     "id": cat_id,
                     "name": cat_name
                }
            })
            
    else:
         status = resp.status_code if resp else "ERR"
         # print(f"--- [Worker] Failed to fetch {cat_name}: {status}")

    return leagues_data

def run_worker(cookies_path=None, threads=10):
    print(f"--- [Worker] Starting SofaScore League Scraper (Threads: {threads}) ---")
    
    cookies, ua = load_session(cookies_path)
    if not cookies:
        return

    # 1. Fetch Categories (The Metadata)
    print("--- [Worker] Fetching Category List...")
    cat_url = "https://www.sofascore.com/api/v1/sport/football/categories"
    resp = fetch_url(cat_url, cookies, ua)
    
    if not resp or resp.status_code != 200:
        print(f"--- [Worker] Critical: Failed to fetch categories. Status: {resp.status_code if resp else 'None'}")
        return
        
    all_categories = resp.json().get('categories', [])
    print(f"--- [Worker] Found {len(all_categories)} categories. Scraping ALL...")
    
    all_leagues = []
    
    # 2. Parallel Execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_cat = {executor.submit(scrape_category, cat, cookies, ua): cat for cat in all_categories}
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_cat):
            cat_leagues = future.result()
            all_leagues.extend(cat_leagues)
            completed += 1
            if completed % 20 == 0:
                print(f"   -> Progress: {completed}/{len(all_categories)} countries processed...")
            
    # 3. Save
    print(f"--- [Worker] Done. Fetched {len(all_leagues)} unique leagues.")
    print(f"--- [Worker] Saving to {OUTPUT_FILE}")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_leagues, f, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cookies", help="Path to cookies.json file", default=None)
    args = parser.parse_args()
    
    run_worker(cookies_path=args.cookies)
