import time
import json
import os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import platform

# Config
# In CI, we output to a shared file that the next step can read
COOKIE_FILE = "cookies.json"

def get_auth_session():
    """
    Launches a headless browser to get cookies.
    Adapted for running in GitHub Actions (Linux).
    """
    print("--- [Auth Service] Starting Browser to Mint New Key ---")
    
    options = uc.ChromeOptions()
    options.add_argument("--headless=new") # Modern headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Check OS to decide on version enforcement
    # On Windows (Local), we might need specific version. On Linux (CI), let UC decide or use installed.
    is_windows = platform.system() == "Windows"
    
    print(f"--- [Auth Service] OS: {platform.system()}")
    
    try:
        if is_windows:
            # Local Dev (Windows) usually matches installed Chrome
            driver = uc.Chrome(options=options, version_main=143)
        else:
            # CI (Linux) - Let undetected_chromedriver handle it or just use standard
            # GitHub Actions runner has Chrome installed.
            driver = uc.Chrome(options=options)
            
    except Exception as e:
        print(f"--- [Auth Service] Error Launching Chrome: {e}")
        # Fallback to standard selenium purely for debugging if UC fails?
        # But UC is needed for Cloudflare. We just return False.
        return False

    try:
        # 1. Hit the Home Page
        print("--- [Auth Service] Navigating to SofaScore...")
        driver.get("https://www.sofascore.com")
        
        # 2. Wait for Cloudflare/Shields
        print("--- [Auth Service] Waiting for 10s (TLS/Cloudflare)...")
        time.sleep(10) 
        
        # 3. Extract Secrets
        cookies = driver.get_cookies()
        user_agent = driver.execute_script("return navigator.userAgent;")
        
        print(f"--- [Auth Service] Success! Captured {len(cookies)} cookies.")
        print(f"--- [Auth Service] UA: {user_agent}")
        
        # 4. Save to JSON
        auth_payload = {
            "user_agent": user_agent,
            "cookies": {c['name']: c['value'] for c in cookies},
            "timestamp": time.time()
        }
        
        with open(COOKIE_FILE, 'w', encoding='utf-8') as f:
            json.dump(auth_payload, f, indent=2)
            
        print(f"--- [Auth Service] Saved keys to {COOKIE_FILE}")
        
        # Also print for GitHub Actions debug
        # print(json.dumps(auth_payload)) 
        
        return True
        
    except Exception as e:
        print(f"--- [Auth Service] Error during extraction: {e}")
        return False
        
    finally:
        print("--- [Auth Service] Closing Browser ---")
        try:
            driver.quit()
        except:
            pass

if __name__ == "__main__":
    success = get_auth_session()
    if not success:
        exit(1)
