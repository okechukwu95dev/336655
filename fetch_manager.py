import requests
import sqlite3
import json
import time
import os
import sys
import base64
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = "https://webws.365scores.com/web"
DEFAULT_PARAMS = {
    'appTypeId': 5,
    'langId': 9,
    'timezoneName': 'UTC',
    'userCountryId': 331
}

# Runtime Config (Global)
SHARD_ID = None
TOTAL_SHARDS = 1
IS_MERGE_MODE = False

DB_PATH = "games.db"  # Will be updated if sharding
LOG_DIR = "logs"
START_DATE = None  # Set via CLI argument
END_DATE = None    # Set via CLI argument
RATE_LIMIT_SECONDS = 0.1
BASE_DOMAIN = "https://webws.365scores.com"

NUM_WORKERS = 10
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "")

# Thread-safe locks
db_lock = threading.Lock()
log_lock = threading.Lock()

# =============================================================================
# LOGGING
# =============================================================================

def ensure_log_dir():
    """Create logs directory if not exist"""
    os.makedirs(LOG_DIR, exist_ok=True)

def get_log_path():
    """Generate log filename based on date range"""
    if START_DATE and END_DATE:
        return f"{LOG_DIR}/fetch_{START_DATE.replace('/', '-')}_{END_DATE.replace('/', '-')}.log"
    return f"{LOG_DIR}/fetch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log_event(data):
    """Thread-safe JSON logging"""
    ensure_log_dir()
    with log_lock:
        timestamp = datetime.now().isoformat()
        log_entry = {
            "timestamp": timestamp,
            "data": data
        }
        log_path = get_log_path()
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, separators=(',', ':')) + "\n")

def print_header(msg):
    """Print formatted header"""
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}\n")

# =============================================================================
# DATABASE INITIALIZATION & RESTORATION
# =============================================================================

def restore_db_from_github():
    """
    Restore SQLite DB from base64-encoded file.
    This reads games.db.b64 from repo and decodes it to games.db for runtime.
    If file doesn't exist, start with fresh DB.
    """
    import gzip
    
    gz_path = "games.db.gz"
    
    if not os.path.exists(gz_path):
        print(f"⚠️  No {gz_path} found - starting with fresh DB")
        return
    
    try:
        print(f"📥 Decompressing {gz_path}...")
        with gzip.open(gz_path, "rb") as gz:
            db_bytes = gz.read()
        
        with open(DB_PATH, "wb") as f:
            f.write(db_bytes)
        
        # Verify DB is valid
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("SELECT 1")
        conn.close()
        
        print(f"✅ DB restored successfully ({len(db_bytes)} bytes)")
    except Exception as e:
        log_event({
            "type": "DB_RESTORE_ERROR",
            "error": str(e),
            "msg": f"Failed to restore DB: {e}"
        })
        print(f"❌ DB restore failed: {e}")
        print(f"   Starting with fresh DB instead")

def init_db():
    """Create games table if not exists"""
    try:
        with db_lock:
            conn = sqlite3.connect(DB_PATH, timeout=10)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id INTEGER PRIMARY KEY,
                    date_time TEXT,
                    status TEXT,
                    home_id INTEGER,
                    away_id INTEGER,
                    home_score INTEGER,
                    away_score INTEGER,
                    league_id INTEGER,
                    context_json TEXT,
                    fetched_at TEXT
                )
            """)
            
            # Create index for faster lookups
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_games_id_status 
                ON games(id, status)
            """)
            
            conn.commit()
            conn.close()
        
        print("✅ Database initialized")
    except Exception as e:
        log_event({
            "type": "DB_INIT_ERROR",
            "error": str(e)
        })
        print(f"❌ DB init failed: {e}")
        sys.exit(1)

def get_db():
    """Get thread-safe SQLite connection"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        log_event({
            "type": "DB_CONNECTION_ERROR",
            "error": str(e)
        })
        raise

# =============================================================================
# HELPERS
# =============================================================================

def fetch(endpoint, extra_params=None, retries=3):
    """
    Fetch from 365Scores API with retry logic.
    Retries up to 3 times on transient failures.
    """
    params = {**DEFAULT_PARAMS, **(extra_params or {})}
    
    for attempt in range(retries):
        try:
            r = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=15)
            time.sleep(RATE_LIMIT_SECONDS)
            
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                # Rate limited - back off
                wait_time = 2 ** attempt
                print(f"   ⚠️  Rate limited, backing off {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                if attempt == retries - 1:
                    log_event({
                        "type": "API_HTTP_ERROR",
                        "endpoint": endpoint,
                        "status_code": r.status_code,
                        "attempt": attempt + 1
                    })
                    return None
        except requests.Timeout:
            if attempt == retries - 1:
                log_event({
                    "type": "API_TIMEOUT",
                    "endpoint": endpoint,
                    "attempt": attempt + 1
                })
                return None
            time.sleep(1)
        except Exception as e:
            if attempt == retries - 1:
                log_event({
                    "type": "API_FETCH_ERROR",
                    "endpoint": endpoint,
                    "error": str(e),
                    "attempt": attempt + 1
                })
                return None
            time.sleep(1)
    
    return None

def get_favorite_competition_ids():
    """Get all favorited competition IDs from local JSON file"""
    try:
        json_path = "favorites.json"
        if not os.path.exists(json_path):
            print(f"⚠️  {json_path} not found - returning empty list")
            return []
            
        with open(json_path, "r") as f:
            ids = json.load(f)
            print(f"✅ Loaded {len(ids)} favorite competitions from {json_path}")
            return ids
    except Exception as e:
        log_event({
            "type": "FAVORITES_LOAD_ERROR",
            "error": str(e)
        })
        print(f"⚠️  Could not load favorites: {e}")
        return []

def is_game_complete_in_db(conn, game_id):
    """Check if game exists and has Final status"""
    try:
        row = conn.execute(
            "SELECT status FROM games WHERE id = ?", (game_id,)
        ).fetchone()
        return row and row['status'] == 'Final'
    except:
        return False

# =============================================================================
# API FETCHING FUNCTIONS
# =============================================================================

def fetch_games_for_period(favorite_ids):
    """Fetch all games in date range, filter to favorites"""
    if not START_DATE or not END_DATE:
        print("❌ START_DATE and END_DATE not set")
        return []
    
    print(f"📅 Fetching games: {START_DATE} to {END_DATE}")
    
    data = fetch("/games/allscores/", {
        "startDate": START_DATE,
        "endDate": END_DATE,
        "sports": 1
    })
    
    if not data or "games" not in data:
        print("❌ Failed to fetch games list")
        log_event({
            "type": "GAMES_LIST_ERROR",
            "msg": "Failed to fetch games for period"
        })
        return []
    
    all_games = data["games"]
    favorite_set = set(favorite_ids)
    
    # Filter by favorites
    relevant_games = [g for g in all_games if g.get("competitionId") in favorite_set]
    
    if SHARD_ID is not None:
        # Sharding Logic: Process games where (id % total) == shard
        my_games = []
        for g in relevant_games:
            try:
                gid = int(g['id'])
                if gid % TOTAL_SHARDS == SHARD_ID:
                    my_games.append(g)
            except:
                pass
        
        print(f"✅ Found {len(relevant_games)} relevant games")
        print(f"🔪 SHARD {SHARD_ID}/{TOTAL_SHARDS}: Processing {len(my_games)} games")
        return my_games
        
    print(f"✅ Found {len(relevant_games)} games (from {len(all_games)} total)")
    return relevant_games

def fetch_game_details(game_id):
    """Fetch full game details including events and members"""
    data = fetch("/game/", {"gameId": game_id})
    return data.get("game") if data else None

def fetch_h2h(game_id):
    """Fetch H2H (head-to-head) history"""
    data = fetch("/games/h2h/", {"gameId": game_id, "appTypeId": 3})
    return data.get("game", {}).get("h2hGames", []) if data else []

def fetch_standings(competition_id):
    """Fetch current competition standings"""
    data = fetch("/standings/", {
        "competitions": competition_id,
        "live": "false"
    })
    
    if data and "standings" in data and data["standings"]:
        return data["standings"][0].get("rows", [])
    return []

def fetch_squad_status(team_id, competition_id):
    """
    Fetch squad with injury/suspension data.
    Step 1: Get basic squad roster
    Step 2: Enrich with injury/suspension metadata
    """
    squad_data = fetch("/squads/", {"competitors": team_id})
    
    if not squad_data or "squads" not in squad_data or not squad_data["squads"]:
        return []
    
    athletes = squad_data["squads"][0].get("athletes", [])
    if not athletes:
        return []
    
    # Batch enrichment: fetch injury data for up to 70 players
    athlete_ids = ",".join([str(a["id"]) for a in athletes[:70]])
    athlete_data = fetch("/athletes/", {
        "athletes": athlete_ids,
        "competitionId": competition_id,
        "fullDetails": "true",
        "topBookmaker": "103"
    })
    
    if athlete_data and "athletes" in athlete_data:
        injury_map = {a["id"]: a for a in athlete_data["athletes"]}
        for athlete in athletes:
            if athlete["id"] in injury_map:
                enriched = injury_map[athlete["id"]]
                athlete["injury"] = enriched.get("injury")
                athlete["suspension"] = enriched.get("suspension")
    
    return athletes

def fetch_team_form(team_id, pages=2):
    """
    Fetch recent results for a team with pagination.
    Default 2 pages of recent games.
    """
    all_games = []
    cursor = None
    
    for page_num in range(pages):
        try:
            if cursor:
                # Use cursor from previous response for pagination
                url = f"{BASE_DOMAIN}{cursor}"
                r = requests.get(url, timeout=15)
                time.sleep(RATE_LIMIT_SECONDS)
                
                if r.status_code != 200:
                    log_event({
                        "type": "FORM_PAGE_ERROR",
                        "team_id": team_id,
                        "page": page_num + 1,
                        "status_code": r.status_code
                    })
                    break
                
                if not r.text.strip():
                    break
                
                data = r.json()
            else:
                data = fetch("/games/results/", {"competitors": team_id})
            
            if not data or "games" not in data:
                break
            
            all_games.extend(data["games"])
            
            # Get next page cursor
            paging = data.get("paging", {})
            cursor = paging.get("previousPage")
            
            if not cursor:
                break
        
        except Exception as e:
            log_event({
                "type": "FORM_FETCH_ERROR",
                "team_id": team_id,
                "page": page_num + 1,
                "error": str(e)
            })
            break
    
    return all_games

# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def save_game(game, context):
    """Save/update game record with context JSON"""
    with db_lock:
        try:
            conn = get_db()
            now = datetime.now().isoformat()
            home = game.get("homeCompetitor", {})
            away = game.get("awayCompetitor", {})
            
            conn.execute("""
                INSERT OR REPLACE INTO games 
                (id, date_time, status, home_id, away_id, home_score, away_score, league_id, context_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                game.get("id"),
                game.get("startTime"),
                game.get("statusText", "Scheduled"),
                home.get("id"),
                away.get("id"),
                home.get("score", -1),
                away.get("score", -1),
                game.get("competitionId"),
                json.dumps(context),
                now
            ))
            
            conn.commit()
            conn.close()
        except Exception as e:
            log_event({
                "type": "DB_SAVE_ERROR",
                "game_id": game.get("id"),
                "error": str(e)
            })

def merge_shards():
    """
    Merge all games_shard_*.db files into games.db
    """
    print_header("Merging Shards")
    
    # Ensure main DB exists/init
    init_db()
    
    # Find shard files
    files = [f for f in os.listdir('.') if f.startswith('games_shard_') and f.endswith('.db')]
    print(f"📦 Found {len(files)} shard databases")
    
    with db_lock:
        main_conn = get_db()
        count = 0
        
        for sf in files:
            print(f"   ➕ Merging {sf}...", end="", flush=True)
            try:
                # Attach shard DB
                main_conn.execute(f"ATTACH DATABASE '{sf}' AS shard_db")
                
                # Copy games
                main_conn.execute("INSERT OR IGNORE INTO games SELECT * FROM shard_db.games")
                
                # Detach
                main_conn.commit()
                main_conn.execute("DETACH DATABASE shard_db")
                print(" ✅")
                count += 1
            except Exception as e:
                print(f" ❌ Error: {e}")
        
        main_conn.close()
        
    print(f"✅ Merged {count} shards into {DB_PATH}")

def push_db_to_github():
    """
    Encode SQLite DB as base64 and push to GitHub.
    This preserves binary data in a text format for Git tracking.
    """
    if not GITHUB_TOKEN or not GITHUB_REPOSITORY:
        print("⚠️  GITHUB_TOKEN or GITHUB_REPOSITORY not set - skipping push")
        return False
    
    try:
        import subprocess
        
        print("\n📤 Encoding DB to base64...")
        
        # Read binary DB
        if not os.path.exists(DB_PATH):
            print("⚠️  games.db not found - nothing to push")
            return False
        
        with open(DB_PATH, "rb") as f:
            db_bytes = f.read()
        
        # Compress with gzip (much better than base64!)
        print(f"📦 Compressing {len(db_bytes)} bytes with gzip...")
        
        import gzip
        with gzip.open("games.db.gz", "wb", compresslevel=9) as gz:
            gz.write(db_bytes)
        
        compressed_size = os.path.getsize("games.db.gz")
        compression_ratio = (1 - compressed_size / len(db_bytes)) * 100
        
        print(f"✅ Compressed to {compressed_size:,} bytes ({compression_ratio:.1f}% smaller)")
        
        # Remove old artifacts
        for f in os.listdir('.'):
            if f.startswith("games.db.b64"):
                os.remove(f)
                print(f"   🗑️  Removed old chunk: {f}")
        
        # Configure git
        print("🔧 Configuring git...")
        subprocess.run(
            ["git", "config", "user.name", "fetch-bot"],
            check=True,
            capture_output=True
        )
        subprocess.run(
            ["git", "config", "user.email", "bot@365scores.local"],
            check=True,
            capture_output=True
        )
        
        # Stage and commit
        print("📝 Staging changes...")
        subprocess.run(
            ["git", "add", "games.db.gz"],
            check=True,
            capture_output=True
        )
        
        result = subprocess.run(
            ["git", "commit", "-m", f"Auto: fetch {START_DATE} to {END_DATE} - {datetime.now().isoformat()}"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"✅ Committed")
        elif "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
            print(f"ℹ️  Nothing to commit")
        else:
            log_event({
                "type": "GIT_COMMIT_ERROR",
                "stderr": result.stderr
            })
            return False
        
        # Push
        print("🚀 Pushing to GitHub...")
        result = subprocess.run(
            ["git", "push"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes for large uploads
        )
        
        if result.returncode == 0:
            print(f"✅ Pushed to GitHub")
            return True
        else:
            log_event({
                "type": "GIT_PUSH_ERROR",
                "stderr": result.stderr
            })
            print(f"❌ Push failed: {result.stderr}")
            return False
    
    except Exception as e:
        log_event({
            "type": "GITHUB_PUSH_ERROR",
            "error": str(e)
        })
        print(f"❌ Push error: {e}")
        return False

# =============================================================================
# WORKER TASK (Parallel Processing)
# =============================================================================

def worker_process_game(game_id, depth):
    """
    Worker processes a single game:
    - Depth 0 (ROOT): Fetch full details, H2H, form, squad, standings
    - Depth 1 (LEAF): Fetch details only, no recursive discovery
    
    Returns list of discovered game IDs to enqueue.
    """
    # Check if already complete in DB
    try:
        conn = get_db()
        if is_game_complete_in_db(conn, game_id):
            conn.close()
            return []
        conn.close()
    except:
        pass
    
    t_name = threading.current_thread().name
    context_label = "ROOT" if depth == 0 else "LEAF"
    
    print(f"   ⚡ [{t_name}] [{context_label:4}] Game {game_id} started")
    
    # Fetch core game details
    details = fetch_game_details(game_id)
    if not details:
        print(f"   ❌ [{t_name}] Game {game_id} failed")
        return []
    
    # Build context object
    context = {
        "members": details.get("members", []),
        "events": details.get("events", []),
        "h2h_refs": [],
        "home_form_refs": [],
        "away_form_refs": []
    }
    
    queued = []
    
    # DEPTH 0: Discover and fetch context
    if depth < 1:
        # H2H
        h2h_games = fetch_h2h(game_id)
        context["h2h_refs"] = [g['id'] for g in h2h_games]
        queued.extend([g['id'] for g in h2h_games])
        
        home_id = details.get('homeCompetitor', {}).get('id')
        away_id = details.get('awayCompetitor', {}).get('id')
        comp_id = details.get("competitionId")
        
        # Home team form
        if home_id:
            form_home = fetch_team_form(home_id, pages=2)
            context["home_form_refs"] = [g['id'] for g in form_home]
            queued.extend([g['id'] for g in form_home])
        
        # Away team form
        if away_id:
            form_away = fetch_team_form(away_id, pages=2)
            context["away_form_refs"] = [g['id'] for g in form_away]
            queued.extend([g['id'] for g in form_away])
        
        # Current standings (root only - not time-sensitive for historical games)
        standings = fetch_standings(comp_id)
        if standings:
            context["standings"] = standings
        
        # Current squad status (root only - for injury/suspension)
        if home_id:
            context["home_squad"] = fetch_squad_status(home_id, comp_id)
        if away_id:
            context["away_squad"] = fetch_squad_status(away_id, comp_id)
    
    # Save to DB
    save_game(details, context)
    
    # Print status
    if queued:
        print(f"   ✅ [{t_name}] Game {game_id} done ({len(queued)} queued)")
    else:
        print(f"   ✅ [{t_name}] Game {game_id} done")
    
    return queued

# =============================================================================
# PARALLEL FETCHER
# =============================================================================

class ParallelFetcher:
    """
    Manages work queue and coordinates workers.
    Uses thread pool for 10 concurrent workers.
    Dynamically discovers related games and enqueues them.
    """
    
    def __init__(self, num_workers=NUM_WORKERS):
        self.num_workers = num_workers
        self.seen_ids = set()
        self.to_process = []
        self.processed_count = 0
        self.start_time = None
    
    def enqueue_batch(self, game_ids, depth=0):
        """Add games to queue if not already seen"""
        for gid in game_ids:
            if gid not in self.seen_ids:
                self.to_process.append((gid, depth))
                self.seen_ids.add(gid)
    
    def process_parallel(self):
        """Execute parallel processing with thread pool"""
        initial_count = len(self.to_process)
        print(f"\\n🔄 Processing {initial_count} root games with {self.num_workers} workers")
        
        self.start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {}
            idx = 0
            
            while idx < len(self.to_process) or futures:
                # Submit new tasks up to worker limit
                while len(futures) < self.num_workers and idx < len(self.to_process):
                    gid, depth = self.to_process[idx]
                    future = executor.submit(
                        worker_process_game, gid, depth
                    )
                    futures[future] = (gid, depth, idx)
                    idx += 1
                
                if not futures:
                    break
                
                # Wait for next completion
                for future in as_completed(futures.keys()):
                    gid, depth, task_idx = futures.pop(future)
                    self.processed_count += 1
                    
                    try:
                        new_ids = future.result()
                        # Enqueue discovered context games at depth+1
                        if new_ids and depth < 1:
                            self.enqueue_batch(new_ids, depth=depth + 1)
                    except Exception as e:
                        log_event({
                            "type": "WORKER_ERROR",
                            "game_id": gid,
                            "depth": depth,
                            "error": str(e)
                        })
                        print(f"\\n   ❌ Worker error on game {gid}: {e}")
                    
                    # Progress update
                    if self.processed_count % 20 == 0:
                        elapsed = time.time() - self.start_time
                        rate = self.processed_count / elapsed if elapsed > 0 else 0
                        remaining = len(self.to_process) - idx + len(futures)
                        print(f"   📊 {self.processed_count}/{len(self.to_process)} | {rate:.1f} games/sec | {remaining} pending")
                    
                    break
    
    def get_summary(self):
        """Return execution summary"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.processed_count / elapsed if elapsed > 0 else 0
        return {
            "processed": self.processed_count,
            "elapsed_seconds": elapsed,
            "elapsed_minutes": elapsed / 60,
            "rate": rate
        }

# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point"""
    print_header("365SCORES PARALLEL FETCH - GitHub Actions")
    
    global START_DATE, END_DATE, SHARD_ID, TOTAL_SHARDS, IS_MERGE_MODE, DB_PATH
    
    args = sys.argv[1:]
    
    # Parse args manually to avoid argparse dep if desired, or use argparse
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", nargs="?", help="Start Date DD/MM/YYYY")
    parser.add_argument("end_date", nargs="?", help="End Date DD/MM/YYYY")
    parser.add_argument("--shard", type=int, help="Shard index (0-N)")
    parser.add_argument("--shards", type=int, default=10, help="Total shards")
    parser.add_argument("--merge", action="store_true", help="Run merge mode")
    
    parsed = parser.parse_args()
    
    if parsed.merge:
        merge_shards()
        push_db_to_github()
        sys.exit(0)
        
    if not parsed.start_date or not parsed.end_date:
        print("❌ Usage: python fetch_manager.py <START_DATE> <END_DATE> [--shard N --shards T] | [--merge]")
        sys.exit(1)
        
    START_DATE = parsed.start_date
    END_DATE = parsed.end_date
    
    if parsed.shard is not None:
        SHARD_ID = parsed.shard
        TOTAL_SHARDS = parsed.shards
        DB_PATH = f"games_shard_{SHARD_ID}.db"
        print(f"🔪 Running Shard {SHARD_ID} of {TOTAL_SHARDS}")
        print(f"💾 DB Output: {DB_PATH}")
    
    print(f"📅 Date Range: {START_DATE} to {END_DATE}")
    print(f"👷 Workers: {NUM_WORKERS}")
    print(f"🔗 GitHub: {GITHUB_REPOSITORY if GITHUB_REPOSITORY else 'local'}")
    
    # Step 1: Restore existing DB (SKIP if sharding, start fresh for shard)
    if SHARD_ID is None:
        restore_db_from_github()
    else:
        print("clean shard db start")
    
    # Step 2: Initialize DB
    init_db()
    
    # Step 3: Fetch root games
    print_header("Fetching Games List")
    ids = get_favorite_competition_ids()
    if not ids:
        print("⚠️  No favorite competitions found")
    
    root_games = fetch_games_for_period(ids)
    
    if not root_games and SHARD_ID is None:
        print("❌ No games found for date range")
        sys.exit(0)
    
    if not root_games and SHARD_ID is not None:
        print("ℹ️  No games assigned to this shard")
        # Ensure DB is created even if empty for artifact
        sys.exit(0)
    
    # Step 4: Parallel fetch
    print_header("Parallel Processing")
    fetcher = ParallelFetcher(num_workers=NUM_WORKERS)
    fetcher.enqueue_batch([g['id'] for g in root_games], depth=0)
    fetcher.process_parallel()
    
    # Step 5: Summary
    summary = fetcher.get_summary()
    print_header("Fetch Complete")
    print(f"✅ {summary['processed']} games processed")
    print(f"⏱️  {summary['elapsed_minutes']:.1f} minutes ({summary['rate']:.1f} games/sec)")
    
    # Step 6: Push (ONLY IF NOT SHARDING)
    if SHARD_ID is None:
        print_header("GitHub Integration")
        push_db_to_github()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\\n\\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\\n❌ Fatal error: {e}")
        log_event({
            "type": "FATAL_ERROR",
            "error": str(e)
        })
        sys.exit(1)
