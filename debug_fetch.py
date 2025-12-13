import fetch_manager
import sys

# Mock CLI args
fetch_manager.START_DATE = "12/12/2025"
fetch_manager.END_DATE = "12/12/2025"

def main():
    print("🔎 DEBUG FETCH START")
    ids = fetch_manager.get_favorite_competition_ids()
    print(f"Found {len(ids)} favorite comps")
    
    games = fetch_manager.fetch_games_for_period(ids)
    print(f"Found {len(games)} games")
    
    if not games:
        print("No games to test")
        return
        
    game_id = games[0]['id']
    print(f"Testing Worker with Game ID: {game_id}")
    
    seen = set()
    res = fetch_manager.worker_process_game(game_id, 0, seen)
    print(f"Result: {res}")
    
    # Check DB
    conn = fetch_manager.get_db()
    c = conn.execute("SELECT count(*) FROM games")
    print(f"DB Count: {c.fetchone()}")
    conn.close()

if __name__ == "__main__":
    main()
