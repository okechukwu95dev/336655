"""
Pick Extractor for GitHub Actions
Processes transcripts in shards for parallel execution.
"""
import argparse
import gzip
import json
import os
import sys
import time
from pathlib import Path

from openai import OpenAI

# Global client, initialized in main()
client = None

# Configuration
TRANSCRIPTS_FILE = "transcripts_all.jsonl.gz"
SCHEMA_FILE = "extraction_schema.json"
PROMPT_FILE = "extraction_prompt.txt"
EXTRACTIONS_DIR = Path("extractions")

# Model pricing per 1M tokens (January 2026)
MODEL_PRICING = {
    "gpt-4.1-mini": {"input": 0.40, "output": 1.60},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
}


def load_transcripts(file_path: str) -> list:
    """Load all transcripts from gzipped JSONL."""
    transcripts = []
    if file_path.endswith('.gz'):
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    transcripts.append(json.loads(line))
    else:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    transcripts.append(json.loads(line))
    return transcripts


def load_schema():
    """Load extraction schema (already in OpenAI format with name/strict/schema)."""
    with open(SCHEMA_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_prompt():
    """Load extraction prompt."""
    with open(PROMPT_FILE, 'r', encoding='utf-8') as f:
        return f.read()


def format_transcript_numbered(transcript_text: str) -> str:
    """Format transcript as numbered lines L0001, L0002, ..."""
    lines = transcript_text.split("\n")
    numbered = []
    for i, line in enumerate(lines, 1):
        numbered.append(f"L{i:04d}: {line}")
    return "\n".join(numbered)


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate cost in USD."""
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["gpt-4.1-mini"])
    input_cost = (input_tokens / 1_000_000) * pricing["input"]
    output_cost = (output_tokens / 1_000_000) * pricing["output"]
    return input_cost + output_cost


def detect_source(transcript: dict) -> str:
    """Detect the source/channel from transcript metadata."""
    source = transcript.get("source", "unknown")
    title = transcript.get("title", "").lower()
    
    if source == "pickdawgz" or "pickdawg" in title:
        return "pickdawgz"
    elif source == "maikito" or "maikito" in title:
        return "maikito"
    elif source == "matt_english" or "matt english" in title:
        return "matt_english"
    return source


def extract_single(transcript: dict, model: str, schema: dict, prompt: str) -> dict:
    """Extract picks from a single transcript."""
    video_id = transcript.get("id", "UNKNOWN")
    transcript_text = transcript.get("transcript", "")
    source = detect_source(transcript)
    
    numbered_lines = format_transcript_numbered(transcript_text)
    user_message = f"""Video ID: {video_id}
Source: {source}

TRANSCRIPT:
{numbered_lines}"""
    
    start_time = time.time()
    
    try:
        response = client.chat.completions.create(
            model=model,
            temperature=0.2,
            max_tokens=8000,
            response_format={
                "type": "json_schema",
                "json_schema": schema  # Schema already contains name/strict/schema
            },
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_message}
            ]
        )
        
        elapsed = time.time() - start_time
        usage = response.usage
        cost = calculate_cost(model, usage.prompt_tokens, usage.completion_tokens)
        
        content = response.choices[0].message.content
        parsed = json.loads(content)
        
        return {
            "success": True,
            "video_id": video_id,
            "model": model,
            "input_tokens": usage.prompt_tokens,
            "output_tokens": usage.completion_tokens,
            "cost_usd": cost,
            "elapsed_seconds": elapsed,
            "picks_count": len(parsed.get("picks", [])),
            "response": parsed
        }
    except Exception as e:
        return {
            "success": False,
            "video_id": video_id,
            "model": model,
            "error": str(e),
            "elapsed_seconds": time.time() - start_time
        }


def run_shard(shard: int, total_shards: int, model: str):
    """Process a single shard of transcripts."""
    EXTRACTIONS_DIR.mkdir(exist_ok=True)
    
    # Load data
    transcripts = load_transcripts(TRANSCRIPTS_FILE)
    schema = load_schema()
    prompt = load_prompt()
    
    # Calculate shard range
    total = len(transcripts)
    per_shard = total // total_shards
    start_idx = shard * per_shard
    end_idx = start_idx + per_shard if shard < total_shards - 1 else total
    
    shard_transcripts = transcripts[start_idx:end_idx]
    print(f"Shard {shard}: Processing {len(shard_transcripts)} transcripts ({start_idx} to {end_idx})")
    
    results = []
    total_cost = 0
    
    for i, t in enumerate(shard_transcripts):
        print(f"  [{i+1}/{len(shard_transcripts)}] {t.get('id', 'UNKNOWN')[:20]}...", end=" ", flush=True)
        result = extract_single(t, model, schema, prompt)
        results.append(result)
        
        if result["success"]:
            total_cost += result["cost_usd"]
            print(f"✓ {result['picks_count']} picks, ${result['cost_usd']:.4f}")
        else:
            print(f"✗ {result.get('error', 'unknown')[:50]}")
    
    # Save shard results
    output_file = EXTRACTIONS_DIR / f"shard_{shard:02d}.json.gz"
    with gzip.open(output_file, 'wt', encoding='utf-8') as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    
    print(f"Shard {shard} complete. Total cost: ${total_cost:.4f}")
    return {"shard": shard, "count": len(results), "cost": total_cost}


def merge_shards():
    """Merge all shard files into one."""
    all_results = []
    total_cost = 0
    
    for shard_file in sorted(EXTRACTIONS_DIR.glob("shard_*.json.gz")):
        with gzip.open(shard_file, 'rt', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    all_results.append(r)
                    if r.get("success"):
                        total_cost += r.get("cost_usd", 0)
    
    # Write merged file
    with gzip.open("extractions_all.jsonl.gz", 'wt', encoding='utf-8') as f:
        for r in all_results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    
    # Write cost report
    cost_report = {
        "total_transcripts": len(all_results),
        "successful": sum(1 for r in all_results if r.get("success")),
        "failed": sum(1 for r in all_results if not r.get("success")),
        "total_cost_usd": total_cost,
        "total_picks": sum(r.get("picks_count", 0) for r in all_results if r.get("success"))
    }
    with open("cost_report.json", 'w', encoding='utf-8') as f:
        json.dump(cost_report, f, indent=2)
    
    print(f"Merged {len(all_results)} results. Total cost: ${total_cost:.2f}")


def main():
    parser = argparse.ArgumentParser(description="Extract picks from transcripts")
    parser.add_argument("--test-single", nargs="?", const="FIRST", metavar="VIDEO_ID", 
                        help="Test single transcript (optionally specify video ID)")
    parser.add_argument("--test-batch", type=int, metavar="N", help="Test with N transcripts")
    parser.add_argument("--shard", type=int, help="Shard index (0-based)")
    parser.add_argument("--shards", type=int, default=20, help="Total number of shards")
    parser.add_argument("--model", default="gpt-4.1-mini", help="OpenAI model")
    parser.add_argument("--merge", action="store_true", help="Merge all shard files")
    parser.add_argument("--stats", action="store_true", help="Show stats from cost_report.json")
    
    args = parser.parse_args()
    
    # Set API key and initialize client
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key and not args.merge and not args.stats:
        print("ERROR: OPENAI_API_KEY not set")
        sys.exit(1)
    
    global client
    client = OpenAI(api_key=api_key)
    
    if args.test_single:
        EXTRACTIONS_DIR.mkdir(exist_ok=True)
        transcripts = load_transcripts(TRANSCRIPTS_FILE)
        schema = load_schema()
        prompt = load_prompt()
        
        # Find transcript by video_id or use first
        if args.test_single == "FIRST":
            transcript = transcripts[0]
        else:
            transcript = next((t for t in transcripts if t.get("id") == args.test_single), None)
            if not transcript:
                print(f"ERROR: Video ID '{args.test_single}' not found")
                sys.exit(1)
        
        print(f"Testing: {transcript.get('id')} - {transcript.get('title', 'Untitled')[:50]}...")
        result = extract_single(transcript, args.model, schema, prompt)
        output_file = EXTRACTIONS_DIR / f"test_{transcript.get('id', 'single')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        print(f"Result saved to {output_file}")
        
    elif args.test_batch:
        EXTRACTIONS_DIR.mkdir(exist_ok=True)
        transcripts = load_transcripts(TRANSCRIPTS_FILE)[:args.test_batch]
        schema = load_schema()
        prompt = load_prompt()
        results = [extract_single(t, args.model, schema, prompt) for t in transcripts]
        output_file = EXTRACTIONS_DIR / "test_batch.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        total_cost = sum(r.get("cost_usd", 0) for r in results if r.get("success"))
        print(f"Batch complete. Total cost: ${total_cost:.4f}")
        
    elif args.shard is not None:
        run_shard(args.shard, args.shards, args.model)
        
    elif args.merge:
        merge_shards()
        
    elif args.stats:
        with open("cost_report.json", 'r') as f:
            report = json.load(f)
        print(json.dumps(report, indent=2))
        
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
