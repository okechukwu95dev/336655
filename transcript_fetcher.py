# YouTube Transcript Fetcher for GitHub Actions
# Fetches transcripts from playlists/channels in parallel shards

import argparse
import json
import gzip
import os
import time
import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi

SOURCES = [
    ("pigdogs", "https://www.youtube.com/playlist?list=PLTj5YsbjO3Rbe0XMf8S1kfsfa7llhwHMe"),
    ("betting_tips", "https://www.youtube.com/playlist?list=PLeOIYRAVi-RLDSKyPC1v_nzG3fdSzKLC-"),
    ("maikito23", "https://www.youtube.com/@Ma-i-kito23/videos"),
]

def get_all_video_ids():
    """Fetch all video IDs from all sources."""
    all_videos = []
    ydl_opts = {'quiet': True, 'extract_flat': True, 'ignoreerrors': True}
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for source_name, url in SOURCES:
            try:
                info = ydl.extract_info(url, download=False)
                entries = info.get('entries', [])
                for entry in entries:
                    all_videos.append({
                        'id': entry['id'],
                        'title': entry.get('title', ''),
                        'source': source_name
                    })
                print(f"✅ {source_name}: {len(entries)} videos")
            except Exception as e:
                print(f"❌ {source_name}: {e}")
    
    return all_videos

def fetch_transcript(video_id):
    """Fetch transcript for a single video."""
    api = YouTubeTranscriptApi()
    try:
        transcript = api.fetch(video_id)
        text = " ".join([item.text for item in transcript])
        return text, None
    except Exception as e:
        return None, str(e)

def fetch_single(video_id):
    """Fetch a single video transcript - for testing."""
    print(f"🧪 Testing single video: {video_id}")
    
    transcript, error = fetch_transcript(video_id)
    if transcript:
        print(f"✅ Success! {len(transcript):,} chars")
        print(f"\n--- First 500 chars ---\n{transcript[:500]}")
        
        # Save result
        with open(f'{video_id}.txt', 'w', encoding='utf-8') as f:
            f.write(transcript)
        print(f"\n📁 Saved to {video_id}.txt")
    else:
        print(f"❌ Failed to fetch transcript")
        print(f"Error: {error}")


def fetch_batch(count=5):
    """Fetch a small batch of videos - for testing."""
    print(f"🧪 Testing batch of {count} videos...")
    
    # Get first N videos
    all_videos = get_all_video_ids()
    batch = all_videos[:count]
    
    results = []
    for i, video in enumerate(batch):
        vid_id = video['id']
        
        # Rate limit
        if i > 0:
            time.sleep(2)
        
        transcript, error = fetch_transcript(vid_id)
        
        if transcript:
            results.append({
                'id': vid_id,
                'title': video['title'],
                'source': video['source'],
                'transcript': transcript,
                'chars': len(transcript)
            })
            print(f"✅ [{i+1}/{count}] {vid_id}: {len(transcript):,} chars")
        else:
            results.append({
                'id': vid_id,
                'title': video['title'],
                'source': video['source'],
                'transcript': None,
                'error': error
            })
            print(f"❌ [{i+1}/{count}] {vid_id}: {error}")
    
    # Save results
    output_file = f'test_batch_{count}.jsonl'
    with open(output_file, 'w', encoding='utf-8') as f:
        for item in results:
            f.write(json.dumps(item) + '\n')
    
    success = sum(1 for r in results if r.get('transcript'))
    print(f"\n🏁 Batch complete: {success}/{count} success")
    print(f"📁 Saved to {output_file}")

def process_shard(shard_idx, total_shards, output_file):
    """Process a specific shard of videos."""
    print(f"🚀 Shard {shard_idx}/{total_shards} starting...")
    
    # Get all video IDs first
    all_videos = get_all_video_ids()
    total = len(all_videos)
    print(f"📊 Total videos: {total}")
    
    # Calculate shard boundaries
    shard_size = (total + total_shards - 1) // total_shards
    start_idx = shard_idx * shard_size
    end_idx = min(start_idx + shard_size, total)
    
    shard_videos = all_videos[start_idx:end_idx]
    print(f"📦 Shard {shard_idx}: videos {start_idx} to {end_idx-1} ({len(shard_videos)} videos)")
    
    results = []
    success = 0
    failed = 0
    
    for i, video in enumerate(shard_videos):
        vid_id = video['id']
        
        # Rate limiting - 1 request per 2 seconds
        if i > 0:
            time.sleep(2)
        
        transcript, error = fetch_transcript(vid_id)
        
        if transcript:
            results.append({
                'id': vid_id,
                'title': video['title'],
                'source': video['source'],
                'transcript': transcript,
                'chars': len(transcript)
            })
            success += 1
            print(f"✅ [{i+1}/{len(shard_videos)}] {vid_id}")
        else:
            results.append({
                'id': vid_id,
                'title': video['title'],
                'source': video['source'],
                'transcript': None,
                'error': error
            })
            failed += 1
            print(f"❌ [{i+1}/{len(shard_videos)}] {vid_id}: {error}")
    
    # Save shard results
    with gzip.open(output_file, 'wt', encoding='utf-8') as f:
        for item in results:
            f.write(json.dumps(item) + '\n')
    
    print(f"\n🏁 Shard {shard_idx} complete: {success} success, {failed} failed")
    print(f"📁 Saved to {output_file}")

def merge_shards(output_file):
    """Merge all shard files into one."""
    import glob
    
    shard_files = sorted(glob.glob('transcripts_shard_*.jsonl.gz'))
    print(f"🔄 Merging {len(shard_files)} shard files...")
    
    total_records = 0
    with gzip.open(output_file, 'wt', encoding='utf-8') as out:
        for shard_file in shard_files:
            with gzip.open(shard_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    out.write(line)
                    total_records += 1
    
    print(f"✅ Merged {total_records} records into {output_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--shard', type=int, help='Shard index (0-based)')
    parser.add_argument('--shards', type=int, default=15, help='Total number of shards')
    parser.add_argument('--merge', action='store_true', help='Merge all shards')
    parser.add_argument('--list-ids', action='store_true', help='Just list all video IDs')
    parser.add_argument('--test-single', type=str, help='Test with single video ID')
    parser.add_argument('--test-batch', type=int, help='Test with N videos')
    args = parser.parse_args()
    
    if args.merge:
        merge_shards('transcripts_all.jsonl.gz')
    elif args.list_ids:
        videos = get_all_video_ids()
        print(f"\nTotal: {len(videos)} videos")
        with open('video_ids.json', 'w') as f:
            json.dump(videos, f, indent=2)
    elif args.test_single:
        fetch_single(args.test_single)
    elif args.test_batch:
        fetch_batch(args.test_batch)
    elif args.shard is not None:
        output_file = f'transcripts_shard_{args.shard}.jsonl.gz'
        process_shard(args.shard, args.shards, output_file)
    else:
        parser.print_help()
