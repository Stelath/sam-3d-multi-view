import objaverse.xl as oxl
import pandas as pd
import os
import multiprocessing
import argparse
from pathlib import Path
from manifest import Manifest, ObjectRecord
import gc
import requests
from tqdm import tqdm

SUPPORTED_EXTS = ['.glb', '.gltf', '.obj', '.fbx', '.ply']

def dummy_callback(*args, **kwargs):
    """Dummy callback for multiprocessing."""
    pass

def download_file(args):
    """Download a single file from a URL."""
    url, target_path = args
    try:
        if os.path.exists(target_path):
            return True # Skip if exists
            
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(target_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception:
        # print(f"Error downloading {url}: {e}")
        return False

def scan_downloaded_objects(download_dir):
    """Scan download directory and create manifest records."""
    print("Scanning downloaded objects...")
    objects = []
    # supported_exts = {'.glb', '.gltf', '.obj', '.fbx', '.blend', '.stl'} 
    # Use global constant
    supported_exts = set(SUPPORTED_EXTS)
    
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            ext = Path(file).suffix.lower()
            if ext in supported_exts:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, download_dir)
                
                # Generate simple ID from filename
                obj_id = Path(file).stem
                # Some IDs might be long, but we need the full UID for Objaverse++
                
                # Determine source based on path
                source = "unknown"
                if "smithsonian" in rel_path.lower():
                    source = "smithsonian"
                elif "objaverse_legacy" in rel_path.lower():
                    source = "objaverse-plusplus"
                elif "github" in rel_path.lower():
                    source = "github"
                
                obj = ObjectRecord(
                    id=obj_id,
                    source_url=f"file://{rel_path}",
                    local_path=rel_path,
                    file_type=ext[1:],
                    source=source,
                    license=None,
                    sha256=obj_id,
                    download_status="success"
                )
                objects.append(obj)
    
    return objects

def download_smithsonian(args, manifest):
    """Download Smithsonian objects."""
    print("\n=== Phase 1: Smithsonian Objects ===")
    
    # Load Objaverse-XL annotations
    print("Loading Objaverse-XL annotations...")
    try:
        annotations = oxl.get_annotations(download_dir=args.download_dir)
    except Exception as e:
        print(f"Error loading annotations: {e}")
        return

    smithsonian_objs = pd.DataFrame()
    if 'source' in annotations.columns:
        smithsonian_objs = annotations[annotations['source'] == 'smithsonian'].copy()
        print(f"Found {len(smithsonian_objs)} Smithsonian objects available.")
    
    # Free memory
    del annotations
    gc.collect()
    
    if smithsonian_objs.empty:
        print("No Smithsonian objects found.")
        return

    # Apply target count / limit
    # "if thats more than the dataset (in the case of the smithsonian) then just have it download all"
    # So we take min(target_count, available)
    count = min(args.target_count, len(smithsonian_objs))
    
    if args.limit:
        count = min(count, args.limit)
        
    print(f"Targeting {count} Smithsonian objects.")
    
    to_download = smithsonian_objs.head(count)
    
    # Download
    print(f"Starting download of {len(to_download)} objects...")
    oxl.download_objects(
        objects=to_download,
        download_dir=args.download_dir,
        processes=args.processes,
        handle_found_object=dummy_callback,
        handle_missing_object=dummy_callback
    )
    print("Smithsonian download complete.")

def download_objaverse_plusplus(args, manifest):
    """Download Objaverse++ High Quality objects."""
    print("\n=== Phase 2: Objaverse++ High Quality Objects ===")
    
    # 1. Get High Quality UIDs
    print("Loading Objaverse++ metadata...")
    high_quality_uids = []
    try:
        from datasets import load_dataset
        dataset = load_dataset("cindyxl/ObjaversePlusPlus", split="train", streaming=True)
        
        needed = args.target_count
        print(f"Targeting {needed} objects.")
        
        # Score 3
        print("Collecting Score 3 (Superior) objects...")
        for item in dataset:
            if item.get('score', 0) >= 3:
                high_quality_uids.append(item['UID'])
                if len(high_quality_uids) >= needed:
                    break
        
        # Score 2
        if len(high_quality_uids) < needed:
            print(f"Collecting Score 2 (High) objects (have {len(high_quality_uids)}, need {needed})...")
            dataset_2 = load_dataset("cindyxl/ObjaversePlusPlus", split="train", streaming=True)
            for item in dataset_2:
                if item.get('score', 0) == 2:
                    high_quality_uids.append(item['UID'])
                    if len(high_quality_uids) >= needed:
                        break
                        
        print(f"Collected {len(high_quality_uids)} high quality UIDs.")
        
    except ImportError:
        print("Error: 'datasets' library not found. Install with: pip install datasets")
        return
    except Exception as e:
        print(f"Error loading Objaverse++ metadata: {e}")
        return

    if not high_quality_uids:
        print("No objects found.")
        return

    # 2. Get URL mapping
    print("Loading Objaverse object paths...")
    import objaverse
    object_paths = objaverse._load_object_paths()
    
    # 3. Prepare download tasks
    tasks = []
    base_url = "https://huggingface.co/datasets/allenai/objaverse/resolve/main/"
    target_dir = os.path.join(args.download_dir, "objaverse_legacy")
    
    print("Preparing download tasks...")
    for uid in high_quality_uids:
        if uid in object_paths:
            rel_path = object_paths[uid]
            url = base_url + rel_path
            # Save as uid.glb in objaverse_legacy folder
            # We flatten the structure to avoid deep nesting if desired, or keep it?
            # User said "save ... to the home objaverse folder".
            # Let's keep it simple: objaverse_legacy/{uid}.glb
            local_path = os.path.join(target_dir, f"{uid}.glb")
            tasks.append((url, local_path))
            
    print(f"Prepared {len(tasks)} downloads.")
    
    if args.limit and len(tasks) > args.limit:
        tasks = tasks[:args.limit]
        print(f"Limiting to {len(tasks)} downloads.")
    
    # 4. Download with progress bar
    print(f"Downloading with {args.processes} processes...")
    with multiprocessing.Pool(processes=args.processes) as pool:
        results = list(tqdm(pool.imap_unordered(download_file, tasks), total=len(tasks)))
    
    success = sum(results)
    print(f"Objaverse++ download complete. Success: {success}/{len(tasks)}")

def main():
    parser = argparse.ArgumentParser(description="Download Objaverse objects.")
    parser.add_argument("--dataset", choices=['all', 'smithsonian', 'objaverse_plusplus'], default='all', 
                        help="Which dataset to download.")
    parser.add_argument("--target-count", type=int, default=30000, help="Number of objects to download per dataset.")
    parser.add_argument("--download-dir", type=str, default="./data/objaverse", help="Directory to save objects.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of downloads (for testing).")
    parser.add_argument("--processes", type=int, default=multiprocessing.cpu_count(), help="Number of processes to use.")
    parser.add_argument("--resume", action="store_true", help="Resume from existing manifest.")
    
    args = parser.parse_args()
    
    # Expand user path
    args.download_dir = os.path.expanduser(args.download_dir)
    os.makedirs(args.download_dir, exist_ok=True)
    
    # Initialize manifest
    manifest_path = os.path.join(args.download_dir, "manifest.json")
    manifest = Manifest(manifest_path)
    
    if args.resume:
        stats = manifest.get_stats()
        print(f"Resuming. Already downloaded: {stats['downloaded']}")

    # Execute requested downloads
    if args.dataset in ['all', 'smithsonian']:
        download_smithsonian(args, manifest)
        
    if args.dataset in ['all', 'objaverse_plusplus']:
        download_objaverse_plusplus(args, manifest)
        
    # Update Manifest
    print("\nUpdating manifest...")
    objects = scan_downloaded_objects(args.download_dir)
    for obj in objects:
        # Only add if not exists or update?
        # scan_downloaded_objects creates new records.
        # We should check if it exists to preserve render status if any.
        existing = manifest.get_object(obj.id)
        if existing:
            # Update fields but keep status if already success
            # Actually, just keep existing unless we want to force update
            pass
        else:
            manifest.add_object(obj)
            
    manifest.save()
    
    stats = manifest.get_stats()
    print(f"\nFinal Statistics:")
    print(f"Total objects: {stats['total']}")
    print(f"Smithsonian: {len([o for o in manifest.get_all_objects() if o.source == 'smithsonian'])}")
    print(f"Objaverse++: {len([o for o in manifest.get_all_objects() if o.source == 'objaverse-plusplus'])}")

if __name__ == "__main__":
    main()
