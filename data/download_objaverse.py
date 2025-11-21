import objaverse.xl as oxl
import pandas as pd
import os
import multiprocessing
import argparse
from pathlib import Path
from manifest import Manifest, ObjectRecord


SUPPORTED_EXTS = ['.glb', '.gltf', '.obj', '.fbx', '.ply']

def dummy_callback(*args, **kwargs):
    """Dummy callback for multiprocessing."""
    pass

def scan_downloaded_objects(download_dir):
    """Scan download directory and create manifest records."""
    print("Scanning downloaded objects...")
    objects = []
    supported_exts = {'.glb', '.gltf', '.obj', '.fbx', '.blend', '.stl'}
    
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            ext = Path(file).suffix.lower()
            if ext in supported_exts:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, download_dir)
                
                # Generate simple ID from filename
                obj_id = Path(file).stem[:16]  # Use filename as ID
                
                obj = ObjectRecord(
                    id=obj_id,
                    source_url=f"file://{rel_path}",
                    local_path=rel_path,
                    file_type=ext[1:],
                    source="github",
                    license=None,
                    sha256=obj_id,  # Use ID as sha for now
                    download_status="success"
                )
                objects.append(obj)
    
    return objects


def main():
    parser = argparse.ArgumentParser(description="Download Objaverse-XL objects.")
    parser.add_argument("--target-count", type=int, default=30000, help="Number of objects to download.")
    parser.add_argument("--download-dir", type=str, default="./data/objaverse", help="Directory to save objects.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of downloads (for testing).")
    parser.add_argument("--processes", type=int, default=multiprocessing.cpu_count(), help="Number of processes to use.")
    parser.add_argument("--resume", action="store_true", help="Resume from existing manifest (skip already downloaded).")
    
    args = parser.parse_args()

    print("Initializing Objaverse download...")
    print(f"Target count: {args.target_count}")
    print(f"Download directory: {args.download_dir}")
    
    # Expand user path if needed
    download_dir = os.path.expanduser(args.download_dir)
    os.makedirs(download_dir, exist_ok=True)
    
    # Initialize manifest - save in download dir
    manifest_path = os.path.join(download_dir, "manifest.json")
    manifest = Manifest(manifest_path)
    
    if args.resume:
        stats = manifest.get_stats()
        print("Resuming from existing manifest:")
        print(f"  - Already downloaded: {stats['downloaded']}")

    # Load Objaverse-XL annotations for Smithsonian check
    print("Loading Objaverse-XL annotations...")
    try:
        annotations = oxl.get_annotations(download_dir=args.download_dir)
    except Exception as e:
        print(f"Error loading annotations: {e}")
        return

    # 1. Smithsonian Objects (Priority 1)
    smithsonian_objs = pd.DataFrame()
    if 'source' in annotations.columns:
        smithsonian_objs = annotations[annotations['source'] == 'smithsonian']
        print(f"Found {len(smithsonian_objs)} Smithsonian objects.")
    
    # 2. Objaverse++ High Quality Objects (Priority 2)
    print("Loading Objaverse++ metadata for high quality objects...")
    high_quality_uids = []
    try:
        from datasets import load_dataset
        # Stream dataset to avoid full download
        dataset = load_dataset("cindyxl/ObjaversePlusPlus", split="train", streaming=True)
        
        # Filter for high quality (score >= 3 first, then 2)
        # We need to collect enough UIDs to fill the target count
        # Target = args.target_count
        # We already have smithsonian_objs
        needed = args.target_count - len(smithsonian_objs)
        if needed > 0:
            print(f"Need {needed} more objects from Objaverse++...")
            
            # First pass: Score 3 (Superior)
            print("Collecting Score 3 (Superior) objects...")
            for item in dataset:
                if item.get('score', 0) >= 3:
                    high_quality_uids.append(item['UID'])
                    if len(high_quality_uids) >= needed:
                        break
            
            # Second pass: Score 2 (High) if still needed
            if len(high_quality_uids) < needed:
                print(f"Collecting Score 2 (High) objects (have {len(high_quality_uids)}, need {needed})...")
                # We need to restart iterator or use a new one. Streaming doesn't support reset easily.
                # Re-load dataset for second pass
                dataset_2 = load_dataset("cindyxl/ObjaversePlusPlus", split="train", streaming=True)
                for item in dataset_2:
                    if item.get('score', 0) == 2:
                        high_quality_uids.append(item['UID'])
                        if len(high_quality_uids) >= needed:
                            break
                            
            print(f"Collected {len(high_quality_uids)} high quality UIDs.")
            
    except ImportError:
        print("Warning: 'datasets' library not found. Skipping Objaverse++ filtering.")
        print("Install with: pip install datasets")
    except Exception as e:
        print(f"Error loading Objaverse++: {e}")

    # Combine lists
    # Smithsonian objects are in 'annotations' DataFrame
    # Objaverse++ objects are a list of UIDs (strings)
    
    # --- Download Phase 1: Smithsonian ---
    if not smithsonian_objs.empty:
        print(f"\n--- Phase 1: Downloading {len(smithsonian_objs)} Smithsonian objects ---")
        
        # Filter out already downloaded
        to_download_smith = []
        for _, row in smithsonian_objs.iterrows():
            obj_id = row['fileIdentifier'].split('/')[-1].split('.')[0] # Rough ID extraction
            # Better to use the index or a unique ID if available. 
            # XL uses fileIdentifier as key often.
            # Let's just pass the dataframe to download_objects, it handles checking.
            pass

        # Use oxl to download
        if args.limit:
            smithsonian_objs = smithsonian_objs.head(args.limit)
            
        oxl.download_objects(
            objects=smithsonian_objs,
            download_dir=args.download_dir,
            processes=args.processes,
            handle_found_object=dummy_callback,
            handle_missing_object=dummy_callback
        )
        
    # --- Download Phase 2: Objaverse++ (Legacy) ---
    if high_quality_uids:
        print(f"\n--- Phase 2: Downloading {len(high_quality_uids)} Objaverse++ High Quality objects ---")
        
        if args.limit and len(high_quality_uids) > args.limit:
             high_quality_uids = high_quality_uids[:args.limit]
             
        import objaverse
        
        # objaverse legacy downloads to ~/.objaverse by default.
        # We want them in args.download_dir.
        # We can download them and then move them, or just let them be there and link them in manifest.
        # For simplicity and containment, let's move them.
        
        # Download in batches to avoid memory issues
        batch_size = 1000
        for i in range(0, len(high_quality_uids), batch_size):
            batch_uids = high_quality_uids[i:i+batch_size]
            print(f"Downloading batch {i//batch_size + 1}/{(len(high_quality_uids)-1)//batch_size + 1} ({len(batch_uids)} objects)...")
            
            try:
                objects = objaverse.load_objects(uids=batch_uids, download_processes=args.processes)
                
                # Move objects to our data dir
                target_dir = os.path.join(args.download_dir, "objaverse_legacy")
                os.makedirs(target_dir, exist_ok=True)
                
                for uid, path in objects.items():
                    # Move file
                    filename = os.path.basename(path)
                    dest_path = os.path.join(target_dir, filename)
                    if not os.path.exists(dest_path):
                        os.rename(path, dest_path)
                    
                    # Add to manifest immediately
                    # We need to construct the record manually since we bypassed the XL downloader
                    obj_record = ObjectRecord(
                        id=uid,
                        source_url="", # Unknown for legacy
                        local_path=os.path.relpath(dest_path, args.download_dir),
                        file_type="glb",
                        source="objaverse-plusplus",
                        license="CC-BY", # Most are CC-BY
                        sha256=uid,
                        download_status="success"
                    )
                    manifest.add_object(obj_record)
                
                manifest.save()
                
            except Exception as e:
                print(f"Error downloading batch: {e}")

    # Scan directory to update manifest for Smithsonian objects (Phase 1)
    # (Phase 2 objects are already added)
    print("\nScanning directory for Smithsonian objects...")
    # ... (existing scan logic, but maybe refined to only look at smithsonian folders?)
    # The existing scan logic looks at everything. That's fine.
    
    # We need to make sure we don't duplicate entries or overwrite Phase 2 entries.
    # The existing logic walks the directory.
    # Smithsonian objects are likely in 'smithsonian/...' subfolders.
    # Objaverse++ objects are in 'objaverse_legacy/...' subfolders.
    
    for root, dirs, files in os.walk(args.download_dir):
        for file in files:
            ext = os.path.splitext(file)[1].lower()
            if ext in SUPPORTED_EXTS:
                # Check if already in manifest
                # We need a robust ID.
                # For Smithsonian, ID is usually filename without extension?
                # For Objaverse++, ID is filename (UID).
                
                obj_id = os.path.splitext(file)[0]
                
                # If already in manifest (e.g. from Phase 2), skip
                if manifest.get_object(obj_id):
                    continue
                    
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, args.download_dir)
                
                # Determine source
                source = "unknown"
                if "smithsonian" in rel_path.lower():
                    source = "smithsonian"
                elif "github" in rel_path.lower(): # Objaverse-XL default source
                    source = "github"
                
                # Add to manifest
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
                manifest.add_object(obj)

    manifest.save()
    
    stats = manifest.get_stats()
    print(f"\nDownload complete!")
    print(f"Total objects in manifest: {stats['total']}")
    print(f"  - Smithsonian/Github (XL): {len([o for o in manifest.get_all_objects() if o.source in ['smithsonian', 'github']])}")
    print(f"  - Objaverse++ (Legacy): {len([o for o in manifest.get_all_objects() if o.source == 'objaverse-plusplus'])}")


if __name__ == "__main__":
    main()
