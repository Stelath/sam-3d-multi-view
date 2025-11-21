import os
import argparse
import subprocess
from tqdm import tqdm
import multiprocessing
import time
from manifest import Manifest, ViewInfo


SUPPORTED_EXTS = ['.glb', '.gltf', '.obj', '.fbx']


def render_object(args_tuple):
    """Render a single object with Blender."""
    obj_record, output_dir, blender_path, script_path, timeout, base_dir = args_tuple
    
    obj_id = obj_record.id
    # Resolve absolute path relative to manifest location
    obj_path = os.path.join(base_dir, obj_record.local_path)
    
    # Create output folder for this object
    obj_output_dir = os.path.join(output_dir, obj_id)
    os.makedirs(obj_output_dir, exist_ok=True)
    
    cmd = [
        blender_path,
        "--background",
        "--python", script_path,
        "--",
        "--input", obj_path,
        "--output_dir", obj_output_dir
    ]
    
    start_time =time.time()
    
    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=timeout
        )
        
        render_time = time.time() - start_time
        
        # Check if views were created
        views = []
        for i in range(6):
            image_path = os.path.join(obj_output_dir, f"{obj_id}_view_{i}.png")
            mask_path = os.path.join(obj_output_dir, f"{obj_id}_view_{i}_mask0001.png")
            
            if os.path.exists(image_path):
                views.append(ViewInfo(
                    view_id=i,
                    image_path=os.path.relpath(image_path, output_dir),
                    mask_path=os.path.relpath(mask_path, output_dir) if os.path.exists(mask_path) else ""
                ))
        
        if len(views) == 6:
            return (obj_id, "success", None, render_time, views)
        else:
            return (obj_id, "failed", f"Only {len(views)}/6 views created", render_time, views)
            
    except subprocess.TimeoutExpired:
        return (obj_id, "failed", f"Timeout after {timeout}s", time.time() - start_time, [])
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode() if e.stderr else "Unknown error"
        return (obj_id, "failed", error_msg[:200], time.time() - start_time, [])
    except Exception as e:
        return (obj_id, "failed", str(e)[:200], time.time() - start_time, [])


def main():
    parser = argparse.ArgumentParser(description="Batch render 3D objects.")
    parser.add_argument("--manifest", default="./data/objaverse/manifest.json", help="Path to download manifest.json")
    parser.add_argument("--output_dir", default="./renders", help="Directory to save renders.")
    parser.add_argument("--blender_path", default="blender", help="Path to blender executable.")
    parser.add_argument("--num_workers", type=int, default=4, help="Number of parallel renders.")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout per object in seconds.")
    parser.add_argument("--resume", action="store_true", help="Skip already rendered objects.")
    parser.add_argument("--retry_failed", action="store_true", help="Retry previously failed renders.")
    parser.add_argument("--dry_run", action="store_true", help="Show what would be rendered without actually rendering.")
    parser.add_argument("--limit", type=int, help="Limit number of objects to render")
    
    args = parser.parse_args()
    
    script_path = os.path.join(os.path.dirname(__file__), "render_objects.py")
    if not os.path.exists(script_path):
        print(f"Error: render_objects.py not found at {script_path}")
        return
    
    # Load manifest
    print(f"Loading manifest from {args.manifest}...")
    manifest = Manifest(args.manifest)
    
    # Get successfully downloaded objects
    downloaded_objects = manifest.get_objects_by_status(download_status="success")
    print(f"Found {len(downloaded_objects)} successfully downloaded objects.")
    
    if len(downloaded_objects) == 0:
        print("No objects to render!")
        return
    
    # Filter based on render status
    if args.resume:
        if args.retry_failed:
            # Render both pending and failed
            to_render = [obj for obj in downloaded_objects 
                        if obj.render_status in ["pending", "failed"]]
        else:
            # Only render pending
            to_render = [obj for obj in downloaded_objects 
                        if obj.render_status == "pending"]
        
        already_rendered = len(downloaded_objects) - len(to_render)
        print(f"Resume mode: {already_rendered} already rendered, {len(to_render)} to render")
    else:
        to_render = downloaded_objects
    
    print(f"Found {len(to_render)} objects to render.")
    
    if args.limit:
        to_render = to_render[:args.limit]
        print(f"Limiting to {args.limit} objects.")
        
    if not to_render:
        print("No objects to render after filtering/limiting!")
        return
    
    print(f"\nRendering {len(to_render)} objects with {args.num_workers} workers...")
    print(f"Timeout: {args.timeout}s per object")
    print(f"Output directory: {args.output_dir}")
    
    if args.dry_run:
        print("\nDRY RUN - would render the following objects:")
        for obj in to_render[:10]:
            print(f"  - {obj.id}: {obj.local_path}")
        if len(to_render) > 10:
            print(f"  ... and {len(to_render) - 10} more")
        return
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Determine base directory for objects (relative to manifest)
    base_dir = os.path.dirname(args.manifest)
    
    # Prepare rendering tasks
    tasks = [
        (obj, args.output_dir, args.blender_path, script_path, args.timeout, base_dir)
        for obj in to_render
    ]
    
    # Render with progress bar
    results = []
    with multiprocessing.Pool(args.num_workers) as pool:
        for result in tqdm(pool.imap_unordered(render_object, tasks), total=len(tasks), desc="Rendering"):
            results.append(result)
            
            # Update manifest with result
            obj_id, status, error, render_time, views = result
            obj = manifest.get_object(obj_id)
            if obj:
                obj.render_status = status
                obj.render_error = error
                obj.render_time_sec = render_time
                obj.views = views
                manifest.add_object(obj)
            
            # Save periodically
            if len(results) % 10 == 0:
                manifest.save()
    
    # Final save
    manifest.save()
    
    # Print statistics
    print("\n" + "="*60)
    print("RENDERING COMPLETE")
    print("="*60)
    
    successes = [r for r in results if r[1] == "success"]
    failures = [r for r in results if r[1] == "failed"]
    
    print("\nResults:")
    print(f"  Successful: {len(successes)}/{len(results)}")
    print(f"  Failed: {len(failures)}/{len(results)}")
    
    if failures:
        print("\nSample failures:")
        for obj_id, status, error, _, _ in failures[:5]:
            print(f"  - {obj_id}: {error}")
    
    avg_time = sum(r[3] for r in successes) / len(successes) if successes else 0
    print(f"\nAverage render time: {avg_time:.2f}s")
    print(f"Manifest updated: {args.manifest}")


if __name__ == "__main__":
    main()
