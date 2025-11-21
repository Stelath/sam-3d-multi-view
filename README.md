# Objaverse Dataset Pipeline

Download and render 3D objects from Objaverse-XL with multi-view images and masks.

## Quick Start

```bash
# 1. Download objects (saves to ./data/objaverse)
python data/download_objaverse.py --limit 10

# 2. Render objects (requires Blender 5.0+)
python data/run_rendering.py --output_dir ./data/renders
```

## Installation

```bash
# Install dependencies
pip install "pandas>=2.0" "pyarrow>=14.0" objaverse tqdm

# Blender 5.0+ (for rendering) - REQUIRED
# Download from: https://www.blender.org/download/
```

## Download Script

```bash
# Test with 10 objects
python data/download_objaverse.py --limit 10

# Full dataset (30k objects)
python data/download_objaverse.py --target-count 30000

# Resume if interrupted
python data/download_objaverse.py --resume
```

**Output:**
- `./data/objaverse/` - Downloaded 3D files
- `./data/objaverse/manifest.json` - Tracking file

## Render Script

```bash
# Render all downloaded objects
python data/run_rendering.py --output_dir ./data/renders

# With more workers (faster)
python data/run_rendering.py --num_workers 8

# Resume if interrupted
python data/run_rendering.py --resume
```

**Output:**
- `./data/renders/<obj_id>/<obj_id>_view_0.png` to `view_5.png` - 6 views per object
- `./data/renders/<obj_id>/<obj_id>_view_0_mask.png` to `view_5_mask.png` - 6 masks per object

## Common Options

**Download:**
- `--limit N` - Download only N objects (for testing)
- `--target-count N` - Target number of objects (default: 30000)
- `--download-dir PATH` - Custom download location (default: ./data/objaverse)
- `--resume` - Skip already downloaded objects

**Render:**
- `--manifest PATH` - Path to manifest (default: ./data/objaverse/manifest.json)
- `--output_dir PATH` - Output directory (default: ./renders)
- `--num_workers N` - Parallel renders (default: 4)
- `--timeout SECS` - Timeout per object (default: 60s)
- `--resume` - Skip already rendered
- `--retry_failed` - Retry failed renders
- `--dry_run` - Preview what would be rendered

## Troubleshooting

**Parquet dimension errors:**
```bash
pip install --upgrade "pandas>=2.0" "pyarrow>=14.0"
```

**Some repos fail to clone:**
This is normal - some GitHub repos are deleted/private. The script continues.

**Rendering fails / no images:**
- Make sure you have Blender 5.0+ installed
- Test manually: `blender --version`
- Specify path if needed: `python data/run_rendering.py --blender_path /path/to/blender`

**"No camera" error:**
Fixed in latest version - update `render_objects.py`

## Testing Results

✅ **Download**: 10 objects from multiple repos  
✅ **Render**: 6 views + 6 masks per object  
✅ **Resume**: Works for both download and render

Example output:
```
test_render/
├── MiniRamps_view_0.png (83K)
├── MiniRamps_view_0_mask.png (4.3K)
├── ...
├── MiniRamps_view_5.png (97K)
└── MiniRamps_view_5_mask.png (6.8K)
```
