"""
Manifest management utilities for tracking download and render progress.
"""
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class ViewInfo:
    """Information about a rendered view."""
    view_id: int
    image_path: str
    mask_path: str


@dataclass
class ObjectRecord:
    """Record for a single 3D object in the dataset."""
    id: str
    source_url: str
    local_path: str
    file_type: str
    source: str
    license: Optional[str]
    sha256: str
    download_status: str  # "success", "failed", "pending"
    download_error: Optional[str] = None
    render_status: str = "pending"  # "success", "failed", "pending"
    render_error: Optional[str] = None
    render_time_sec: Optional[float] = None
    views: List[ViewInfo] = None
    
    def __post_init__(self):
        if self.views is None:
            self.views = []


class Manifest:
    """Manages the dataset manifest file."""
    
    def __init__(self, manifest_path: str):
        self.manifest_path = Path(manifest_path)
        self.data = self._load()
    
    def _load(self) -> Dict[str, Any]:
        """Load manifest from disk or create new."""
        if self.manifest_path.exists():
            with open(self.manifest_path, 'r') as f:
                return json.load(f)
        else:
            return {
                "version": "1.0",
                "created": datetime.now().isoformat(),
                "total_objects": 0,
                "objects": {}
            }
    
    def save(self):
        """Save manifest to disk."""
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.manifest_path, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def add_object(self, obj: ObjectRecord):
        """Add or update an object record."""
        # Convert dataclass to dict, handling nested ViewInfo objects
        obj_dict = asdict(obj)
        self.data["objects"][obj.id] = obj_dict
        self.data["total_objects"] = len(self.data["objects"])
    
    def get_object(self, obj_id: str) -> Optional[ObjectRecord]:
        """Get an object record by ID."""
        obj_dict = self.data["objects"].get(obj_id)
        if obj_dict is None:
            return None
        
        # Convert dict back to dataclass
        # Make a copy to avoid modifying the internal data
        obj_data = obj_dict.copy()
        views = [ViewInfo(**v) if isinstance(v, dict) else v for v in obj_data.get("views", [])]
        obj_data["views"] = views
        return ObjectRecord(**obj_data)
    
    def get_all_objects(self) -> List[ObjectRecord]:
        """Get all object records."""
        objects = []
        for obj_dict in self.data["objects"].values():
            obj_data = obj_dict.copy()
            views = [ViewInfo(**v) if isinstance(v, dict) else v for v in obj_data.get("views", [])]
            obj_data["views"] = views
            objects.append(ObjectRecord(**obj_data))
        return objects
    
    def get_objects_by_status(self, download_status: Optional[str] = None, 
                             render_status: Optional[str] = None) -> List[ObjectRecord]:
        """Filter objects by status."""
        objects = self.get_all_objects()
        
        if download_status is not None:
            objects = [o for o in objects if o.download_status == download_status]
        
        if render_status is not None:
            objects = [o for o in objects if o.render_status == render_status]
        
        return objects
    
    def get_stats(self) -> Dict[str, Any]:
        """Get manifest statistics."""
        all_objs = self.get_all_objects()
        
        return {
            "total": len(all_objs),
            "downloaded": len([o for o in all_objs if o.download_status == "success"]),
            "download_failed": len([o for o in all_objs if o.download_status == "failed"]),
            "download_pending": len([o for o in all_objs if o.download_status == "pending"]),
            "rendered": len([o for o in all_objs if o.render_status == "success"]),
            "render_failed": len([o for o in all_objs if o.render_status == "failed"]),
            "render_pending": len([o for o in all_objs if o.render_status == "pending"]),
        }
