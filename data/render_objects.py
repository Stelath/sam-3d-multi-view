import bpy
import os
import sys
import argparse
import math
import mathutils

def reset_scene():
    bpy.ops.object.select_all(action='SELECT')
    bpy.ops.object.delete()

def setup_lighting():
    # Create a light
    bpy.ops.object.light_add(type='SUN', radius=1, location=(0, 0, 5))
    light = bpy.context.active_object
    light.data.energy = 5
    
    # Add an area light for fill
    bpy.ops.object.light_add(type='AREA', radius=5, location=(5, 5, 5))
    area = bpy.context.active_object
    area.data.energy = 50

def normalize_object(obj):
    # Center object
    bpy.ops.object.origin_set(type='ORIGIN_GEOMETRY', center='BOUNDS')
    obj.location = (0, 0, 0)
    
    # Scale object to fit in unit sphere
    dim = obj.dimensions
    max_dim = max(dim)
    if max_dim > 0:
        scale_factor = 1.0 / max_dim
        obj.scale = (scale_factor, scale_factor, scale_factor)
    
    bpy.ops.object.transform_apply(location=True, rotation=True, scale=True)

def setup_camera():
    bpy.ops.object.camera_add(location=(0, -2.5, 1))
    cam = bpy.context.active_object
    cam.data.lens = 35
    
    # Look at origin constraint
    constraint = cam.constraints.new(type='TRACK_TO')
    constraint.target = bpy.data.objects.new("Target", None) # Empty at origin
    constraint.track_axis = 'TRACK_NEGATIVE_Z'
    constraint.up_axis = 'UP_Y'
    
    # Set as active camera for the scene
    bpy.context.scene.camera = cam
    
    return cam

def configure_rendering(output_path):
    bpy.context.scene.render.image_settings.file_format = 'PNG'
    bpy.context.scene.render.filepath = output_path
    bpy.context.scene.render.resolution_x = 512
    bpy.context.scene.render.resolution_y = 512
    
    # Enable transparency
    bpy.context.scene.render.film_transparent = True
    
    # Use Cycles for better quality or Eevee for speed. Eevee is fine for simple synthetic data.
    bpy.context.scene.render.engine = 'BLENDER_EEVEE'

def render_views(obj, output_dir, object_id):
    cam = setup_camera()
    
    # 6 views: Front, Back, Left, Right, Top, Bottom? 
    # Or 6 rotations around Z? Let's do 6 rotations around Z for now + maybe some elevation variation.
    # User asked for "different viewpoints". 
    # Let's do 6 azimuth angles at a fixed elevation.
    
    for i in range(6):
        angle = (i / 6.0) * 2 * math.pi
        
        # Orbit camera
        dist = 2.2
        x = dist * math.sin(angle)
        y = -dist * math.cos(angle)
        z = 1.0 # Slight elevation
        
        cam.location = (x, y, z)
        
        # Render RGB with alpha
        image_path = os.path.join(output_dir, f"{object_id}_view_{i}.png")
        bpy.context.scene.render.filepath = image_path
        bpy.ops.render.render(write_still=True)
        
        # Create mask from alpha channel
        # Save a separate mask file by reading the rendered PNG
        mask_path = os.path.join(output_dir, f"{object_id}_view_{i}_mask.png")
        
        # Use Blender's image functions to extract alpha
        try:
            # Load the rendered image
            img = bpy.data.images.load(image_path)
            
            # Create a new image for the mask
            mask_img = bpy.data.images.new(name="mask", width=img.size[0], height=img.size[1], alpha=False, float_buffer=False)
            
            # Convert alpha channel to grayscale mask
            pixels = list(img.pixels)
            mask_pixels = []
            for i_pix in range(0, len(pixels), 4):  # RGBA
                alpha = pixels[i_pix + 3]  # Get alpha channel
                mask_pixels.extend([alpha, alpha, alpha, 1.0])  # Convert to grayscale RGBA
            
            mask_img.pixels = mask_pixels
            mask_img.filepath_raw = mask_path
            mask_img.file_format = 'PNG'
            mask_img.save()
            
            # Clean up
            bpy.data.images.remove(img)
            bpy.data.images.remove(mask_img)
        except Exception as e:
            print(f"Warning: Could not create mask for view {i}: {e}")

def main():
    # Parse args
    # Blender args are passed after "--"
    argv = sys.argv
    if "--" in argv:
        argv = argv[argv.index("--") + 1:]
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to input 3D file")
    parser.add_argument("--output_dir", required=True, help="Directory to save renders")
    args = parser.parse_args(argv)
    
    reset_scene()
    setup_lighting()
    
    # Import object
    ext = os.path.splitext(args.input)[1].lower()
    try:
        if ext in ['.glb', '.gltf']:
            bpy.ops.import_scene.gltf(filepath=args.input)
        elif ext == '.obj':
            bpy.ops.import_scene.obj(filepath=args.input)
        elif ext == '.fbx':
            bpy.ops.import_scene.fbx(filepath=args.input)
        else:
            print(f"Unsupported file format: {ext}")
            return
    except Exception as e:
        print(f"Failed to import {args.input}: {e}")
        return

    # Select the imported object(s)
    bpy.ops.object.select_all(action='DESELECT')
    mesh_objs = [o for o in bpy.context.scene.objects if o.type == 'MESH']
    
    if not mesh_objs:
        print("No mesh found in file.")
        return
        
    # Select all meshes
    for obj in mesh_objs:
        obj.select_set(True)
    
    # Set active object to the first mesh
    bpy.context.view_layer.objects.active = mesh_objs[0]
    
    # Join meshes if there are multiple
    if len(mesh_objs) > 1:
        bpy.ops.object.join()
    
    # Now we have one object
    obj = bpy.context.active_object
    normalize_object(obj)
    
    object_id = os.path.splitext(os.path.basename(args.input))[0]
    configure_rendering(args.output_dir)
    
    render_views(bpy.context.active_object, args.output_dir, object_id)

if __name__ == "__main__":
    main()
