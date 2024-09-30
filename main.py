import os
from pathlib import Path
from PIL import Image, ImageFile, ImageOps
import sys
import subprocess
import shutil
from multiprocessing import Pool, cpu_count
import logging
from io import BytesIO
import json
from collections import defaultdict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# To handle truncated images gracefully
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Define supported image extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff'}

def is_image_file(file_path):
    return file_path.suffix.lower() in IMAGE_EXTENSIONS

def strip_exif(image):
    """
    Strips EXIF data from the image.
    """
    data = list(image.getdata())
    image_no_exif = Image.new(image.mode, image.size)
    image_no_exif.putdata(data)
    return image_no_exif

def optimize_image(image, image_format, quality=85, progressive=True):
    """
    Optimize image without changing its dimensions or quality.
    """
    save_kwargs = {}
    if image_format.upper() in ['JPEG', 'JPG']:
        save_kwargs['quality'] = quality
        save_kwargs['optimize'] = True
        if progressive:
            save_kwargs['progressive'] = True
    elif image_format.upper() == 'PNG':
        save_kwargs['optimize'] = True
        save_kwargs['compress_level'] = 9
        # Convert to palette-based PNG if possible
        image = ImageOps.autocontrast(image)
        if image.mode != 'P':
            image = image.convert('P', palette=Image.ADAPTIVE)
    # Add more formats if needed

    buffer = BytesIO()
    image.save(buffer, format=image_format, **save_kwargs)
    buffer.seek(0)
    return buffer

def create_minified_image(image, min_path, image_format, quality=75, convert_to_webp=False):
    """
    Creates a minified version of the image with half the dimensions and optimizes it.
    """
    width, height = image.size
    min_size = (width // 2, height // 2)
    min_image = image.resize(min_size, Image.LANCZOS)

    if convert_to_webp:
        min_path = min_path.with_suffix('.webp')
        image_format = 'WEBP'

    save_kwargs = {}
    if image_format.upper() in ['JPEG', 'JPG']:
        save_kwargs['quality'] = quality
        save_kwargs['optimize'] = True
        save_kwargs['progressive'] = True
    elif image_format.upper() == 'PNG':
        save_kwargs['optimize'] = True
        save_kwargs['compress_level'] = 9
        # Convert to palette-based
        min_image = ImageOps.autocontrast(min_image)
        if min_image.mode != 'P':
            min_image = min_image.convert('P', palette=Image.ADAPTIVE)
    elif image_format.upper() == 'WEBP':
        save_kwargs['quality'] = quality
        save_kwargs['method'] = 6  # Best compression

    # Save minified image
    min_image.save(min_path, format=image_format, **save_kwargs)

    # Further optimize with external tools
    try:
        if image_format.upper() in ['JPEG', 'JPG'] and is_tool_available('jpegoptim'):
            subprocess.run(['jpegoptim', '--strip-all', f'--max={quality}', str(min_path)], check=True)
        elif image_format.upper() == 'PNG':
            if is_tool_available('optipng'):
                subprocess.run(['optipng', '-o7', str(min_path)], check=True)
            elif is_tool_available('pngquant'):
                subprocess.run(['pngquant', '--force', '--ext', '.png', '256', str(min_path)], check=True)
        elif image_format.upper() == 'WEBP':
            # Typically, Pillow's options suffice, but you can integrate cwebp if needed
            pass
    except subprocess.CalledProcessError as e:
        logging.warning(f"External optimizer failed for {min_path}: {e}")

    return min_path

def is_tool_available(tool_name):
    return shutil.which(tool_name) is not None

def process_image(args):
    """
    Processes a single image: strips EXIF, optimizes, creates a minified version, and collects metadata.
    """
    file_path, root_folder = args
    try:
        # Ensure file_path is absolute
        file_path = file_path.resolve()

        with Image.open(file_path) as img:
            image_format = img.format
            # Ensure image is in a compatible mode
            if image_format.upper() in ['JPEG', 'JPG'] and img.mode not in ['RGB', 'L']:
                img = img.convert('RGB')
            elif image_format.upper() == 'PNG' and img.mode not in ['RGB', 'RGBA', 'P', 'L']:
                img = img.convert('RGBA')

            # Strip EXIF data
            img_no_exif = strip_exif(img)

            # Optimize main image
            optimized_buffer = optimize_image(img_no_exif, image_format, quality=85)
            with open(file_path, 'wb') as f:
                f.write(optimized_buffer.getvalue())

            logging.info(f"Stripped EXIF and optimized: {file_path}")

            # Create minified image
            min_filename = f"{file_path.stem}-min{file_path.suffix}"
            min_path = file_path.parent / min_filename

            # Decide whether to convert to WebP for minified images
            convert_to_webp = False  # Change to True if desired

            min_path = create_minified_image(img_no_exif, min_path, image_format, quality=75, convert_to_webp=convert_to_webp)

            logging.info(f"Created minified image: {min_path}")

            # Prepare data for JSON
            relative_url = file_path.relative_to(root_folder).as_posix()
            relative_min = min_path.relative_to(root_folder).as_posix()

            photo_entry = {
                "title": file_path.stem,  # Using filename as title; modify as needed
                "meta": {
                    "description": "",
                    "keywords": [""],
                    "category": [""]
                },
                "img": {
                    "url": relative_url,
                    "min": relative_min
                },
                "series": {
                    "seriesName": file_path.parent.name,
                    "frontPage": False  # Default value; modify as needed
                }
            }

            return photo_entry

    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None  # Indicate failure for this image

def process_directory(directory_path, root_photos, pool):
    """
    Processes all images in a single directory and generates a photos.json file within it.
    """
    try:
        image_files = [
            file for file in directory_path.iterdir()
            if file.is_file() and is_image_file(file)
        ]

        if not image_files:
            logging.info(f"No image files found in {directory_path}. Skipping.")
            return []

        logging.info(f"Processing {len(image_files)} image(s) in {directory_path}.")

        # Process images using the provided Pool
        results = pool.map(process_image, [(file, root_photos) for file in image_files])

        # Filter out any None results due to errors
        photos_data = [result for result in results if result is not None]

        if not photos_data:
            logging.info(f"No valid images processed in {directory_path}. Skipping JSON generation.")
            return []

        # Create JSON structure
        json_data = {
            "photos": photos_data
        }

        # Define JSON file path within the current directory
        json_file_path = directory_path / "photos.json"

        # Write JSON data to file
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, indent=4)

        logging.info(f"JSON file created at: {json_file_path}")

        return photos_data  # Return the list for aggregation

    except Exception as e:
        logging.error(f"Error processing directory {directory_path}: {e}")
        return []

def aggregate_all_photos(root_photos):
    """
    Aggregates all photos from individual photos.json files into allPhotos.json at the root.
    """
    try:
        all_photos = []
        for json_file in root_photos.rglob('photos.json'):
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_photos.extend(data.get("photos", []))

        if not all_photos:
            logging.info("No photos to aggregate into allPhotos.json.")
            return

        # Create JSON structure
        json_data = {
            "photos": all_photos
        }

        # Define allPhotos.json path at the root_photos
        all_photos_json = root_photos / "allPhotos.json"

        # Write aggregated JSON data to file
        with open(all_photos_json, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, indent=4)

        logging.info(f"Aggregated all photos into: {all_photos_json}")

    except Exception as e:
        logging.error(f"Error aggregating all photos: {e}")

def process_folder(folder_path):
    """
    Recursively processes all subdirectories in the given photos folder.
    Generates individual photos.json files and an aggregated allPhotos.json file.
    """
    root_photos = Path(folder_path).resolve()

    if not root_photos.is_dir():
        logging.error(f"The path {root_photos} is not a valid directory.")
        sys.exit(1)

    # Gather all subdirectories (including root_photos)
    all_directories = [dir_path for dir_path in root_photos.rglob('*') if dir_path.is_dir()]

    # Include the root_photos directory itself if it contains images
    image_files_in_root = [
        file for file in root_photos.iterdir()
        if file.is_file() and is_image_file(file)
    ]
    if image_files_in_root:
        all_directories.append(root_photos)

    if not all_directories:
        logging.info("No directories found to process.")
        return

    logging.info(f"Found {len(all_directories)} directory(ies) to process.")

    # Initialize a single Pool for all image processing tasks
    with Pool(cpu_count()) as pool:
        # Process each directory sequentially to avoid nested Pools
        all_photos_lists = []
        for directory in all_directories:
            photos_data = process_directory(directory, root_photos, pool)
            all_photos_lists.extend(photos_data)

    # Generate individual photos.json files have already been created within process_directory

    # Aggregate all photos into allPhotos.json
    aggregate_all_photos(root_photos)

    logging.info("Image optimization and JSON generation complete.")

def main():
    if len(sys.argv) != 2:
        print("Usage: python optimize_images.py <path_to_photos_folder>")
        sys.exit(1)

    folder_path = Path(sys.argv[1])

    if not folder_path.is_dir():
        print(f"The path {folder_path} is not a valid directory.")
        sys.exit(1)

    process_folder(folder_path)

if __name__ == "__main__":
    main()
