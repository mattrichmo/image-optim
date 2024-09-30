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
import re

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
        image = ImageOps.autocontrast(image)
        if image.mode != 'P':
            image = image.convert('P', palette=Image.ADAPTIVE)

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
        min_image = ImageOps.autocontrast(min_image)
        if min_image.mode != 'P':
            min_image = min_image.convert('P', palette=Image.ADAPTIVE)
    elif image_format.upper() == 'WEBP':
        save_kwargs['quality'] = quality
        save_kwargs['method'] = 6

    min_image.save(min_path, format=image_format, **save_kwargs)

    try:
        if image_format.upper() in ['JPEG', 'JPG'] and is_tool_available('jpegoptim'):
            subprocess.run(['jpegoptim', '--strip-all', f'--max={quality}', str(min_path)], check=True)
        elif image_format.upper() == 'PNG':
            if is_tool_available('optipng'):
                subprocess.run(['optipng', '-o7', str(min_path)], check=True)
            elif is_tool_available('pngquant'):
                subprocess.run(['pngquant', '--force', '--ext', '.png', '256', str(min_path)], check=True)
    except subprocess.CalledProcessError as e:
        logging.warning(f"External optimizer failed for {min_path}: {e}")

    return min_path

def is_tool_available(tool_name):
    return shutil.which(tool_name) is not None

def process_image(args):
    file_path, root_folder = args
    try:
        file_path = file_path.resolve()

        with Image.open(file_path) as img:
            image_format = img.format
            if image_format.upper() in ['JPEG', 'JPG'] and img.mode not in ['RGB', 'L']:
                img = img.convert('RGB')
            elif image_format.upper() == 'PNG' and img.mode not in ['RGB', 'RGBA', 'P', 'L']:
                img = img.convert('RGBA')

            img_no_exif = strip_exif(img)

            optimized_buffer = optimize_image(img_no_exif, image_format, quality=85)
            with open(file_path, 'wb') as f:
                f.write(optimized_buffer.getvalue())

            logging.info(f"Stripped EXIF and optimized: {file_path}")

            min_filename = f"{file_path.stem}-min{file_path.suffix}"
            min_path = file_path.parent / min_filename

            convert_to_webp = False

            min_path = create_minified_image(img_no_exif, min_path, image_format, quality=75, convert_to_webp=convert_to_webp)

            logging.info(f"Created minified image: {min_path}")

            relative_url = file_path.relative_to(root_folder).as_posix()
            relative_min = min_path.relative_to(root_folder).as_posix()

            photo_entry = {
                "title": file_path.stem,
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
                    "frontPage": False
                }
            }

            return photo_entry

    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None

def create_series_metadata(directory_path):
    """
    Create a series metadata dictionary based on the folder name.
    """
    series_name = directory_path.name
    slug = re.sub(r'[^a-zA-Z0-9]+', '-', series_name.lower()).strip('-')

    series_metadata = {
        "seriesName": series_name,
        "slug": slug,
        "description": "",
        "intentPurpose": "",
        "year": 2024,
        "frontPage": True,
        "keywords": []
    }

    return series_metadata

def process_directory(directory_path, root_photos, pool, all_series_data):
    image_files = [
        file for file in directory_path.iterdir()
        if file.is_file() and is_image_file(file)
    ]

    if not image_files:
        logging.info(f"No image files found in {directory_path}. Skipping.")
        return []

    logging.info(f"Processing {len(image_files)} image(s) in {directory_path}.")

    results = pool.map(process_image, [(file, root_photos) for file in image_files])

    photos_data = [result for result in results if result is not None]

    if not photos_data:
        logging.info(f"No valid images processed in {directory_path}. Skipping JSON generation.")
        return []

    json_data = {
        "photos": photos_data
    }

    json_file_path = directory_path / "photos.json"

    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, indent=4)

    logging.info(f"JSON file created at: {json_file_path}")

    # Add series metadata for the current directory to the all_series_data list
    all_series_data.append(create_series_metadata(directory_path))

    return photos_data

def aggregate_all_photos(root_photos):
    all_photos = []
    for json_file in root_photos.rglob('photos.json'):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            all_photos.extend(data.get("photos", []))

    if not all_photos:
        logging.info("No photos to aggregate into allPhotos.json.")
        return

    json_data = {
        "photos": all_photos
    }

    all_photos_json = root_photos / "allPhotos.json"

    with open(all_photos_json, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, indent=4)

    logging.info(f"Aggregated all photos into: {all_photos_json}")

    return all_photos  # Return the photo data for master.json

def aggregate_all_series(root_photos, all_series_data):
    """
    Writes the aggregated series metadata to allSeries.json at the root.
    """
    all_series_json = root_photos / "allSeries.json"

    with open(all_series_json, 'w', encoding='utf-8') as json_file:
        json.dump(all_series_data, json_file, indent=4)

    logging.info(f"Aggregated all series into: {all_series_json}")

    return all_series_data  # Return the series data for master.json

def create_master_json(root_photos, all_photos_data, all_series_data):
    """
    Create master.json which contains both all photos and all series.
    """
    master_json_path = root_photos / "master.json"

    master_data = {
        "photos": all_photos_data,
        "series": all_series_data
    }

    with open(master_json_path, 'w', encoding='utf-8') as json_file:
        json.dump(master_data, json_file, indent=4)

    logging.info(f"Created master.json at: {master_json_path}")

def process_folder(folder_path):
    root_photos = Path(folder_path).resolve()

    if not root_photos.is_dir():
        logging.error(f"The path {root_photos} is not a valid directory.")
        sys.exit(1)

    all_directories = [dir_path for dir_path in root_photos.rglob('*') if dir_path.is_dir()]

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

    all_series_data = []

    with Pool(cpu_count()) as pool:
        all_photos_lists = []
        for directory in all_directories:
            photos_data = process_directory(directory, root_photos, pool, all_series_data)
            all_photos_lists.extend(photos_data)

    all_photos_data = aggregate_all_photos(root_photos)
    all_series_data = aggregate_all_series(root_photos, all_series_data)

    # Create master.json with both photos and series data
    create_master_json(root_photos, all_photos_data, all_series_data)

    logging.info("Image optimization, series metadata, and JSON generation complete.")

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
