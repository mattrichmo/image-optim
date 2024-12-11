import os
import sys
import shutil
import subprocess
import logging
import json
import re
from pathlib import Path
from io import BytesIO
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from PIL import Image, ImageFile, ImageOps

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Handle truncated images gracefully
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Define supported image extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff'}

def is_image_file(file_path):
    return file_path.suffix.lower() in IMAGE_EXTENSIONS

def strip_exif(image):
    """
    Strips EXIF data from the image.
    """
    try:
        data = list(image.getdata())
        image_no_exif = Image.new(image.mode, image.size)
        image_no_exif.putdata(data)
        return image_no_exif
    except Exception as e:
        logging.warning(f"Failed to strip EXIF data: {e}")
        return image  # Return original image if stripping fails

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
    try:
        image.save(buffer, format=image_format, **save_kwargs)
    except Exception as e:
        logging.error(f"Failed to optimize image: {e}")
        return None
    buffer.seek(0)
    return buffer

def resize_image(image, scale_factor):
    """
    Resize image by the given scale factor.
    """
    try:
        new_size = (max(1, image.width // scale_factor), max(1, image.height // scale_factor))
        return image.resize(new_size, Image.LANCZOS)
    except Exception as e:
        logging.error(f"Failed to resize image: {e}")
        return image  # Return original image if resizing fails

def create_minified_image(image, min_path, image_format, quality=75, convert_to_webp=False):
    """
    Creates a minified version of the image with 1/8 the original dimensions and optimizes it.
    """
    # Resize to 1/8 of original dimensions
    min_image = resize_image(image, scale_factor=2)  # Half the dimensions (1/4 area)
    min_image = resize_image(min_image, scale_factor=2)  # Quarter the dimensions (1/16 area total)

    if convert_to_webp:
        min_path = min_path.with_suffix('.webp')
        image_format = 'WEBP'

    # Create 'min' directory if it doesn't exist
    min_directory = min_path.parent / "min"
    min_directory.mkdir(exist_ok=True)

    # Update min_path to be inside the 'min' directory
    min_path = min_directory / min_path.name

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

    try:
        min_image.save(min_path, format=image_format, **save_kwargs)
    except Exception as e:
        logging.error(f"Failed to save minified image {min_path}: {e}")
        return None

    # Further optimize using external tools if available
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

def create_thumbnail_image(image, thumb_path, image_format, quality=60, size=(150, 150)):
    """
    Creates a thumbnail version of the image with specified dimensions and optimizes it.
    """
    # Resize to thumbnail dimensions
    thumb_image = image.copy()
    thumb_image.thumbnail(size, Image.LANCZOS)

    if image_format.upper() == 'WEBP':
        thumb_path = thumb_path.with_suffix('.webp')
        image_format = 'WEBP'

    # Create 'thumb' directory if it doesn't exist
    thumb_directory = thumb_path.parent / "thumb"
    thumb_directory.mkdir(exist_ok=True)

    # Update thumb_path to be inside the 'thumb' directory
    thumb_path = thumb_directory / thumb_path.name

    save_kwargs = {}
    if image_format.upper() in ['JPEG', 'JPG']:
        save_kwargs['quality'] = quality
        save_kwargs['optimize'] = True
        save_kwargs['progressive'] = True
    elif image_format.upper() == 'PNG':
        save_kwargs['optimize'] = True
        save_kwargs['compress_level'] = 9
        thumb_image = ImageOps.autocontrast(thumb_image)
        if thumb_image.mode != 'P':
            thumb_image = thumb_image.convert('P', palette=Image.ADAPTIVE)
    elif image_format.upper() == 'WEBP':
        save_kwargs['quality'] = quality
        save_kwargs['method'] = 6

    try:
        thumb_image.save(thumb_path, format=image_format, **save_kwargs)
    except Exception as e:
        logging.error(f"Failed to save thumbnail image {thumb_path}: {e}")
        return None

    return thumb_path

def process_image(file_path, root_folder):
    """
    Process a single image: strip EXIF, optimize, create minified and thumbnail versions, and prepare metadata.
    """
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
            if optimized_buffer:
                with open(file_path, 'wb') as f:
                    f.write(optimized_buffer.getvalue())
                logging.info(f"Stripped EXIF and optimized: {file_path}")
            else:
                logging.warning(f"Skipping optimization for {file_path} due to errors.")

            # Create minified image with 1/8 original dimensions
            min_filename = f"{file_path.stem}-min{file_path.suffix}"
            min_path = file_path.parent / min_filename

            convert_to_webp = False  # Set to True if you want to convert minified images to WebP

            min_path = create_minified_image(
                img_no_exif,
                min_path,
                image_format,
                quality=75,
                convert_to_webp=convert_to_webp
            )

            if min_path:
                logging.info(f"Created minified image: {min_path}")
            else:
                logging.warning(f"Failed to create minified image for {file_path}")

            # Create thumbnail image
            thumb_filename = f"{file_path.stem}-thumb{file_path.suffix}"
            thumb_path = file_path.parent / thumb_filename

            thumb_path = create_thumbnail_image(
                img_no_exif,
                thumb_path,
                image_format,
                quality=60
            )

            if thumb_path:
                logging.info(f"Created thumbnail image: {thumb_path}")
            else:
                logging.warning(f"Failed to create thumbnail image for {file_path}")

            # Prepare metadata
            relative_url = file_path.relative_to(root_folder.parent).as_posix()
            relative_min = min_path.relative_to(root_folder.parent).as_posix() if min_path else ""
            relative_thumb = thumb_path.relative_to(root_folder.parent).as_posix() if thumb_path else ""

            photo_entry = {
                "title": file_path.stem,
                "meta": {
                    "description": "",
                    "keywords": [""],
                    "category": [""]
                },
                "img": {
                    "url": relative_url,
                    "min": relative_min,
                    "thumb": relative_thumb
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

def process_directory(directory_path, root_photos, executor):
    """
    Process all images in a directory concurrently.
    """
    image_files = [
        file for file in directory_path.iterdir()
        if file.is_file() and is_image_file(file)
    ]

    if not image_files:
        logging.info(f"No image files found in {directory_path}. Skipping.")
        return [], None

    logging.info(f"Processing {len(image_files)} image(s) in {directory_path}.")

    futures = {
        executor.submit(process_image, file, root_photos): file for file in image_files
    }

    photos_data = []
    for future in as_completed(futures):
        result = future.result()
        if result:
            photos_data.append(result)

    if not photos_data:
        logging.info(f"No valid images processed in {directory_path}. Skipping JSON generation.")
        return [], None

    # Generate photos.json for the directory
    json_data = {
        "photos": photos_data
    }

    json_file_path = directory_path / "photos.json"
    try:
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, indent=4)
        logging.info(f"JSON file created at: {json_file_path}")
    except Exception as e:
        logging.error(f"Failed to write JSON file at {json_file_path}: {e}")

    # Create series metadata
    series_metadata = create_series_metadata(directory_path)

    return photos_data, series_metadata

def aggregate_all_photos(root_photos):
    """
    Aggregate all photos from individual photos.json files into allPhotos.json.
    """
    all_photos = []
    for json_file in root_photos.rglob('photos.json'):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_photos.extend(data.get("photos", []))
        except Exception as e:
            logging.error(f"Failed to read {json_file}: {e}")

    if not all_photos:
        logging.info("No photos to aggregate into allPhotos.json.")
        return None

    json_data = {
        "photos": all_photos
    }

    all_photos_json = root_photos / "allPhotos.json"

    try:
        with open(all_photos_json, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, indent=4)
        logging.info(f"Aggregated all photos into: {all_photos_json}")
    except Exception as e:
        logging.error(f"Failed to write allPhotos.json: {e}")

    return all_photos

def aggregate_all_series(root_photos, all_series_data):
    """
    Aggregate all series metadata into allSeries.json.
    """
    if not all_series_data:
        logging.info("No series data to aggregate into allSeries.json.")
        return None

    all_series_json = root_photos / "allSeries.json"

    try:
        with open(all_series_json, 'w', encoding='utf-8') as json_file:
            json.dump(all_series_data, json_file, indent=4)
        logging.info(f"Aggregated all series into: {all_series_json}")
    except Exception as e:
        logging.error(f"Failed to write allSeries.json: {e}")

    return all_series_data

def create_master_json(root_photos, all_photos_data, all_series_data):
    """
    Create master.json which contains both all photos and all series.
    """
    master_json_path = root_photos / "master.json"

    master_data = {}
    if all_photos_data:
        master_data["photos"] = all_photos_data
    if all_series_data:
        master_data["series"] = all_series_data

    try:
        with open(master_json_path, 'w', encoding='utf-8') as json_file:
            json.dump(master_data, json_file, indent=4)
        logging.info(f"Created master.json at: {master_json_path}")
    except Exception as e:
        logging.error(f"Failed to write master.json: {e}")

def process_folder(folder_path):
    """
    Process the entire folder: optimize images, create metadata, and aggregate data.
    """
    root_photos = Path(folder_path).resolve()

    if not root_photos.is_dir():
        logging.error(f"The path {root_photos} is not a valid directory.")
        sys.exit(1)

    # Gather all directories including root if it contains images
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
    all_photos_data = []

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        for directory in all_directories:
            photos, series = process_directory(directory, root_photos, executor)
            if photos:
                all_photos_data.extend(photos)
            if series:
                all_series_data.append(series)

    # Aggregate all photos and series
    aggregated_photos = aggregate_all_photos(root_photos)
    aggregated_series = aggregate_all_series(root_photos, all_series_data)

    # Create master.json with both photos and series data
    create_master_json(root_photos, aggregated_photos, aggregated_series)

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
