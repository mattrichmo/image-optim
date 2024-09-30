import os
from pathlib import Path
from PIL import Image, ImageFile
import sys

# To handle truncated images gracefully
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Define supported image extensions
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff'}

def is_image_file(file_path):
    return file_path.suffix.lower() in IMAGE_EXTENSIONS

def strip_exif(image, image_format):
    """
    Strips EXIF data from the image.
    """
    data = list(image.getdata())
    image_no_exif = Image.new(image.mode, image.size)
    image_no_exif.putdata(data)
    return image_no_exif

def create_minified_image(image, min_path, image_format):
    """
    Creates a minified version of the image with half the dimensions.
    """
    width, height = image.size
    min_size = (width // 2, height // 2)
    min_image = image.resize(min_size, Image.LANCZOS)  # Updated here

    save_kwargs = {}
    if image_format.upper() in ['JPEG', 'JPG']:
        save_kwargs['quality'] = 85  # Adjust quality as needed
        save_kwargs['optimize'] = True
    elif image_format.upper() == 'PNG':
        save_kwargs['optimize'] = True
    # Add more formats if needed

    min_image.save(min_path, format=image_format, **save_kwargs)


def process_image(file_path):
    """
    Processes a single image: strips EXIF and creates a minified version.
    """
    try:
        with Image.open(file_path) as img:
            image_format = img.format
            # Strip EXIF data
            img_no_exif = strip_exif(img, image_format)
            
            # Save the image without EXIF data
            save_kwargs = {}
            if image_format.upper() in ['JPEG', 'JPG']:
                save_kwargs['quality'] = 85  # Adjust quality as needed
                save_kwargs['optimize'] = True
            elif image_format.upper() == 'PNG':
                save_kwargs['optimize'] = True
            # Add more formats if needed

            img_no_exif.save(file_path, format=image_format, **save_kwargs)
            print(f"Stripped EXIF and optimized: {file_path}")

            # Create minified version
            min_filename = f"{file_path.stem}-min{file_path.suffix}"
            min_path = file_path.parent / min_filename

            create_minified_image(img_no_exif, min_path, image_format)
            print(f"Created minified image: {min_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def process_folder(folder_path):
    """
    Recursively processes all images in the given folder.
    """
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = Path(root) / file
            if is_image_file(file_path):
                process_image(file_path)

def main():
    if len(sys.argv) != 2:
        print("Usage: python optimize_images.py <path_to_folder>")
        sys.exit(1)
    
    folder_path = Path(sys.argv[1])

    if not folder_path.is_dir():
        print(f"The path {folder_path} is not a valid directory.")
        sys.exit(1)
    
    process_folder(folder_path)
    print("Image optimization complete.")

if __name__ == "__main__":
    main()
