from pathlib import Path
from PIL import Image
import json
import pandas as pd

def add_image_to_catalog_list(
    image_path, 
    metadata_folder, 
    catalog_json_path,
    root_folder=None  # <-- nova opção
):
    """
    1) Loads catalog as a JSON list
    2) Appends new metadata with RELATIVE path
    3) Saves the list back
    4) Saves individual JSON with extra info
    """

    image_path = Path(image_path)
    extension = image_path.suffix.lower()

    # Define root folder for relative path
    if root_folder is not None:
        root_folder = Path(root_folder).resolve()
        rel_path = image_path.resolve().relative_to(root_folder)
    else:
        rel_path = image_path.name  # fallback: just filename

    if extension not in ['.jpg', '.jpeg', '.png', '.bmp', '.gif']:
        raise ValueError(f"Extension {extension} is not supported as an image!")

    # Load existing catalog
    catalog_json_path = Path(catalog_json_path)
    if catalog_json_path.exists():
        with open(catalog_json_path, "r", encoding="utf-8") as f:
            catalog = json.load(f)
    else:
        catalog = []

    # Check if already exists (by relative path)
    existing = [entry for entry in catalog if entry.get("Path") == str(rel_path)]
    if existing:
        print(f"⚠️ Image already exists in catalog: {rel_path}")
        if image_path.exists():
            image_path.unlink()
            print(f"🗑️  Physical image deleted: {image_path}")
        return None

    # New entry
    info = {
        "Dataset": None,
        "Description": f"Image stored in Data Lake: {image_path.name}",
        "Path": str(image_path).replace("\\", "/"),  # Always forward slashes
        "Format": extension.replace('.', '').upper(),
        "Date": pd.to_datetime(
            image_path.stat().st_mtime, unit='s'
        ).strftime("%Y-%m-%d"),
        "Version": "1.0"
    }

    # Resolution & size for individual JSON
    try:
        with Image.open(image_path) as img:
            width, height = img.size
        extra_info = {
            "Width": width,
            "Height": height,
            "SizeBytes": image_path.stat().st_size
        }
    except Exception as e:
        extra_info = {
            "Width": None,
            "Height": None,
            "SizeBytes": image_path.stat().st_size
        }
        print(f"Error opening image {image_path}: {e}")

    # Save individual JSON
    metadata_folder = Path(metadata_folder)
    metadata_folder.mkdir(parents=True, exist_ok=True)
    individual_json_path = metadata_folder / f"metadados_supply_chain_{image_path.stem}_metadata.json"

    individual_json = info.copy()
    individual_json.update(extra_info)

    with open(individual_json_path, "w", encoding="utf-8") as f:
        json.dump(individual_json, f, indent=4, ensure_ascii=False)

    print(f"✅ Individual JSON created: {individual_json_path}")

    # Append to catalog and save
    catalog.append(info)
    with open(catalog_json_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=4, ensure_ascii=False)

    print(f"✅ Catalog updated: {catalog_json_path}")

    return individual_json

def add_dataset_to_catalog_list(
    dataset_path,
    metadata_folder,
    catalog_json_path,
    root_folder=None
):
    """
    1) Loads catalog as a JSON list
    2) Appends new dataset metadata with RELATIVE path
    3) Saves the list back
    4) Saves individual JSON with optional dataset info
    """

    dataset_path = Path(dataset_path)
    extension = dataset_path.suffix.lower()

    # Define relative path
    if root_folder is not None:
        root_folder = Path(root_folder).resolve()
        rel_path = dataset_path.resolve().relative_to(root_folder)
    else:
        rel_path = dataset_path.name

    if extension not in ['.csv', '.parquet']:
        raise ValueError(f"Extension {extension} is not supported as dataset!")

    # Load catalog as list
    catalog_json_path = Path(catalog_json_path)
    if catalog_json_path.exists():
        with open(catalog_json_path, "r", encoding="utf-8") as f:
            catalog = json.load(f)
    else:
        catalog = []

    # Check if already exists (by relative path)
    existing = [entry for entry in catalog if entry.get("Caminho") == str(rel_path)]
    if existing:
        print(f"⚠️ Dataset already exists in catalog: {rel_path}")
        return None

    # New metadata (matches your style)
    info = {
        "Dataset": dataset_path.stem,
        "Descrição": f"Dataset stored in Data Lake: {dataset_path.name}",
        "Caminho": str(dataset_path).replace("\\", "/"),
        "Formato": extension.replace('.', '').upper(),
        "Data": pd.to_datetime(
            dataset_path.stat().st_mtime, unit='s'
        ).strftime("%Y-%m-%d"),
        "Versão": "1.0"
    }

    # Optional: read basic stats for individual JSON
    try:
        if extension == '.csv':
            df = pd.read_csv(dataset_path, nrows=5)  # read sample
        elif extension == '.parquet':
            df = pd.read_parquet(dataset_path)
        else:
            df = None
    except Exception as e:
        print(f"Error reading dataset {dataset_path}: {e}")
        df = None

    if df is not None:
        extra_info = {
            "NumColumns": len(df.columns),
            "SampleColumns": df.columns.tolist(),
        }
    else:
        extra_info = {
            "NumColumns": None,
            "SampleColumns": []
        }

    # Save individual JSON
    metadata_folder = Path(metadata_folder)
    metadata_folder.mkdir(parents=True, exist_ok=True)
    individual_json_path = metadata_folder / f"metadados_supply_chain_{dataset_path.stem}_metadata.json"

    individual_json = info.copy()
    individual_json.update(extra_info)

    with open(individual_json_path, "w", encoding="utf-8") as f:
        json.dump(individual_json, f, indent=4, ensure_ascii=False)

    print(f"✅ Individual JSON created: {individual_json_path}")

    # Append to catalog and save
    catalog.append(info)
    with open(catalog_json_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=4, ensure_ascii=False)

    print(f"✅ Catalog updated: {catalog_json_path}")

    return individual_json