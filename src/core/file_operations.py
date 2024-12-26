"""
file_operations.py
------------------
File handling utilities for LAS corridor processing, including:
- Listing LAS files in directories
- Validating and inspecting LAS files
- Downloading files from a network path
- Converting LAS to TXT
"""

import logging
import shutil
import time
from collections import defaultdict
from datetime import datetime
import threading
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import laspy
import numpy as np


def get_las_files_from_directory(directory: Path) -> List[Path]:
    """
    Retrieve all LAS and LAZ files from a specified directory.

    Args:
        directory: The directory to search.

    Returns:
        A list of file paths for all LAS/LAZ files found.
    """
    if not directory.exists():
        logging.warning(f"Directory {directory} does not exist. Please check the path.")
        return []
    return [f for ext in ("*.las", "*.laz") for f in directory.glob(ext)]


def get_classification_name(code: int) -> str:
    """
    Return a human-readable classification name for a given LAS classification code.
    Uses standard ASPRS classification codes where possible.

    Args:
        code: The classification code.

    Returns:
        A descriptive classification name.
    """
    standard_classifications = {
        0: "Created, never classified",
        1: "Unassigned",
        2: "Ground",
        3: "Low Vegetation",
        4: "Medium Vegetation",
        5: "High Vegetation",
        6: "Building",
        7: "Low Point (noise)",
        8: "Model Key-point",
        9: "Water",
        10: "Rail",
        11: "Road Surface",
        12: "Overlap Points",
        13: "Wire - Guard (Shield)",
        14: "Wire - Conductor (Phase)",
        15: "Transmission Tower",
        16: "Wire-structure Connector",
        17: "Bridge Deck",
        18: "High Noise",
        19: "Overhead Structure",
        20: "Ignored Ground",
        21: "Snow",
        22: "Temporal Exclusion",
    }

    if 23 <= code <= 63:
        return f"Reserved (ASPRS) [{code}]"
    if 64 <= code <= 255:
        return f"User Defined [{code}]"
    return standard_classifications.get(code, f"Unknown [{code}]")


def inspect_las_file(file_path: Path, detailed: bool = True) -> str:
    """
    Generate a textual report about a LAS file: header info, classification counts, etc.

    Args:
        file_path: Path to the LAS file.
        detailed: Whether to generate a detailed report.

    Returns:
        A formatted string report about the LAS file.
    """
    try:
        report = [
            f"\nInspecting LAS file: {file_path}",
            f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-" * 80,
        ]

        las = laspy.read(file_path)

        report.append("FILE INFORMATION:")
        report.append(f"Version: {las.header.version}")
        report.append(f"Point Format ID: {las.header.point_format.id}")
        report.append(f"Point Count: {las.header.point_count:,}")

        file_size_bytes = file_path.stat().st_size
        if file_size_bytes > 1024**3:
            report.append(f"File Size: {file_size_bytes / (1024 ** 3):.2f} GB")
        else:
            report.append(f"File Size: {file_size_bytes / (1024 ** 2):.2f} MB")

        report.append("\nCOORDINATE SYSTEM:")
        report.append(f"X range: {las.header.min[0]:.3f} to {las.header.max[0]:.3f}")
        report.append(f"Y range: {las.header.min[1]:.3f} to {las.header.max[1]:.3f}")
        report.append(f"Z range: {las.header.min[2]:.3f} to {las.header.max[2]:.3f}")

        scales = [float(x) for x in las.header.scales]
        offsets = [float(x) for x in las.header.offsets]
        report.append(f"Scale factors: {scales}")
        report.append(f"Offsets: {offsets}")

        report.append("\nPOINT CLASSIFICATION ANALYSIS:")

        unique_classes, class_counts = np.unique(las.classification, return_counts=True)
        total_points = len(las.points)

        report.append("\nClassifications Found:")
        report.append("-" * 80)
        report.append(f"{'Code':<6} {'Name':<30} {'Count':<15} {'Percentage'}")
        report.append("-" * 80)

        for class_code, count in zip(unique_classes, class_counts):
            name = get_classification_name(int(class_code))
            percentage = (count / total_points) * 100
            report.append(
                f"{int(class_code):<6} {name:<30} {count:<15,} {percentage:>6.2f}%"
            )

        area = (las.header.max[0] - las.header.min[0]) * (
            las.header.max[1] - las.header.min[1]
        )
        if area > 0:
            density = total_points / area
            report.append(f"\nApproximate Point Density: {density:.2f} points/m²")
        else:
            report.append("\nApproximate Point Density: Area is zero, cannot compute.")

        return "\n".join(report)
    except Exception as exc:
        return f"\nError inspecting file {file_path}: {str(exc)}"


def validate_las_file(file_path: Path) -> Tuple[bool, Optional[str], Optional[dict]]:
    """
    Validate a LAS file, ensuring it exists, is non-empty, and has points.

    Args:
        file_path: The LAS file path.

    Returns:
        (valid, error_message, header_info)
        valid: True if valid, False if invalid.
        error_message: Error message if invalid.
        header_info: Dictionary of file header info if valid.
    """
    if not file_path.exists():
        return False, f"File not found: {file_path}", None

    file_size = file_path.stat().st_size
    if file_size == 0:
        return False, f"File is empty: {file_path}", None

    try:
        with laspy.open(file_path) as las:
            header = las.header
            if header.point_count == 0:
                return False, f"No points in file: {file_path}", None

            header_info = {
                "point_count": header.point_count,
                "version": f"{header.version.major}.{header.version.minor}",
                "point_format": header.point_format.id,
                "file_size_gb": file_size / (1024**3),
                "source": "local" if file_path.parent.name != "network" else "network",
                "filepath": file_path,
            }
        return True, None, header_info
    except Exception as exc:
        return False, f"Validation error for {file_path}: {str(exc)}", None


def download_required_files(
    required_files: List[Path], network_directory: Path, local_directory: Path
) -> Tuple[List[Path], List[str], List[str]]:
    """
    Download required LAS files from a network directory to a local directory.
    Skip files that are already up-to-date locally.

    Args:
        required_files: Files needed from the network.
        network_directory: The network directory path.
        local_directory: The local directory path.

    Returns:
        (downloaded_files, skipped_files, failed_files)
    """
    downloaded_files: List[Path] = []
    skipped_files: List[str] = []
    failed_files: List[str] = []

    network_directory = network_directory.resolve()
    logging.info(f"Network source path after normalization: {network_directory}")

    if not network_directory.exists():
        logging.warning(f"Network source '{network_directory}' not available.")
        failed_files = [f.name for f in required_files]
        return downloaded_files, skipped_files, failed_files

    local_directory = local_directory.resolve()
    local_directory.mkdir(parents=True, exist_ok=True)

    logging.info("Checking source files...")
    total_size = sum((f.stat().st_size for f in required_files if f.exists()), 0)
    if total_size > 0:
        logging.info(f"Total download size: {total_size / (1024 ** 3):.2f} GB")

    copied_size = 0
    for file_path in required_files:
        filename = file_path.name
        source_path = file_path
        destination_path = local_directory / filename

        try:
            if source_path.exists():
                if destination_path.exists():
                    source_mtime = source_path.stat().st_mtime
                    dest_mtime = destination_path.stat().st_mtime
                    if source_mtime <= dest_mtime:
                        logging.info(f"Skipping {filename} (up-to-date)")
                        skipped_files.append(filename)
                        continue

                logging.info(f"Copying {filename}...")
                shutil.copy2(source_path, destination_path)
                file_size = source_path.stat().st_size
                copied_size += file_size
                logging.info(f"Copied {filename} ({file_size / (1024 ** 3):.2f} GB)")
                downloaded_files.append(destination_path)
            else:
                failed_files.append(filename)
                logging.warning(f"Source not found: {filename}")
        except Exception as exc:
            failed_files.append(filename)
            logging.error(f"Error copying {filename}: {exc}")

    logging.info("Download summary:")
    logging.info(
        f"Downloaded: {len(downloaded_files)} ({copied_size / (1024 ** 3):.2f} GB)"
    )
    logging.info(f"Skipped: {len(skipped_files)}")
    if failed_files:
        logging.warning("Failed downloads:")
        for filename in failed_files:
            logging.warning(filename)

    return downloaded_files, skipped_files, failed_files


def convert_las_to_txt(
    las_file: Path,
    txt_file: Optional[Path] = None,
    cancel_event: Optional[threading.Event] = None,
) -> bool:
    """
    Convert a LAS file to TXT. Points are sorted by classification, then by coordinates.
    Each line in TXT: class_code,x,y,z

    Args:
        las_file: The LAS file to convert.
        txt_file: The target TXT file path (default: same name as LAS).
        cancel_event: Event for cancellation.

    Returns:
        True if successful, else False.
    """
    if txt_file is None:
        txt_file = las_file.with_suffix(".txt")

    classification_counts: Dict[int, int] = defaultdict(int)
    points_by_class: Dict[int, List[Tuple[float, float, float]]] = defaultdict(list)

    try:
        start_time = time.time()
        logging.info(f"Converting {las_file.name} to TXT...")

        with laspy.open(las_file) as las:
            total_points = las.header.point_count
            chunk_size = 1_000_000
            processed = 0

            for points_chunk in las.chunk_iterator(chunk_size):
                if cancel_event and cancel_event.is_set():
                    logging.info("Conversion canceled.")
                    return False

                classifications = points_chunk.classification
                xs = points_chunk.x
                ys = points_chunk.y
                zs = points_chunk.z

                for c, x_val, y_val, z_val in zip(classifications, xs, ys, zs):
                    c_code = int(c)
                    points_by_class[c_code].append((x_val, y_val, z_val))
                    classification_counts[c_code] += 1

                processed += len(points_chunk)
                progress = (processed / total_points) * 100
                # Example: you could log progress in intervals
                if int(progress) % 10 == 0:
                    logging.info(f"Reading {progress:.1f}%")

        logging.info("Sorting and writing points to TXT...")
        written = 0
        with txt_file.open("w", encoding="utf-8") as outfile:
            for c_code in sorted(points_by_class.keys()):
                sorted_points = sorted(
                    points_by_class[c_code], key=lambda p: (p[0], p[1])
                )
                for x_val, y_val, z_val in sorted_points:
                    if cancel_event and cancel_event.is_set():
                        logging.info("Conversion canceled during write.")
                        return False
                    outfile.write(f"{c_code},{x_val:.3f},{y_val:.3f},{z_val:.3f}\n")
                    written += 1

        elapsed = time.time() - start_time
        logging.info(f"Conversion complete: {written:,} points in {elapsed:.2f}s")
        return True

    except Exception as exc:
        logging.error(f"Error converting {las_file}: {exc}")
        if txt_file.exists():
            txt_file.unlink()
        return False
    finally:
        if cancel_event and cancel_event.is_set() and txt_file.exists():
            txt_file.unlink()
