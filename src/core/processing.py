"""
processing.py
-------------
Core logic for corridor-based LAS processing:
- Filtering LAS points within corridor polygons
- Multi-file processing logic
- Downsampling (nth point)
- Coordinating transformations
"""

import logging
import math
import time
import threading
import queue
from pathlib import Path
from typing import List, Optional, Tuple, Dict

import laspy
import numpy as np
from pyproj import CRS, Transformer
from shapely.geometry import Polygon, box

# Import from our own modules:
from .geometry import (
    transform_polygon,
    calculate_corridor_polygon,
    points_in_polygon_chunk,
)
from .file_operations import (
    get_las_files_from_directory,
    validate_las_file,
    download_required_files,
    inspect_las_file,
)


def select_las_files_for_corridor(
    las_files_list: List[Path],
    corridor_polygon: Polygon,
    corridor_crs: CRS,
    default_las_crs: Optional[CRS] = None,
) -> List[Path]:
    """
    Select from a list of LAS files those that intersect with the corridor polygon.

    Args:
        las_files_list: The LAS file paths to check.
        corridor_polygon: The corridor polygon in its CRS.
        corridor_crs: The CRS of the corridor.
        default_las_crs: Default CRS if LAS files lack CRS info.

    Returns:
        The subset of files that intersect the corridor polygon.
    """
    logging.info("Selecting LAS files from the provided list.")
    selected_files: List[Path] = []
    total_files = len(las_files_list)
    logging.info(f"Files to check: {total_files}")

    for idx, las_file_path in enumerate(las_files_list, start=1):
        logging.info(f"Checking {idx}/{total_files}: {las_file_path.name}")
        try:
            with laspy.open(las_file_path) as las:
                las_crs = las.header.parse_crs()
                if las_crs is None:
                    if default_las_crs is not None:
                        logging.warning(
                            f"No CRS found in {las_file_path}. Using default EPSG:{default_las_crs.to_epsg()}."
                        )
                        las_crs = default_las_crs
                    else:
                        logging.error(
                            f"No CRS in {las_file_path} and no default provided. Skipping."
                        )
                        continue

                # Get bounding box of the LAS file
                min_x, min_y, _ = las.header.min
                max_x, max_y, _ = las.header.max
                las_bounds = box(min_x, min_y, max_x, max_y)

                # Transform bounding box if needed
                if not las_crs.equals(corridor_crs):
                    transformer = Transformer.from_crs(
                        las_crs, corridor_crs, always_xy=True
                    )
                    corners = [
                        (min_x, min_y),
                        (min_x, max_y),
                        (max_x, min_y),
                        (max_x, max_y),
                    ]
                    transformed = [transformer.transform(xc, yc) for xc, yc in corners]
                    xs, ys = zip(*transformed)
                    las_bounds = box(min(xs), min(ys), max(xs), max(ys))

                if las_bounds.intersects(corridor_polygon):
                    selected_files.append(las_file_path)
                    logging.info(f"Selected: {las_file_path.name}")
                else:
                    logging.info(f"No intersection for {las_file_path.name}")
        except Exception as exc:
            logging.error(f"Error reading {las_file_path.name}: {exc}")
            continue

    return selected_files


def process_las_file(
    las_file_path: Path,
    corridor_polygon: Polygon,
    corridor_crs: CRS,
    writer: laspy.LasWriter,
    nth_point: int = 1,
    cancel_event: Optional[threading.Event] = None,
    queue_obj: Optional[queue.Queue] = None,
    file_number: Optional[int] = None,
    total_files: Optional[int] = None,
    default_las_crs: Optional[CRS] = None,
) -> Optional[Dict[str, float]]:
    """
    Process a single LAS file: filter by corridor polygon, transform coords, downsample, write output.

    Args:
        las_file_path: Input LAS file path.
        corridor_polygon: Polygon representing the corridor in corridor CRS.
        corridor_crs: The CRS of the corridor and output file.
        writer: LAS writer instance for output.
        nth_point: Downsampling rate (take every nth point).
        cancel_event: Event to signal cancellation.
        queue_obj: For updating GUI progress.
        file_number: Current file index being processed.
        total_files: Total number of files to process.
        default_las_crs: Default CRS if input LAS lacks CRS.

    Returns:
        A dictionary of stats if successful, else None.
    """
    processing_msg = (
        f"\nProcessing file {file_number} of {total_files}: {las_file_path.name}"
    )
    logging.info("=" * len(processing_msg))
    logging.info(processing_msg)
    logging.info("=" * len(processing_msg))

    file_stats = {
        "file_name": las_file_path.name,
        "total_points": 0,
        "points_processed": 0,
        "points_within_corridor": 0,
        "points_written": 0,
        "processing_time": 0.0,
    }

    start_time = time.time()
    inspection_report = inspect_las_file(las_file_path)
    logging.info(inspection_report)

    try:
        with laspy.open(las_file_path) as inlas:
            las_crs = inlas.header.parse_crs()
            if las_crs is None:
                if default_las_crs:
                    logging.warning(
                        f"No CRS in {las_file_path}, using default EPSG:{default_las_crs.to_epsg()}."
                    )
                    las_crs = default_las_crs
                else:
                    logging.error(
                        f"No CRS in {las_file_path} and no default provided. Skipping."
                    )
                    return None

            # Transform corridor polygon to the LAS file's CRS for filtering
            corridor_in_las_crs = transform_polygon(
                corridor_polygon, corridor_crs, las_crs
            )
            transformer_to_corridor_crs = None
            if not las_crs.equals(corridor_crs):
                transformer_to_corridor_crs = Transformer.from_crs(
                    las_crs, corridor_crs, always_xy=True
                )

            total_points = inlas.header.point_count
            file_stats["total_points"] = total_points

            chunk_size = 1_000_000
            last_logged_progress = -1

            for point_chunk in inlas.chunk_iterator(chunk_size):
                if cancel_event and cancel_event.is_set():
                    logging.info(f"Canceled processing {las_file_path.name}")
                    return None

                x = point_chunk.x
                y = point_chunk.y

                # Determine which points are within the corridor
                in_corridor = points_in_polygon_chunk(x, y, corridor_in_las_crs)
                num_points_chunk = len(point_chunk)
                num_points_corridor = np.sum(in_corridor)

                file_stats["points_processed"] += num_points_chunk
                file_stats["points_within_corridor"] += num_points_corridor

                if num_points_corridor > 0:
                    filtered_chunk = point_chunk[in_corridor]
                    # Downsampling
                    if nth_point > 1:
                        indices = np.arange(0, len(filtered_chunk), nth_point)
                        filtered_chunk = filtered_chunk[indices]

                    # Transform coordinates if needed
                    if transformer_to_corridor_crs:
                        x_coords = filtered_chunk.x
                        y_coords = filtered_chunk.y
                        z_coords = filtered_chunk.z
                        x_trans, y_trans = transformer_to_corridor_crs.transform(
                            x_coords, y_coords
                        )
                        filtered_chunk.x = x_trans
                        filtered_chunk.y = y_trans
                        filtered_chunk.z = z_coords

                    # Write to the shared output writer
                    writer.write_points(filtered_chunk)
                    file_stats["points_written"] += len(filtered_chunk)

                # Update progress in 10% increments or custom intervals
                progress = int((file_stats["points_processed"] / total_points) * 100)
                if queue_obj:
                    queue_obj.put(("UPDATE_PROGRESS", progress))
                if progress >= last_logged_progress + 10:
                    logging.info(
                        f"File {file_number}/{total_files} "
                        f"({las_file_path.name}) - {progress}% complete"
                    )
                    last_logged_progress = progress

            file_stats["processing_time"] = time.time() - start_time
            completion_msg = (
                f"Completed file {file_number} of {total_files} ({las_file_path.name}): "
                f"{file_stats['points_written']:,} points written in {file_stats['processing_time']:.2f}s"
            )
            logging.info("=" * len(completion_msg))
            logging.info(completion_msg)
            logging.info("=" * len(completion_msg))

            return file_stats

    except Exception as exc:
        logging.error(f"Error processing {las_file_path.name}: {exc}")
        return None


def process_corridor(
    x_start: float,
    y_start: float,
    x_end: float,
    y_end: float,
    corridor_half_width: float,
    source_directory: str,
    output_file_path: str,
    nth_point: int = 1,
    source_option: int = 1,
    network_directory: Optional[str] = None,
    cancel_event: Optional[threading.Event] = None,
    queue_obj: Optional[queue.Queue] = None,
    corridor_epsg_code: Optional[int] = None,
    default_las_epsg_code: Optional[int] = None,
) -> bool:
    """
    Main function to process a corridor:
      1. Build a corridor polygon from provided coordinates.
      2. Retrieve LAS files from local or network sources.
      3. Select LAS files intersecting the corridor polygon.
      4. Filter and write points to a new LAS file.
      5. Optionally download missing files (source_option=3).
      6. Update GUI with progress if queue_obj provided.

    Args:
        x_start, y_start, x_end, y_end: Corridor line segment coordinates.
        corridor_half_width: Corridor half-width.
        source_directory: Path to local source directory.
        output_file_path: Output LAS file path.
        nth_point: Downsampling factor.
        source_option: 1=local only, 2=network only, 3=download then local.
        network_directory: Network directory path if needed.
        cancel_event: Event for cancellation support.
        queue_obj: Queue for GUI progress updates.
        corridor_epsg_code: Corridor EPSG code.
        default_las_epsg_code: Default LAS EPSG code if missing.

    Returns:
        True if processing succeeded and points were written, else False.
    """
    from .geometry import calculate_corridor_polygon  # local import to avoid cycles
    from .file_operations import get_las_files_from_directory, download_required_files

    source_dir_path = Path(source_directory).resolve()
    network_dir_path = Path(network_directory).resolve() if network_directory else None
    logging.info(f"Source directory: {source_dir_path}")
    logging.info(f"Network directory: {network_dir_path}")

    try:
        # Default corridor CRS if none is given
        if corridor_epsg_code is None:
            corridor_epsg_code = 25832
        corridor_crs = CRS.from_epsg(corridor_epsg_code)

        default_las_crs = (
            CRS.from_epsg(default_las_epsg_code) if default_las_epsg_code else None
        )

        # Build the corridor polygon in corridor_crs
        corridor_polygon = calculate_corridor_polygon(
            x_start, y_start, x_end, y_end, corridor_half_width
        )
        corridor_length = math.hypot(x_end - x_start, y_end - y_start)
        logging.info(
            f"\nCorridor Stats: Area={corridor_polygon.area:.2f}m² "
            f"Length={corridor_length:.2f}m Width={corridor_half_width*2:.2f}m"
        )

        # Acquire list of LAS files
        if source_option == 1:
            # Local only
            all_las_files = get_las_files_from_directory(source_dir_path)
        elif source_option == 2:
            # Network only
            if not network_dir_path:
                logging.error("Network directory not specified.")
                return False
            all_las_files = get_las_files_from_directory(network_dir_path)
        elif source_option == 3:
            # Download from network, then use local
            if not network_dir_path:
                logging.error("Network directory not specified.")
                return False
            source_las_files = get_las_files_from_directory(source_dir_path)
            network_las_files = get_las_files_from_directory(network_dir_path)

            network_files_by_name = {f.name: f for f in network_las_files}
            file_dict = {f.name: f for f in source_las_files}
            for f in network_las_files:
                if f.name not in file_dict:
                    file_dict[f.name] = f
            all_las_files = list(file_dict.values())
        else:
            logging.error(f"Invalid source_option: {source_option}")
            return False

        if not all_las_files:
            logging.error("No LAS files found in the specified directories.")
            return False

        # Filter LAS files by intersection with corridor
        valid_files = select_las_files_for_corridor(
            all_las_files, corridor_polygon, corridor_crs, default_las_crs
        )
        if not valid_files:
            logging.error("No files intersect the corridor.")
            return False

        # If we are in "download" mode, attempt to download any missing files
        if source_option == 3 and network_dir_path:
            network_files_by_name = {
                f.name: f for f in get_las_files_from_directory(network_dir_path)
            }
            files_to_download = [
                vf.name for vf in valid_files if vf.name in network_files_by_name
            ]

            if files_to_download:
                network_files_needed = [
                    network_files_by_name[name] for name in files_to_download
                ]
                downloaded_files, skipped_files, failed_files = download_required_files(
                    network_files_needed, network_dir_path, source_dir_path
                )

                # Replace references with local paths if downloaded
                for idx, vf in enumerate(valid_files):
                    if vf.name in network_files_by_name:
                        local_file = source_dir_path / vf.name
                        if local_file.exists():
                            valid_files[idx] = local_file
                        else:
                            logging.error(f"Failed to download {vf.name}, skipping.")

                # Filter out any that still don't exist
                valid_files = [f for f in valid_files if f.exists()]

            if not valid_files:
                logging.error("No valid local files found after download attempts.")
                return False

        # Prepare an output LAS writer
        output_path = Path(output_file_path).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use the first file as a template for the output header
        with laspy.open(valid_files[0]) as template:
            header = laspy.LasHeader(
                point_format=template.header.point_format,
                version=template.header.version,
            )
            header.scales = template.header.scales
            header.offsets = template.header.offsets
            header.add_crs(corridor_crs)

        total_points_processed = 0
        total_points_written = 0
        processing_stats = []

        # Open output in "write" mode
        with laspy.open(output_path, mode="w", header=header) as writer:
            # Process each file in a loop
            for idx, file_path in enumerate(valid_files, start=1):
                if cancel_event and cancel_event.is_set():
                    logging.info("Processing canceled by user.")
                    break
                stats = process_las_file(
                    file_path,
                    corridor_polygon,
                    corridor_crs,
                    writer,
                    nth_point,
                    cancel_event,
                    queue_obj,
                    file_number=idx,
                    total_files=len(valid_files),
                    default_las_crs=default_las_crs,
                )
                if stats is None:
                    logging.warning(f"Skipping file {file_path.name} due to errors.")
                    continue
                total_points_processed += stats["points_processed"]
                total_points_written += stats["points_written"]
                processing_stats.append(stats)

        # Summaries
        if not (cancel_event and cancel_event.is_set()):
            logging.info("\nProcessing Summary:")
            logging.info(f"Processed: {len(processing_stats)}/{len(valid_files)} files")
            logging.info(f"Points processed: {total_points_processed:,}")
            logging.info(f"Points written: {total_points_written:,}")
            logging.info(f"Output: {output_path}")

        return total_points_written > 0

    except Exception as exc:
        logging.exception("Processing failed.")
        if Path(output_file_path).exists():
            Path(output_file_path).unlink()
        return False
