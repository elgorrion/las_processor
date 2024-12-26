"""
Core modules for las_corridor_processor.
----------------------------------------
This package contains the essential functionality:
- Geometry operations
- File I/O operations
- Processing pipelines
"""

# For convenience, you can import frequently used functions/classes here,
# allowing a shorter import path elsewhere in your code.

from .geometry import (
    transform_polygon,
    calculate_corridor_polygon,
    # ... add other geometry functions as needed
)

from .file_operations import (
    get_las_files_from_directory,
    download_required_files,
    validate_las_file,
    # ... add other file ops as needed
)

from .processing import (
    process_corridor,
    process_las_file,
    select_las_files_for_corridor,
    convert_las_to_txt,
    # ... add other processing functions as needed
)
