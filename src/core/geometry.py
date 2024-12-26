"""
geometry.py
-----------
Geometry-related functions for corridor creation and point-in-polygon checks.
"""

import math
from typing import Optional
import numpy as np
import shapely.ops
from shapely.geometry import Polygon, Point
from shapely import ops as shapely_ops
from pyproj import CRS, Transformer


def transform_polygon(polygon: Polygon, source_crs: CRS, target_crs: CRS) -> Polygon:
    """
    Transform a polygon from a source CRS to a target CRS using a Transformer.

    Args:
        polygon: The input polygon in the source CRS.
        source_crs: The current CRS of the polygon.
        target_crs: The target CRS for transformation.

    Returns:
        A Polygon transformed into the target CRS.
    """
    if not source_crs.equals(target_crs):
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        return shapely_ops.transform(transformer.transform, polygon)
    return polygon


def calculate_corridor_polygon(
    x_start: float,
    y_start: float,
    x_end: float,
    y_end: float,
    corridor_half_width: float,
) -> Polygon:
    """
    Calculate the corridor polygon given a start/end line segment and a half-width.
    The corridor is represented as a rectangular polygon around the line.

    Args:
        x_start, y_start: Start coordinates.
        x_end, y_end: End coordinates.
        corridor_half_width: Half-width of the corridor.

    Returns:
        A shapely Polygon representing the corridor.
    """
    dx = x_end - x_start
    dy = y_end - y_start
    angle = math.atan2(dy, dx)
    perpendicular_angle = angle + (math.pi / 2)

    buffer_x = corridor_half_width * math.cos(perpendicular_angle)
    buffer_y = corridor_half_width * math.sin(perpendicular_angle)

    end_buffer_x = corridor_half_width * math.cos(angle)
    end_buffer_y = corridor_half_width * math.sin(angle)

    corners = [
        (x_start - end_buffer_x + buffer_x, y_start - end_buffer_y + buffer_y),
        (x_start - end_buffer_x - buffer_x, y_start - end_buffer_y - buffer_y),
        (x_end + end_buffer_x - buffer_x, y_end + end_buffer_y - buffer_y),
        (x_end + end_buffer_x + buffer_x, y_end + end_buffer_y + buffer_y),
    ]
    return Polygon(corners)


def points_in_polygon_chunk(
    x_coords: np.ndarray, y_coords: np.ndarray, polygon: Polygon
) -> np.ndarray:
    """
    Efficiently determine which points lie inside a polygon using vectorized Shapely operations.

    Args:
        x_coords: X-coordinates of points.
        y_coords: Y-coordinates of points.
        polygon: Polygon to test points against.

    Returns:
        Boolean np.ndarray mask indicating which points are inside the polygon.
    """
    min_x, min_y, max_x, max_y = polygon.bounds
    points_within_bounds = (
        (x_coords >= min_x)
        & (x_coords <= max_x)
        & (y_coords >= min_y)
        & (y_coords <= max_y)
    )

    if not np.any(points_within_bounds):
        return np.zeros_like(points_within_bounds, dtype=bool)

    filtered_x = x_coords[points_within_bounds]
    filtered_y = y_coords[points_within_bounds]
    candidate_points = np.column_stack((filtered_x, filtered_y))

    # Vectorized point-in-polygon check
    candidate_mask = np.array(
        [polygon.contains(Point(xy)) for xy in candidate_points], dtype=bool
    )

    result_mask = np.zeros_like(points_within_bounds, dtype=bool)
    result_mask[points_within_bounds] = candidate_mask
    return result_mask
