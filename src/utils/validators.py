"""
validators.py
-------------
Functions for validating user inputs (EPSG codes, numeric fields, etc.).
"""

from typing import Optional, Tuple
import math

try:
    import tkinter as tk
    from tkinter import messagebox

    # If you want to handle errors with GUI popups,
    # keep these imports. Otherwise, handle them differently.
except ImportError:
    # If in a non-GUI context, you might do something else
    pass


def validate_inputs(
    x_start_str: str,
    y_start_str: str,
    x_end_str: str,
    y_end_str: str,
    corridor_half_width_str: str,
    nth_point_str: str,
) -> Tuple[bool, Optional[str]]:
    """
    Validate user inputs for corridor coordinates, width, and sampling.

    Args:
        x_start_str, y_start_str, x_end_str, y_end_str: Coordinate inputs as strings.
        corridor_half_width_str: Half-width of the corridor as a string.
        nth_point_str: Sampling rate as a string.

    Returns:
        (True, None) if valid, otherwise (False, error_message).
    """
    try:
        x_start = float(x_start_str)
        y_start = float(y_start_str)
        x_end = float(x_end_str)
        y_end = float(y_end_str)
        corridor_half_width = float(corridor_half_width_str)
        nth_point = int(nth_point_str)
    except ValueError as exc:
        return False, f"Invalid input: {exc}"

    if corridor_half_width <= 0:
        return False, "Corridor half-width must be a positive number."
    if nth_point <= 0:
        return False, "Point sampling rate must be a positive integer."

    return True, None


def validate_epsg_code(epsg_str: str, field_name: str) -> Optional[int]:
    """
    Validate an EPSG code is a valid integer. If invalid, show an error message or return None.

    Args:
        epsg_str: The EPSG code as a string.
        field_name: The field name for error messaging.

    Returns:
        The EPSG code as an integer if valid, otherwise None.
    """
    try:
        return int(epsg_str)
    except ValueError:
        # If you have tkinter installed and want to show a pop-up:
        try:
            messagebox.showerror(
                "Input Error",
                f"Please enter a valid EPSG code (integer) for {field_name}.",
            )
        except:
            # Fallback if tkinter is not available or messagebox fails
            print(
                f"[Error] Invalid EPSG for {field_name}: '{epsg_str}' is not an integer."
            )
        return None
