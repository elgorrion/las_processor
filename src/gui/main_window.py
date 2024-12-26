"""
main_window.py
--------------
Defines the main Tkinter-based GUI application (the `Application` class),
including all widget layout, user input handling, and thread-based orchestration.
"""

import logging
import math
import os
import platform
import queue
import subprocess
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
from tkinter import ttk
from pathlib import Path
from typing import List, Optional

# Assuming these are in your repo under src/core and src/utils
# Adjust relative imports to match your package structure
from ..core.file_operations import download_files_from_network, convert_las_to_txt
from ..core.file_operations import convert_las_to_txt
from ..core.processing import process_corridor
from ..utils.validators import validate_inputs, validate_epsg_code

# Import the custom QueueHandler defined in logging_handler.py
from .logging_handler import QueueHandler


class Application(tk.Tk):
    """
    The main GUI application window.
    Provides inputs for corridor parameters, directories, and processing options.
    Includes a progress bar, logging text area, and start/cancel controls.
    """

    def __init__(self) -> None:
        super().__init__()
        self.title("LAS Corridor Processing by VoNa")
        self.style = ttk.Style(self)
        self.style.theme_use("clam")
        self.queue = queue.Queue()
        self.messages: List[str] = []
        self.processing_thread: Optional[threading.Thread] = None
        self.cancel_event = threading.Event()
        self.close_requested = False

        # GUI element creation
        self.create_widgets()
        self.setup_logging()

        # Logging a startup message
        logging.info("Application started.")

        # Handle window close event
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_widgets(self) -> None:
        """
        Create all tkinter widgets: input fields, buttons, progress bar, etc.
        """
        main_frame = ttk.Frame(self, padding="5 5 5 5")
        main_frame.grid(row=0, column=0, sticky=(tk.N, tk.W, tk.E, tk.S))
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        # Coordinates
        coord_frame = ttk.LabelFrame(main_frame, text="Coordinates", padding="5 5 5 5")
        coord_frame.grid(row=0, column=0, sticky=tk.EW)
        coord_frame.columnconfigure(1, weight=1)
        coord_frame.columnconfigure(3, weight=1)

        coords_frame = ttk.Frame(coord_frame)
        coords_frame.grid(row=0, column=0, columnspan=2, sticky="w", padx=(5, 20))

        ttk.Label(coords_frame, text="Start X:").grid(row=0, column=0, sticky="e")
        self.x_start_entry = ttk.Entry(coords_frame, width=15)
        self.x_start_entry.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(coords_frame, text="Start Y:").grid(row=0, column=2, sticky="e")
        self.y_start_entry = ttk.Entry(coords_frame, width=15)
        self.y_start_entry.grid(row=0, column=3, padx=5, pady=5)

        ttk.Label(coords_frame, text="End X:").grid(row=1, column=0, sticky="e")
        self.x_end_entry = ttk.Entry(coords_frame, width=15)
        self.x_end_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(coords_frame, text="End Y:").grid(row=1, column=2, sticky="e")
        self.y_end_entry = ttk.Entry(coords_frame, width=15)
        self.y_end_entry.grid(row=1, column=3, padx=5, pady=5)

        # Corridor CRS
        crs_frame = ttk.Frame(coord_frame)
        crs_frame.grid(row=0, column=2, columnspan=2, sticky="e", padx=5)
        ttk.Label(crs_frame, text="Corridor CRS EPSG Code:").grid(
            row=0, column=0, sticky="e"
        )
        self.corridor_crs_entry = ttk.Entry(crs_frame, width=15)
        self.corridor_crs_entry.grid(row=0, column=1, padx=5, pady=5)
        self.corridor_crs_entry.insert(0, "25832")

        ttk.Label(crs_frame, text="Default LAS CRS EPSG Code:").grid(
            row=1, column=0, sticky="e"
        )
        self.default_las_crs_entry = ttk.Entry(crs_frame, width=15)
        self.default_las_crs_entry.grid(row=1, column=1, padx=5, pady=5)
        self.default_las_crs_entry.insert(0, "25832")

        # Corridor settings
        corridor_frame = ttk.LabelFrame(
            main_frame, text="Corridor Settings", padding="5 5 5 5"
        )
        corridor_frame.grid(row=1, column=0, sticky=tk.EW, pady=10)

        ttk.Label(corridor_frame, text="Corridor half-width (m):").grid(
            row=0, column=0, sticky="e"
        )
        self.buffer_entry = ttk.Entry(corridor_frame, width=20)
        self.buffer_entry.grid(row=0, column=1, padx=5, pady=5)
        self.buffer_entry.insert(0, "80")

        ttk.Label(corridor_frame, text="Point sampling rate (nth point):").grid(
            row=1, column=0, sticky="e"
        )
        self.nth_point_entry = ttk.Entry(corridor_frame, width=20)
        self.nth_point_entry.grid(row=1, column=1, padx=5, pady=5)
        self.nth_point_entry.insert(0, "10")

        # Paths
        path_frame = ttk.LabelFrame(main_frame, text="Paths", padding="5 5 5 5")
        path_frame.grid(row=2, column=0, sticky=tk.EW)
        path_frame.columnconfigure(1, weight=1)

        ttk.Label(path_frame, text="Local source directory:").grid(
            row=0, column=0, sticky="e"
        )
        self.source_dir_entry = ttk.Entry(path_frame)
        self.source_dir_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        source_buttons_frame = ttk.Frame(path_frame)
        source_buttons_frame.grid(row=0, column=2, sticky="e")
        ttk.Button(
            source_buttons_frame, text="Browse...", command=self.browse_source_dir
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            source_buttons_frame, text="Open", command=self.open_source_dir
        ).pack(side=tk.LEFT, padx=2)

        ttk.Label(path_frame, text="Network source directory:").grid(
            row=1, column=0, sticky="e"
        )
        self.network_dir_entry = ttk.Entry(path_frame)
        self.network_dir_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        network_buttons_frame = ttk.Frame(path_frame)
        network_buttons_frame.grid(row=1, column=2, sticky="e")
        ttk.Button(
            network_buttons_frame, text="Browse...", command=self.browse_network_dir
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            network_buttons_frame, text="Open", command=self.open_network_dir
        ).pack(side=tk.LEFT, padx=2)

        ttk.Label(path_frame, text="Output file path:").grid(
            row=2, column=0, sticky="e"
        )
        self.output_file_entry = ttk.Entry(path_frame)
        self.output_file_entry.grid(row=2, column=1, padx=5, pady=5, sticky=tk.EW)
        output_buttons_frame = ttk.Frame(path_frame)
        output_buttons_frame.grid(row=2, column=2, sticky="e")
        ttk.Button(
            output_buttons_frame, text="Browse...", command=self.browse_output_file
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            output_buttons_frame, text="Open", command=self.open_output_file
        ).pack(side=tk.LEFT, padx=2)

        # Sample default paths
        self.source_dir_entry.insert(0, r"C:\LAS-Files")
        self.network_dir_entry.insert(
            0, r"\\ATNAS103\Berichte\2022-06_ARCADIS_EAP_B\2_ALS-Daten\B"
        )
        self.output_file_entry.insert(0, "corridor_output.las")

        # Options
        options_frame = ttk.LabelFrame(main_frame, text="Options", padding="5 5 5 5")
        options_frame.grid(row=3, column=0, sticky=tk.EW, pady=10)

        self.source_option_var = tk.IntVar(value=1)
        source_options_frame = ttk.Frame(options_frame)
        source_options_frame.grid(row=0, column=0, sticky="w", padx=5, pady=5)
        ttk.Label(source_options_frame, text="Source Option:").pack(side=tk.LEFT)
        ttk.Radiobutton(
            source_options_frame,
            text="Use Local Source",
            variable=self.source_option_var,
            value=1,
        ).pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(
            source_options_frame,
            text="Use Network Source",
            variable=self.source_option_var,
            value=2,
        ).pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(
            source_options_frame,
            text="Download from Network to Local First",
            variable=self.source_option_var,
            value=3,
        ).pack(side=tk.LEFT, padx=5)

        self.export_txt_var = tk.BooleanVar()
        self.export_txt_check = ttk.Checkbutton(
            options_frame,
            text="Convert output LAS file to TXT",
            variable=self.export_txt_var,
        )
        self.export_txt_check.grid(row=1, column=0, sticky="w", padx=5, pady=5)

        # Control and progress
        control_frame = ttk.LabelFrame(main_frame, text="Control", padding="5 5 5 5")
        control_frame.grid(row=4, column=0, sticky=tk.EW, pady=10)
        control_frame.columnconfigure(1, weight=1)

        self.start_button = ttk.Button(
            control_frame, text="Start Processing", command=self.start_processing
        )
        self.start_button.grid(row=0, column=0, padx=5, pady=5)
        self.processing_progress = ttk.Progressbar(
            control_frame, orient="horizontal", mode="determinate"
        )
        self.processing_progress.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        self.cancel_button = ttk.Button(
            control_frame,
            text="Cancel Processing",
            command=self.cancel_processing,
            state=tk.DISABLED,
        )
        self.cancel_button.grid(row=0, column=2, padx=5, pady=5)

        self.progress_text = scrolledtext.ScrolledText(
            main_frame, wrap=tk.WORD, height=15, state=tk.DISABLED
        )
        self.progress_text.grid(
            row=5, column=0, sticky=(tk.N, tk.S, tk.E, tk.W), pady=10
        )
        main_frame.rowconfigure(5, weight=1)
        main_frame.columnconfigure(0, weight=1)

        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=6, column=0, sticky=tk.EW, pady=5)
        self.save_log_button = ttk.Button(
            button_frame, text="Save Log", command=self.save_log
        )
        self.save_log_button.pack(side=tk.RIGHT)

    def open_path(self, path_str: str) -> None:
        """
        Open a file or directory in the OS's file explorer if it exists.
        """
        path = Path(path_str)
        if path.exists():
            if platform.system() == "Windows":
                # On Windows: open the directory or select the file
                os_cmd = (
                    ["explorer", str(path)]
                    if path.is_dir()
                    else ["explorer", "/select,", str(path)]
                )
                subprocess.Popen(os_cmd)
            elif platform.system() == "Darwin":
                # On macOS
                subprocess.Popen(["open", str(path)])
            else:
                # Linux or others
                subprocess.Popen(["xdg-open", str(path)])
        else:
            messagebox.showerror("Error", f"Path '{path}' does not exist.")

    def open_source_dir(self) -> None:
        """
        Open the source directory in the file explorer if it exists.
        """
        path = self.source_dir_entry.get().strip()
        if Path(path).is_dir():
            self.open_path(path)
        else:
            messagebox.showerror("Error", f"Directory '{path}' does not exist.")

    def open_network_dir(self) -> None:
        """
        Open the network directory in the file explorer if it exists.
        """
        path = self.network_dir_entry.get().strip()
        if Path(path).is_dir():
            self.open_path(path)
        else:
            messagebox.showerror("Error", f"Directory '{path}' does not exist.")

    def open_output_file(self) -> None:
        """
        Open the output file or its directory in the file explorer if it exists.
        """
        file_path = self.output_file_entry.get().strip()
        p = Path(file_path)
        if p.is_file():
            self.open_path(str(p))
        elif p.parent.is_dir():
            self.open_path(str(p.parent))
        else:
            messagebox.showerror("Error", f"File '{p}' does not exist.")

    def browse_source_dir(self) -> None:
        """
        Open a dialog to browse for the source directory.
        """
        directory = filedialog.askdirectory()
        if directory:
            self.source_dir_entry.delete(0, tk.END)
            self.source_dir_entry.insert(0, directory)

    def browse_network_dir(self) -> None:
        """
        Open a dialog to browse for the network directory.
        """
        directory = filedialog.askdirectory()
        if directory:
            self.network_dir_entry.delete(0, tk.END)
            self.network_dir_entry.insert(0, directory)

    def browse_output_file(self) -> None:
        """
        Open a dialog to choose the output LAS file path.
        """
        file_path = filedialog.asksaveasfilename(
            defaultextension=".las", filetypes=[("LAS files", "*.las")]
        )
        if file_path:
            self.output_file_entry.delete(0, tk.END)
            self.output_file_entry.insert(0, file_path)

    def start_processing(self) -> None:
        """
        Start the processing in a separate thread after validating inputs.
        """
        x_start_str = self.x_start_entry.get().strip()
        y_start_str = self.y_start_entry.get().strip()
        x_end_str = self.x_end_entry.get().strip()
        y_end_str = self.y_end_entry.get().strip()
        corridor_half_width_str = self.buffer_entry.get().strip()
        nth_point_str = self.nth_point_entry.get().strip()

        valid, error_msg = validate_inputs(
            x_start_str,
            y_start_str,
            x_end_str,
            y_end_str,
            corridor_half_width_str,
            nth_point_str,
        )
        if not valid:
            messagebox.showerror("Input Error", error_msg)
            return

        corridor_epsg_code = validate_epsg_code(
            self.corridor_crs_entry.get().strip(), "Corridor CRS"
        )
        if corridor_epsg_code is None:
            return

        default_las_epsg_code = None
        default_epsg_str = self.default_las_crs_entry.get().strip()
        if default_epsg_str:
            default_las_epsg_code = validate_epsg_code(
                default_epsg_str, "Default LAS CRS"
            )
            if default_las_epsg_code is None:
                return

        x_start = float(x_start_str)
        y_start = float(y_start_str)
        x_end = float(x_end_str)
        y_end = float(y_end_str)
        corridor_half_width = float(corridor_half_width_str)
        nth_point = int(nth_point_str)

        source_directory = self.source_dir_entry.get().strip()
        output_file_path = self.output_file_entry.get().strip()
        source_option = self.source_option_var.get()
        export_txt = self.export_txt_var.get()
        network_directory = self.network_dir_entry.get().strip()

        self.start_button.config(state=tk.DISABLED)
        self.cancel_button.config(state=tk.NORMAL)
        self.progress_text.config(state=tk.NORMAL)
        self.progress_text.delete(1.0, tk.END)
        self.progress_text.config(state=tk.DISABLED)

        # Launch background thread
        self.processing_thread = threading.Thread(
            target=self.run_processing,
            args=(
                x_start,
                y_start,
                x_end,
                y_end,
                corridor_half_width,
                source_directory,
                output_file_path,
                nth_point,
                source_option,
                export_txt,
                corridor_epsg_code,
                default_las_epsg_code,
                network_directory,
            ),
        )
        self.processing_thread.start()
        self.after(100, self.process_queue)
        self.cancel_event.clear()

    def run_processing(
        self,
        x_start: float,
        y_start: float,
        x_end: float,
        y_end: float,
        corridor_half_width: float,
        source_directory: str,
        output_file_path: str,
        nth_point: int,
        source_option: int,
        export_txt: bool,
        corridor_epsg_code: int,
        default_las_epsg_code: Optional[int],
        network_directory: str,
    ) -> None:
        """
        The target function for the processing thread.
        Calls process_corridor and optionally converts output to TXT if requested.
        """
        try:
            start_time = time.time()
            success = process_corridor(
                x_start,
                y_start,
                x_end,
                y_end,
                corridor_half_width,
                source_directory,
                output_file_path,
                nth_point,
                source_option,
                network_directory,
                cancel_event=self.cancel_event,
                queue_obj=self.queue,
                corridor_epsg_code=corridor_epsg_code,
                default_las_epsg_code=default_las_epsg_code,
            )

            if self.cancel_event.is_set():
                logging.info("Processing canceled.")
            else:
                end_time = time.time()
                elapsed = end_time - start_time
                hours, rem = divmod(elapsed, 3600)
                minutes, seconds = divmod(rem, 60)
                logging.info(
                    f"Total processing time: {int(hours)}h {int(minutes)}m {seconds:.2f}s"
                )

                if success and export_txt:
                    logging.info("Converting output LAS to TXT.")
                    convert_start = time.time()
                    convert_las_to_txt(
                        Path(output_file_path), cancel_event=self.cancel_event
                    )
                    convert_end = time.time()
                    convert_time = convert_end - convert_start
                    logging.info(f"Conversion time: {convert_time:.2f}s")

            # Notify GUI thread that processing is complete
            self.queue.put("PROCESSING_COMPLETE")

        except Exception as exc:
            logging.error(f"Error during processing: {exc}")
            self.queue.put("PROCESSING_COMPLETE")

    def process_queue(self) -> None:
        """
        Poll the GUI queue for messages and update widgets accordingly.
        """
        try:
            while True:
                msg = self.queue.get_nowait()
                if msg == "PROCESSING_COMPLETE":
                    self.start_button.config(state=tk.NORMAL)
                    self.cancel_button.config(state=tk.DISABLED)
                    self.save_log_button.config(state=tk.NORMAL)
                    self.processing_progress["value"] = 0
                elif isinstance(msg, tuple) and msg[0] == "UPDATE_PROGRESS":
                    progress_value = int(msg[1])
                    self.processing_progress["value"] = progress_value
                else:
                    self.progress_text.config(state=tk.NORMAL)
                    self.progress_text.insert(tk.END, msg + "\n")
                    self.progress_text.see(tk.END)
                    self.progress_text.config(state=tk.DISABLED)
                    self.messages.append(msg)
        except queue.Empty:
            pass
        finally:
            # Keep polling
            self.after(100, self.process_queue)

    def setup_logging(self) -> None:
        """
        Set up a queue handler for logging so logs can be displayed in the GUI.
        """
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        queue_handler = QueueHandler(self.queue)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        queue_handler.setFormatter(formatter)
        logging.getLogger().addHandler(queue_handler)
        logging.getLogger().setLevel(logging.INFO)

    def save_log(self) -> None:
        """
        Save the accumulated log messages to a .log file located next to the output LAS file.
        """
        if not self.messages:
            messagebox.showinfo("Information", "No log messages to save.")
            return

        output_file_path = self.output_file_entry.get().strip()
        if not output_file_path:
            messagebox.showerror("Error", "Output file path is not specified.")
            return

        log_file = str(Path(output_file_path).with_suffix(".log"))
        try:
            with open(log_file, "w", encoding="utf-8") as f:
                f.write("\n".join(self.messages))
            messagebox.showinfo("Success", f"Log saved to {log_file}")
        except Exception as exc:
            logging.error(f"Failed to save log: {exc}. Trying alternative method.")
            try:
                with open(log_file, "w", encoding="utf-8", errors="replace") as f:
                    sanitized = [
                        msg.encode("utf-8", errors="replace").decode("utf-8")
                        for msg in self.messages
                    ]
                    f.write("\n".join(sanitized))
                messagebox.showinfo(
                    "Success", f"Log saved to {log_file} (with substitution)"
                )
            except Exception as exc2:
                messagebox.showerror("Error", f"Failed to save log: {exc2}")

    def cancel_processing(self) -> None:
        """
        Signal the cancel event to stop the ongoing processing.
        """
        if self.processing_thread and self.processing_thread.is_alive():
            self.cancel_event.set()
            self.cancel_button.config(state=tk.DISABLED)
            logging.info("Cancel requested.")

    def check_processing_thread(self) -> None:
        """
        Check if the processing thread is still alive and close the application if requested.
        """
        if self.processing_thread and self.processing_thread.is_alive():
            self.after(100, self.check_processing_thread)
        else:
            self.destroy()

    def on_closing(self) -> None:
        """
        Handle window close event. If a process is running, confirm cancellation before exit.
        """
        if self.processing_thread and self.processing_thread.is_alive():
            if messagebox.askokcancel(
                "Quit", "Processing in progress. Cancel and exit?"
            ):
                self.cancel_event.set()
                self.disable_widgets()
                self.close_requested = True
                self.after(100, self.check_processing_thread)
        else:
            self.destroy()

    def disable_widgets(self) -> None:
        """
        Disable all widgets to prevent interaction while closing.
        """
        for widget in self.winfo_children():
            self.disable_widget_recursive(widget)

    def disable_widget_recursive(self, widget: tk.Widget) -> None:
        """
        Recursively disable a widget and its children.
        """
        try:
            widget.config(state=tk.DISABLED)
        except Exception:
            pass
        for child in widget.winfo_children():
            self.disable_widget_recursive(child)


if __name__ == "__main__":
    app = Application()
    app.mainloop()
