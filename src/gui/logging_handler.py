"""
logging_handler.py
------------------
Custom logging handler that routes log records into a Tkinter-safe queue
so they can be displayed or handled by the GUI.
"""

import logging


class QueueHandler(logging.Handler):
    """
    A custom logging handler that places log records into a GUI queue.
    """

    def __init__(self, queue_obj):
        super().__init__()
        self.queue = queue_obj

    def emit(self, record: logging.LogRecord) -> None:
        log_entry = self.format(record)
        self.queue.put(log_entry)
