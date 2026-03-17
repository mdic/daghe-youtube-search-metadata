import logging
import os
import re
import subprocess
from logging.handlers import RotatingFileHandler
from pathlib import Path


def sanitize_filename(filename: str) -> str:
    if not filename:
        return "unknown_title"
    # Forza a stringa, rimuove caratteri illegali
    filename = str(filename)
    filename = re.sub(r"(?u)[^-\w. ]", "", filename)
    filename = " ".join(filename.split())
    return filename[:200] if filename else "unknown_title"


def get_dir_size_human(directory: Path) -> str:
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if total_size < 1024.0:
            return f"{total_size:.2f} {unit}"
        total_size /= 1024.0
    return f"{total_size:.2f} PB"
