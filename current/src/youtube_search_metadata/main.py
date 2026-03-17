import logging
import os
import random
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

from dateutil.relativedelta import relativedelta

from .archive import ArchiveManager
from .config import load_config
from .downloader import MetadataDownloader
from .git_ops import run_git_sync
from .notifier import send_notification
from .utils import get_dir_size_human, sanitize_filename


def generate_windows(config):
    """Generates a list of (start, end) date strings for yt-dlp."""
    ts = config.time_slicing
    if not ts.get("enabled"):
        return [(None, None)]

    start = datetime.strptime(ts["start_date"], "%Y-%m-%d")
    end = datetime.strptime(ts["end_date"], "%Y-%m-%d")
    interval = ts.get("interval", "month")

    windows = []
    curr = start
    while curr < end:
        if interval == "month":
            nxt = curr + relativedelta(months=1)
        elif interval == "quarter":
            nxt = curr + relativedelta(months=3)
        else:  # year
            nxt = curr + relativedelta(years=1)

        windows.append((curr.strftime("%Y%m%d"), nxt.strftime("%Y%m%d")))
        curr = nxt
    return windows


def run_job(config_path: str, dry_run: bool, verbose: bool):
    """
    Main execution pipeline. Iterates through multiple search queries,
    organising results into sub-directories while preserving logging and dry-run safety.
    """
    config = load_config(config_path)

    # 1. Setup Robust Logging (Preserved from previous iterations)
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console Handler for 'daghe run'
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Rotating File Handler for production stability
    log_dir = os.path.join(os.path.expandvars("${BASE_DIR}"), "logs")
    log_file = os.path.join(log_dir, f"{config.get('job_name')}.log")

    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception as e:
        print(f"Warning: Could not initialise persistent log file: {e}")

    archive = ArchiveManager(config.archive_file)
    downloader = MetadataDownloader(config, archive)

    total_new = 0
    all_errors = []
    queries_summary = []

    logger.info(f"Initialising multi-query pipeline for: {config.get('job_name')}")
    if dry_run:
        logger.warning(
            "DRY-RUN MODE ENABLED: No files will be written, no Git commits performed."
        )

    # 2. Main Pipeline Loop
    # Requires: uv add python-dateutil

    for search_item in config.searches:
        query = search_item.get("query")
        target_new = search_item.get("max_results", 10)
        windows = generate_windows(config)

        pool_size = config.strategy["pool_size"]
        all_candidates = {}  # Deduplication map: {id: entry_data}

        logger.info(f"--- Query: '{query}' | Target: {target_new} new videos ---")
        logger.info(f"Temporal slicing: {len(windows)} windows generated.")

        # FALLBACK LOOP
        while pool_size <= config.strategy["pool_size_max"]:
            for start, end in windows:
                entries = downloader.search_videos(
                    query, pool_size, search_item.get("extra_ydl_opts"), start, end
                )
                for entry in entries:
                    if entry.get("id"):
                        all_candidates[entry["id"]] = entry

            # Deduplicate and filter archived items
            unique_ids = list(all_candidates.keys())
            fresh_candidates = [
                all_candidates[v_id]
                for v_id in unique_ids
                if not archive.is_processed(v_id)
            ]

            logger.info(
                f"Pool Size {pool_size}: {len(unique_ids)} unique found, {len(fresh_candidates)} are fresh."
            )

            if (
                len(fresh_candidates) >= target_new
                or pool_size == config.strategy["pool_size_max"]
            ):
                break

            pool_size += config.strategy["pool_size_step"]
            logger.warning(
                f"Insufficient fresh candidates. Expanding pool size to {pool_size}..."
            )

        # SHUFFLE AND PROCESS
        random.shuffle(fresh_candidates)
        saved_count = 0
        target_dir = config.data_dir / sanitize_filename(query)

        for entry in fresh_candidates:
            if saved_count >= target_new:
                break

            if downloader.process_video(entry, target_dir, dry_run=dry_run):
                saved_count += 1

        logger.info(f"Completed query '{query}': Saved {saved_count} new files.")

        # 3. Synchronisation & Reporting
        git_success, git_msg = (
            (True, "Skipped") if dry_run else run_git_sync(config, total_new)
        )

        # Status determination based on errors and actual work done
        if not all_errors and git_success:
            status = "success"
        elif total_new > 0:
            status = "partial"
        else:
            status = "failure"

        total_size = get_dir_size_human(config.data_dir)
        summary = (
            f"Job: {config.get('job_name')}\n"
            f"Results: {', '.join(queries_summary)}\n"
            f"Total New Files: {total_new}\n"
            f"Git Status: {git_msg}\n"
            f"Data Size: {total_size}\n"
            f"Status: {status.upper()}"
        )

        if not dry_run:
            send_notification(
                config,
                config.get("telegram", f"level_on_{status}", default="info"),
                summary,
            )

        print(summary)
        return 0 if status == "success" else (2 if status == "partial" else 1)
