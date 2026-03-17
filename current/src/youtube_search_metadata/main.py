import logging
import os
import random
from datetime import datetime
from logging.handlers import RotatingFileHandler

from dateutil.relativedelta import relativedelta  # Requires: uv add python-dateutil

from .archive import ArchiveManager
from .config import load_config
from .downloader import MetadataDownloader
from .git_ops import run_git_sync
from .notifier import send_notification
from .utils import get_dir_size_human, sanitize_filename


def generate_windows(config):
    """
    Divides the configured date range into standardised intervals.
    UK English spelling applied.
    """
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

        # Format for yt-dlp date filters: YYYYMMDD
        windows.append((curr.strftime("%Y%m%d"), nxt.strftime("%Y%m%d")))
        curr = nxt
    return windows


def run_job(config_path: str, dry_run: bool, verbose: bool):
    config = load_config(config_path)

    # Setup Logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    log_dir = os.path.join(os.path.expandvars("${BASE_DIR}"), "logs")
    log_file = os.path.join(log_dir, f"{config.get('job_name')}.log")

    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(logging.StreamHandler())

    archive = ArchiveManager(config.archive_file)
    downloader = MetadataDownloader(config, archive)

    total_new_saved = 0
    all_errors = []
    summary_lines = []

    all_possible_windows = generate_windows(config)
    logger.info(
        f"Temporal Stratification: {len(all_possible_windows)} windows available."
    )

    for search_item in config.searches:
        query = search_item.get("query")
        if not query:
            continue

        selected_window = random.choice(all_possible_windows)
        date_after, date_before = selected_window

        n_fetch = config.sampling["candidates_to_fetch"]
        y_save = config.sampling["max_results_to_save"]

        logger.info(f"--- Processing Query: '{query}' ---")
        logger.info(f"Sampling Window: {date_after or 'Any'} to {date_before or 'Any'}")

        candidates = downloader.search_videos(
            query,
            n_fetch,
            search_opts=search_item.get("extra_ydl_opts"),
            date_after=date_after,
            date_before=date_before,
        )

        unique_candidates = {c["id"]: c for c in candidates if c.get("id")}.values()
        fresh_candidates = [
            c for c in unique_candidates if not archive.is_processed(c["id"])
        ]

        logger.info(
            f"Pool: {len(candidates)} fetched -> {len(fresh_candidates)} fresh candidates found."
        )

        random.shuffle(fresh_candidates)

        query_saved_count = 0
        target_dir = config.data_dir / sanitize_filename(query)

        for entry in fresh_candidates:
            if query_saved_count >= y_save:
                break

            # Phase 2: Now strictly metadata-only
            if downloader.process_video(
                entry,
                target_dir,
                dry_run=dry_run,
                search_opts=search_item.get("extra_ydl_opts"),
            ):
                query_saved_count += 1

        total_new_saved += query_saved_count
        summary_lines.append(f"'{query}' [{date_after}]: {query_saved_count} saved")
        logger.info(f"Finished query '{query}': {query_saved_count} files created.")

    # Finalise: Git Sync & Reporting
    git_success, git_msg = (
        (True, "Skipped") if dry_run else run_git_sync(config, total_new_saved)
    )
    status = "success" if git_success else "partial"

    summary_text = (
        f"Job: {config.get('job_name')}\n"
        f"Windows: {len(all_possible_windows)} total\n"
        f"Details: {', '.join(summary_lines)}\n"
        f"Total New: {total_new_saved}\n"
        f"Data Size: {get_dir_size_human(config.data_dir)}\n"
        f"Status: {status.upper()}"
    )

    if not dry_run:
        send_notification(
            config,
            config.get("telegram", f"level_on_{status}", default="info"),
            summary_text,
        )

    print(summary_text)
    return 0 if status == "success" else 2
