import logging
import os
from logging.handlers import RotatingFileHandler

from .archive import ArchiveManager
from .config import load_config
from .downloader import MetadataDownloader
from .git_ops import run_git_sync
from .notifier import send_notification
from .utils import get_dir_size_human, sanitize_filename


def run_job(config_path: str, dry_run: bool, verbose: bool):
    """
    Main execution pipeline. Iterates through multiple search queries,
    organising results into sub-directories.
    """
    config = load_config(config_path)

    # Logging Setup
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console & File Handlers
    log_dir = os.path.join(os.path.expandvars("${BASE_DIR}"), "logs")
    log_file = os.path.join(log_dir, f"{config.get('job_name')}.log")

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    try:
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception as e:
        print(f"Warning: Log file error: {e}")

    archive = ArchiveManager(config.archive_file)
    downloader = MetadataDownloader(config, archive)

    total_new = 0
    all_errors = []
    queries_processed = []

    logger.info(f"Initialising multi-query pipeline for: {config.get('job_name')}")

    # --- MAIN PIPELINE LOOP ---
    for search_item in config.searches:
        query = search_item.get("query")
        max_res = search_item.get("max_results", 5)
        # Extract search-specific yt-dlp options if they exist
        search_opts = search_item.get("extra_ydl_opts", {})

        if not query:
            continue

        logger.info(f"--- Processing Query: {query} ---")

        # Define and create query-specific sub-directory
        query_dir_name = sanitize_filename(query)
        target_dir = config.data_dir / query_dir_name

        candidates = downloader.search_videos(query, max_res, search_opts)
        query_new_count = 0

        for entry in candidates:
            try:
                if downloader.process_video(
                    entry, target_dir, dry_run=dry_run, search_opts=search_opts
                ):
                    query_new_count += 1
            except Exception as e:
                all_errors.append(f"Query '{query}': {str(e)}")

        total_new += query_new_count
        queries_processed.append(f"{query} ({query_new_count})")

    # --- POST-PROCESSING ---
    git_success, git_msg = (
        (True, "Skipped") if dry_run else run_git_sync(config, total_new)
    )

    status = (
        "success"
        if not all_errors and git_success
        else ("partial" if total_new > 0 else "failure")
    )

    summary = (
        f"Job: {config.get('job_name')}\n"
        f"Queries: {', '.join(queries_processed)}\n"
        f"Total New Files: {total_new}\n"
        f"Git Status: {git_msg}\n"
        f"Data Size: {get_dir_size_human(config.data_dir)}\n"
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
