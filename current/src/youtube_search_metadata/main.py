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
    for search_item in config.searches:
        query = search_item.get("query")
        max_res = search_item.get("max_results", 5)
        search_opts = search_item.get("extra_ydl_opts", {})

        if not query:
            continue

        logger.info(f"--- Processing Query: '{query}' ---")

        query_dir_name = sanitize_filename(query)
        target_dir = config.data_dir / query_dir_name

        # Discovery candidates
        candidates = downloader.search_videos(query, max_res, search_opts=search_opts)

        query_saved_count = 0
        query_skipped_count = 0
        query_failed_count = 0

        # Iterating through ALL search results
        for entry in candidates:
            video_id = entry.get("id", "unknown")

            # Check archive before calling extractor to save time
            if archive.is_processed(video_id):
                query_skipped_count += 1
                continue

            try:
                # Execution of Phase 2 for each candidate
                if downloader.process_video(
                    entry, target_dir, dry_run=dry_run, search_opts=search_opts
                ):
                    query_saved_count += 1
                else:
                    # process_video returns False on non-fatal failures
                    query_failed_count += 1
            except Exception as e:
                # Catching any unexpected errors to prevent loop breakage
                err_msg = (
                    f"Query '{query}', Video {video_id}: Unexpected error: {str(e)}"
                )
                logger.error(err_msg)
                all_errors.append(err_msg)
                query_failed_count += 1

        total_new += query_saved_count
        summary_msg = (
            f"'{query}': {query_saved_count} saved, {query_skipped_count} skipped"
        )
        if query_failed_count > 0:
            summary_msg += f", {query_failed_count} failed"

        queries_summary.append(summary_msg)
        logger.info(
            f"Query Summary for '{query}': {query_saved_count} new, {query_skipped_count} skipped."
        )

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
