import logging
import os
from logging.handlers import RotatingFileHandler

from .archive import ArchiveManager
from .config import load_config
from .downloader import MetadataDownloader
from .git_ops import run_git_sync
from .notifier import send_notification
from .utils import get_dir_size_human


def run_job(config_path: str, dry_run: bool, verbose: bool):
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    config = load_config(config_path)
    archive = ArchiveManager(config.archive_file)
    downloader = MetadataDownloader(config, archive)

    # Setup Logging
    logger = logging.getLogger()
    logger.setLevel(log_level)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Handler per Console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler per File (nella cartella logs/ dell'orchestrazione)
    # BASE_DIR viene dal tuo global.env tramite os.path.expandvars nel config
    log_file = os.path.join(
        os.path.expandvars("${BASE_DIR}"), "logs", "youtube-search-metadata.log"
    )

    try:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Impossibile creare il file di log: {e}")

    newly_downloaded = 0
    errors = []

    try:
        candidates = downloader.search_videos()
        for entry in candidates:
            try:
                if downloader.process_video(entry, dry_run=dry_run):
                    newly_downloaded += 1
            except Exception as e:
                logging.error(
                    f"Errore durante l'elaborazione del video {entry.get('id')}: {e}"
                )
                errors.append(str(e))

        git_success, git_msg = (
            (True, "Skipped") if dry_run else run_git_sync(config, newly_downloaded)
        )

        if not errors and git_success:
            status = "success"
        elif newly_downloaded > 0:
            status = "partial"
        else:
            status = "failure"

        # Final Summary
        total_size = get_dir_size_human(config.data_dir)
        summary = (
            f"Job: {config.get('job_name')}\n"
            f"Query: {config.get('search', 'query')}\n"
            f"New Files: {newly_downloaded}\n"
            f"Git Status: {git_msg}\n"
            f"Data Size: {total_size}\n"
            f"Status: {status.upper()}"
        )

        if errors:
            summary += f"\nErrors: {len(errors)} occurred."

        if not dry_run:
            notify_level = config.get("telegram", f"level_on_{status}", default="info")
            send_notification(config, notify_level, summary)

        print(summary)
        return 0 if status == "success" else (2 if status == "partial" else 1)

    except Exception as e:
        logging.error(f"Fatal job failure: {e}")
        if not dry_run:
            send_notification(config, "error", f"Job Failed: {e}")
        return 1
