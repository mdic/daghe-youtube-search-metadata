import json
import logging
import os
from pathlib import Path

import yt_dlp

from .archive import ArchiveManager
from .utils import sanitize_filename

logger = logging.getLogger(__name__)


class MetadataDownloader:
    def __init__(self, config, archive: ArchiveManager):
        """
        Initialise the downloader with DaGhE configuration and yt-dlp options.
        UK English spelling and robust session management.
        """
        self.config = config
        self.archive = archive

        # 1. Define base technical options
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": "in_playlist",  # Initial search is flat to save resources
        }

        # 2. Apply cookies if provided in the configuration
        # We use the specific key 'cookiefile' required by the yt-dlp API
        cookie_path = self.config.ydl_cookie_file
        if cookie_path:
            if os.path.exists(cookie_path):
                self.ydl_opts["cookiefile"] = cookie_path
                logger.info(f"Using specialised cookies from: {cookie_path}")
            else:
                logger.warning(f"Cookie file configured but not found: {cookie_path}")

        # 3. Merge extra options from YAML (e.g., sleep_interval_requests, verbose)
        extra_opts = self.config.ydl_extra_options
        if extra_opts:
            logger.info(
                f"Applying custom yt-dlp session options: {list(extra_opts.keys())}"
            )
            self.ydl_opts.update(extra_opts)

    def search_videos(self) -> list:
        """
        Perform a YouTube search based on the query and limit defined in config.
        """
        query = self.config.get("search", "query")
        max_res = self.config.get("search", "max_results")
        search_str = f"ytsearch{max_res}:{query}"

        logger.info(f"Searching YouTube for: '{query}' (Targeting {max_res} results)")

        try:
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                result = ydl.extract_info(search_str, download=False)
                entries = result.get("entries", [])
                logger.info(f"Found {len(entries)} candidate videos.")
                return entries
        except Exception as e:
            logger.error(f"YouTube search failed: {e}")
            raise e

    def process_video(self, entry: dict, dry_run: bool = False) -> bool:
        """
        Extract full metadata for a single video and save it to the data directory.
        """
        video_id = entry.get("id")
        title = entry.get("title", "unknown_video")

        if not video_id:
            logger.warning("Encountered an entry without a valid video ID. Skipping.")
            return False

        if self.archive.is_processed(video_id):
            logger.debug(f"Skipping {video_id} - already exists in persistent archive.")
            return False

        # --- Filename Generation Logic ---
        use_id = self.config.get("output", "use_id_filenames", default=True)

        if use_id:
            filename_base = video_id
        else:
            # We append the ID to the title to ensure uniqueness in the filesystem
            safe_title = sanitize_filename(title)
            filename_base = f"{safe_title}_{video_id}"

        out_path = self.config.data_dir / f"{filename_base}.json"

        if dry_run:
            logger.info(f"[Dry-run] Would download and save: {out_path.name}")
            return True

        try:
            # Re-fetch full info for this specific video using the session options
            # This ensures cookies and rate-limiting (sleep) are applied here too
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                full_info = ydl.extract_info(video_id, download=False)

            # Prevent overwriting unless explicitly authorised in config
            if out_path.exists() and not self.config.get(
                "output", "overwrite_existing_json"
            ):
                logger.warning(f"File {out_path.name} already exists. Skipping write.")
                return False

            # Format JSON output
            indent = 4 if self.config.get("output", "pretty_json") else None

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(full_info, f, indent=indent, ensure_ascii=False)

            # Update archive only after a successful write
            self.archive.add(video_id)
            logger.info(f"Successfully saved metadata: {out_path.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to process video {video_id}: {e}")
            # Decision to continue or abort based on configuration
            if not self.config.get("runtime", "continue_on_video_error", default=True):
                raise e
            return False
