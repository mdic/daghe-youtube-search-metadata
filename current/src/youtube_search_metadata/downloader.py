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
        Initialise the downloader with multi-layer option merging.
        UK English spelling and robust path resolution applied.
        """
        self.config = config
        self.archive = archive

        # LAYER 1: Hardcoded Protection Defaults
        # These ensure no media is downloaded and search is efficient
        self.base_ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": "in_playlist",
        }

        # LAYER 2: Global YAML Options (extra_ydl_opts)
        global_extras = self.config.global_ydl_opts
        if global_extras:
            self.base_ydl_opts.update(global_extras)

        # Handle Cookies for authenticated sessions
        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.base_ydl_opts["cookiefile"] = os.path.abspath(cookie_path)
            logger.info(f"Authenticated session enabled via: {cookie_path}")

    def _get_merged_opts(self, search_specific_opts: dict = None) -> dict:
        """Helper to merge base options with specific query overrides."""
        opts = self.base_ydl_opts.copy()
        if search_specific_opts:
            logger.debug(
                f"Applying search overrides: {list(search_specific_opts.keys())}"
            )
            opts.update(search_specific_opts)
        return opts

    def search_videos(
        self, query, max_results, search_opts=None, date_after=None, date_before=None
    ):
        """Phase 1: Search within a specific temporal window."""
        search_str = f"ytsearch{max_results}:{query}"
        opts = self.base_ydl_opts.copy()
        if search_opts:
            opts.update(search_opts)

        opts.update(
            {
                "extract_flat": "in_playlist",
                "dateafter": date_after,
                "datebefore": date_before,
            }
        )

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                result = ydl.extract_info(search_str, download=False)
                return result.get("entries", [])
        except Exception as e:
            logger.error(f"Search failed for '{query}': {e}")
            return []

    def process_video(
        self,
        entry: dict,
        target_dir: Path,
        dry_run: bool = False,
        search_opts: dict = None,
    ) -> bool:
        """
        Extract full metadata and save to query sub-directory.
        Strictly respects the dry_run flag and UK English standards.
        """
        video_id = entry.get("id")
        title = entry.get("title", "unknown_video")

        if not video_id:
            logger.warning("Entry missing ID, skipping.")
            return False

        if self.archive.is_processed(video_id):
            return False

        # Normalise the target: Use ID to build a clean watch URL for deep extraction
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        use_id = self.config.get("output", "use_id_filenames", default=True)
        filename = (
            f"{video_id}.json"
            if use_id
            else f"{sanitize_filename(title)}_{video_id}.json"
        )
        out_path = target_dir / filename

        logger.info(f"Full Fetch: Processing {video_id} (URL: {video_url})")

        if dry_run:
            logger.info(f"[Dry-run] Would save metadata to: {out_path.name}")
            return True

        try:
            # Ensure target query directory exists
            target_dir.mkdir(parents=True, exist_ok=True)

            # LAYER 3: Merge overrides and FORCE full deep extraction (non-flat)
            video_opts = self._get_merged_opts(search_opts)
            video_opts.update({"extract_flat": False, "noplaylist": True})

            with yt_dlp.YoutubeDL(video_opts) as ydl:
                # Deep extraction to get description, tags, and full metadata
                full_info = ydl.extract_info(video_url, download=False)

            if not full_info:
                logger.warning(f"Could not retrieve full metadata for {video_id}")
                return False

            # Check if overwriting is disallowed
            if out_path.exists() and not self.config.get(
                "output", "overwrite_existing_json"
            ):
                logger.debug(f"File {out_path.name} exists, skipping write.")
                return False

            # Determine JSON formatting
            indent = 4 if self.config.get("output", "pretty_json") else None

            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(full_info, f, indent=indent, ensure_ascii=False)

            # Mark as processed in the persistent archive
            self.archive.add(video_id)
            logger.info(f"Successfully saved: {filename}")
            return True

        except Exception as e:
            logger.error(f"Failed to extract metadata for {video_id}: {e}")
            return False
