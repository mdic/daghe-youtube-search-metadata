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

        # Handle Cookies
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
        self, query: str, max_results: int, search_opts: dict = None
    ) -> list:
        """Perform search using merged global and specific options."""
        search_str = f"ytsearch{max_results}:{query}"
        current_opts = self._get_merged_opts(search_opts)

        logger.info(f"Searching: '{query}' (Limit: {max_results})")
        try:
            with yt_dlp.YoutubeDL(current_opts) as ydl:
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
        Strictly respects the dry_run flag.
        """
        video_id = entry.get("id")
        title = entry.get("title", "unknown_video")

        if not video_id or self.archive.is_processed(video_id):
            return False

        use_id = self.config.get("output", "use_id_filenames", default=True)
        filename = (
            f"{video_id}.json"
            if use_id
            else f"{sanitize_filename(title)}_{video_id}.json"
        )
        out_path = target_dir / filename

        if dry_run:
            logger.info(f"[Dry-run] Would save metadata to: {out_path}")
            return True

        try:
            # Ensure target directory exists
            target_dir.mkdir(parents=True, exist_ok=True)

            # LAYER 3: Merge specific overrides for full extraction
            video_opts = self._get_merged_opts(search_opts)
            video_opts["extract_flat"] = False

            with yt_dlp.YoutubeDL(video_opts) as ydl:
                full_info = ydl.extract_info(video_id, download=False)

            if not full_info:
                return False

            if out_path.exists() and not self.config.get(
                "output", "overwrite_existing_json"
            ):
                return False

            indent = 4 if self.config.get("output", "pretty_json") else None
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(full_info, f, indent=indent, ensure_ascii=False)

            self.archive.add(video_id)
            logger.info(f"Saved: {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed video {video_id}: {e}")
            return False
