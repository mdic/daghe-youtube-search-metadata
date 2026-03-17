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

        # Ensure search phase is always flat
        current_opts["extract_flat"] = "in_playlist"

        logger.info(f"Searching: '{query}' (Targeting {max_results} results)")
        try:
            with yt_dlp.YoutubeDL(current_opts) as ydl:
                result = ydl.extract_info(search_str, download=False)
                entries = result.get("entries", [])
                logger.info(f"Search Phase: Found {len(entries)} candidate(s) for '{query}'")
                return entries
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

        if not video_id:
            logger.warning("Entry missing ID, skipping.")
            return False

        if self.archive.is_processed(video_id):
            return False

        # Normalise the target: Use ID to build a clean watch URL
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
            # Ensure target directory exists
            target_dir.mkdir(parents=True, exist_ok=True)

            # LAYER 3: Merge overrides and FORCE full extraction (non-flat)
            video_opts = self._get_merged_opts(search_opts)
            video_opts.update({
                "extract_flat": False,
                "noplaylist": True
            })
