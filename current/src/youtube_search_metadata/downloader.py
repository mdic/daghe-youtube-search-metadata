import json
import logging
import os
from pathlib import Path

import yt_dlp

from .utils import sanitize_filename

logger = logging.getLogger(__name__)


class MetadataDownloader:
    def __init__(self, config, archive):
        """
        Initialise the downloader with core DaGhE requirements.
        UK English spelling and metadata-only extraction focus.
        """
        self.config = config
        self.archive = archive

        # Default safety settings for all operations
        self.base_ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
        }

        # Apply global overrides from job.yaml
        self.base_ydl_opts.update(self.config.global_ydl_opts)

        # Resolve cookies for authenticated metadata access
        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.base_ydl_opts["cookiefile"] = os.path.abspath(cookie_path)

    def _get_merged_opts(self, search_specific_opts: dict = None) -> dict:
        """Helper to merge base options with query-specific overrides."""
        opts = self.base_ydl_opts.copy()
        if search_specific_opts:
            opts.update(search_specific_opts)
        return opts

    def search_videos(
        self, query, max_results, search_opts=None, date_after=None, date_before=None
    ):
        """Phase 1: Efficient temporal search retrieval."""
        search_str = f"ytsearch{max_results}:{query}"
        opts = self._get_merged_opts(search_opts)

        # Force flat extraction for search to avoid overhead
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
            logger.error(f"Search retrieval failed for '{query}': {e}")
            return []

    def process_video(self, entry, target_dir, dry_run=False, search_opts=None):
        """
        Phase 2: Metadata-only full fetch.
        Ensures metadata is saved even if media formats are unavailable.
        """
        video_id = entry.get("id")
        if not video_id or self.archive.is_processed(video_id):
            return False

        # Construct normalised URL for deep extraction
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        # File naming logic
        use_id = self.config.get("output", "use_id_filenames", default=True)
        title = entry.get("title", "unknown")
        filename = (
            f"{video_id}.json"
            if use_id
            else f"{sanitize_filename(title)}_{video_id}.json"
        )
        out_path = target_dir / filename

        if dry_run:
            logger.info(f"[Dry-run] Metadata-only fetch for: {video_id}")
            return True

        try:
            target_dir.mkdir(parents=True, exist_ok=True)

            # Build specialized options for Phase 2: Metadata-only
            # We explicitly ignore format resolution errors here
            video_opts = self._get_merged_opts(search_opts)
            video_opts.update(
                {
                    "extract_flat": False,  # We want full metadata (tags, etc.)
                    "skip_download": True,  # Media download disabled
                    "ignore_no_formats_error": True,  # Succeed even if formats are unplayable
                    "noplaylist": True,
                }
            )

            # CRITICAL: Remove format selection constraints for metadata fetch
            # This prevents "Requested format is not available" errors
            video_opts.pop("format", None)
            video_opts.pop("format_sort", None)

            logger.info(f"Metadata-only full fetch: {video_id} (No media resolution)")

            with yt_dlp.YoutubeDL(video_opts) as ydl:
                # download=False ensures we only extract the info_dict
                full_info = ydl.extract_info(video_url, download=False)

            if not full_info:
                logger.warning(
                    f"Metadata extraction returned empty result for {video_id}"
                )
                return False

            # Check if any formats were found (logged as info only, not failure)
            if not full_info.get("formats"):
                logger.info(
                    f"Note: Video {video_id} has no downloadable formats, but metadata was successfully retrieved."
                )

            # Save JSON
            indent = 4 if self.config.get("output", "pretty_json") else None
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(full_info, f, indent=indent, ensure_ascii=False)

            self.archive.add(video_id)
            logger.info(f"Successfully saved metadata: {filename}")
            return True

        except Exception as e:
            logger.error(f"Failed to extract metadata for {video_id}: {e}")
            return False
