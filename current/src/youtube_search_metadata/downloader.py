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
        """Initialise with persistent settings and yt-dlp session options."""
        self.config = config
        self.archive = archive
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "ignore_no_formats_error": True,
            "extract_flat": "in_playlist",
        }

        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.ydl_opts["cookiefile"] = os.path.abspath(cookie_path)

        extra_opts = self.config.ydl_extra_options
        if extra_opts:
            self.ydl_opts.update(extra_opts)

    def search_videos(self, query: str, max_results: int) -> list:
        """Execute a specific YouTube search query."""
        search_str = f"ytsearch{max_results}:{query}"
        logger.info(f"Searching: '{query}' (Target: {max_results})")

        try:
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                result = ydl.extract_info(search_str, download=False)
                return result.get("entries", [])
        except Exception as e:
            logger.error(f"Search failed for '{query}': {e}")
            return []

    def process_video(
        self, entry: dict, target_dir: Path, dry_run: bool = False
    ) -> bool:
        """Extract metadata and save to the query-specific sub-directory."""
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
            logger.info(f"[Dry-run] Would save: {out_path}")
            return True

        try:
            # Ensure query sub-directory exists
            target_dir.mkdir(parents=True, exist_ok=True)

            video_opts = self.ydl_opts.copy()
            video_opts["extract_flat"] = False

            with yt_dlp.YoutubeDL(video_opts) as ydl:
                full_info = ydl.extract_info(video_id, download=False)

            if not full_info:
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
