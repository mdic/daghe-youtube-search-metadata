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
        Initialise the downloader with DaGhE configuration.
        UK English spelling applied.
        Optimised for metadata-only extraction, ignoring media format errors.
        """
        self.config = config
        self.archive = archive

        # 1. Base options for yt-dlp API
        # We set ignore_no_formats_error to True to handle YouTube's format blocking
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "ignore_no_formats_error": True,  # Equivalent to --ignore-no-formats-error
            "extract_flat": "in_playlist",  # Used for efficient searching
        }

        # 2. COOKIE INTEGRATION
        cookie_path = self.config.ydl_cookie_file
        if cookie_path:
            abs_cookie_path = os.path.abspath(cookie_path)
            if os.path.exists(abs_cookie_path):
                self.ydl_opts["cookiefile"] = abs_cookie_path
                logger.info(f"Using specialised cookies from: {abs_cookie_path}")
            else:
                logger.warning(
                    f"Cookie file configured but not found: {abs_cookie_path}"
                )

        # 3. EXTRA OPTIONS (Merged from job.yaml)
        extra_opts = self.config.ydl_extra_options
        if extra_opts:
            logger.info(
                f"Applying custom yt-dlp session options: {list(extra_opts.keys())}"
            )
            self.ydl_opts.update(extra_opts)

    def search_videos(self) -> list:
        """
        Perform a YouTube search using the 'flat' extraction mode for speed.
        """
        query = self.config.get("search", "query")
        max_res = self.config.get("search", "max_results")
        search_str = f"ytsearch{max_res}:{query}"

        logger.info(f"Searching YouTube for: '{query}' (Target: {max_res} results)")

        try:
            # For searching, we keep extract_flat enabled
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
        Extract full metadata for a single video.
        Uses a non-flat extraction but ignores format errors to bypass SABR/Bot blocks.
        """
        video_id = entry.get("id")
        title = entry.get("title", "unknown_video")

        if not video_id:
            return False

        if self.archive.is_processed(video_id):
            logger.debug(f"Skipping {video_id} - already exists in persistent archive.")
            return False

        # Filename Generation
        use_id = self.config.get("output", "use_id_filenames", default=True)
        safe_title = sanitize_filename(title)
        filename_base = video_id if use_id else f"{safe_title}_{video_id}"
        out_path = self.config.data_dir / f"{filename_base}.json"

        if dry_run:
            logger.info(f"[Dry-run] Would save metadata to {out_path.name}")
            return True

        try:
            # We must create a specific options dict for full metadata extraction
            # We set extract_flat to False to get descriptions, tags, etc.
            video_opts = self.ydl_opts.copy()
            video_opts["extract_flat"] = False

            with yt_dlp.YoutubeDL(video_opts) as ydl:
                # This will now succeed even if formats are missing
                full_info = ydl.extract_info(video_id, download=False)

            if not full_info:
                logger.error(
                    f"Failed to extract any info for {video_id} despite ignoring errors."
                )
                return False

            if out_path.exists() and not self.config.get(
                "output", "overwrite_existing_json"
            ):
                logger.warning(f"File {out_path.name} already exists. Skipping write.")
                return False

            indent = 4 if self.config.get("output", "pretty_json") else None
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(full_info, f, indent=indent, ensure_ascii=False)

            self.archive.add(video_id)
            logger.info(f"Successfully saved metadata: {out_path.name}")
            return True

        except Exception as e:
            logger.error(f"Failed to process video {video_id}: {e}")
            if not self.config.get("runtime", "continue_on_video_error", default=True):
                raise e
            return False
