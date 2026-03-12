import json
import logging
from pathlib import Path

import yt_dlp

from .archive import ArchiveManager

logger = logging.getLogger(__name__)


class MetadataDownloader:
    def __init__(self, config, archive: ArchiveManager):
        self.config = config
        self.archive = archive
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": "in_playlist",  # Initial search is flat to save time
        }

    def search_videos(self) -> list:
        query = self.config.get("search", "query")
        max_res = self.config.get("search", "max_results")
        search_str = f"ytsearch{max_res}:{query}"

        logger.info(f"Searching YouTube for: '{query}' (max {max_res})")
        with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
            result = ydl.extract_info(search_str, download=False)
            return result.get("entries", [])

    def process_video(self, entry: dict, dry_run: bool = False) -> bool:
        video_id = entry.get("id")
        title = entry.get("title", "unknown_video")

        if not video_id:
            return False

        if self.archive.is_processed(video_id):
            logger.debug(f"Skipping {video_id} - already in archive.")
            return False

        # --- LOGICA NOME FILE ---
        use_id = self.config.get("output", "use_id_filenames", default=True)

        if use_id:
            filename_base = video_id
        else:
            filename_base = f"{sanitize_filename(title)}_{video_id}"
            # Consiglio: aggiungere comunque l'ID alla fine del titolo per evitare
            # conflitti se due video hanno lo stesso titolo identico.

        out_path = self.config.data_dir / f"{filename_base}.json"
        # ------------------------

        if dry_run:
            logger.info(f"[Dry-run] Would save metadata to {out_path.name}")
            return True

        try:
            # Re-fetch full info
            with yt_dlp.YoutubeDL({"quiet": True, "skip_download": True}) as ydl:
                full_info = ydl.extract_info(video_id, download=False)

            if out_path.exists() and not self.config.get(
                "output", "overwrite_existing_json"
            ):
                logger.warning(f"File {out_path.name} already exists, skipping.")
                return False

            indent = 4 if self.config.get("output", "pretty_json") else None
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(full_info, f, indent=indent, ensure_ascii=False)

            self.archive.add(video_id)
            logger.info(f"Saved metadata: {out_path.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to process {video_id}: {e}")
            if not self.config.get("runtime", "continue_on_video_error"):
                raise e
            return False
