import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ArchiveManager:
    def __init__(self, archive_path: Path):
        self.path = archive_path
        self.processed_ids = self._load()

    def _load(self) -> set:
        if not self.path.exists():
            return set()

        ids = set()
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    ids.add(line)
        return ids

    def is_processed(self, video_id: str) -> bool:
        return video_id in self.processed_ids

    def add(self, video_id: str):
        if video_id not in self.processed_ids:
            with open(self.path, "a") as f:
                f.write(f"{video_id}\n")
            self.processed_ids.add(video_id)
            logger.debug(f"Video {video_id} added to archive.")
