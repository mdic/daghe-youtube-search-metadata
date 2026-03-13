import logging
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


@dataclass
class JobConfig:
    raw: dict

    def _expand_path(self, path_str: str) -> Path:
        """Expand environment variables like ${BASE_DIR} and return Path object."""
        if not path_str:
            return None
        expanded = os.path.expandvars(str(path_str))
        return Path(expanded)

    @property
    def searches(self) -> list:
        """Returns the list of search objects from configuration."""
        return self.raw.get("searches", [])

    @property
    def data_dir(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("data_dir"))

    @property
    def archive_file(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("archive_file"))

    @property
    def telegram_helper(self) -> str:
        path = self.raw.get("paths", {}).get("telegram_helper")
        return os.path.expandvars(path) if path else ""

    @property
    def ydl_cookie_file(self) -> str | None:
        """Returns expanded cookie file path from the yt_dlp section."""
        yt_section = self.raw.get("yt_dlp", {})
        path = yt_section.get("cookie_file")
        return os.path.expandvars(path) if path else None

    @property
    def global_ydl_opts(self) -> dict:
        """Returns global extra options for yt-dlp API."""
        return self.raw.get("yt_dlp", {}).get("extra_ydl_opts", {})

    def get(self, *keys, default=None):
        """Deep get utility for nested dictionaries."""
        data = self.raw
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return default
        return data if data is not None else default


def load_config(path: str) -> JobConfig:
    """Load and validate the YAML configuration."""
    with open(path, "r") as f:
        return JobConfig(yaml.safe_load(f))
