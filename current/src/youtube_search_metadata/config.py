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
    def data_dir(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("data_dir"))

    @property
    def archive_file(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("archive_file"))

    @property
    def telegram_helper(self) -> str:
        path = self.raw.get("paths", {}).get("telegram_helper")
        return os.path.expandvars(path) if path else ""

    # --- NUOVE PROPRIETÀ PER YT-DLP ---

    @property
    def ydl_cookie_file(self) -> str | None:
        """Returns the absolute path to the cookie file after expanding variables."""
        yt_section = self.raw.get("yt_dlp", {})
        path = yt_section.get("cookie_file")

        if path:
            expanded = os.path.expandvars(path)
            # Log di debug interno per capire se BASE_DIR è stata espansa
            logger.debug(f"Config: Expanded cookie path to {expanded}")
            return expanded
        return None

    @property
    def ydl_extra_options(self) -> dict:
        """Returns the extra options dictionary for yt-dlp API."""
        return self.raw.get("yt_dlp", {}).get("extra_options", {})

    # --- FINE SEZIONE ---

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
    """Load YAML config and initialise JobConfig object."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, "r") as f:
        config_dict = yaml.safe_load(f)

    if not config_dict:
        raise ValueError(f"Configuration file is empty or invalid: {path}")

    return JobConfig(config_dict)
