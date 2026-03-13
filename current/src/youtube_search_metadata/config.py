import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class JobConfig:
    raw: dict

    def _expand_path(self, path_str: str) -> Path:
        # Espande variabili come ${BASE_DIR} usando l'ambiente del sistema
        expanded = os.path.expandvars(path_str)
        return Path(expanded)

    @property
    def data_dir(self) -> Path:
        return self._expand_path(self.raw["paths"]["data_dir"])

    @property
    def archive_file(self) -> Path:
        return self._expand_path(self.raw["paths"]["archive_file"])

    @property
    def telegram_helper(self) -> str:
        # Per lo script helper, restituiamo la stringa espansa
        return os.path.expandvars(self.raw["paths"]["telegram_helper"])

    @property
    def ydl_cookie_file(self) -> str | None:
        path = self.get("yt_dlp", "cookie_file")
        if path:
            return str(self._expand_path(path))
        return None

    @property
    def ydl_extra_options(self) -> dict:
        return self.get("yt_dlp", "extra_options", default={})

    def get(self, *keys, default=None):
        data = self.raw
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return default
        return data if data is not None else default


def load_config(path: str) -> JobConfig:
    with open(path, "r") as f:
        # Carichiamo il dizionario YAML
        config_dict = yaml.safe_load(f)
        return JobConfig(config_dict)
