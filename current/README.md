# DaGhE YouTube Search Metadata Module

This module is a professionalised metadata sampling engine designed to build a diverse corpus of YouTube video information. It avoids algorithmic bias by using **Stochastic Temporal Sampling** and ensures high reliability by performing **Metadata-only Deep Extraction**.

## 🏗 Core Strategy

The module operates in two distinct phases:

1.  **Phase 1: Stochastic Search**: 
    *   The total configured time range (e.g., 2020–2025) is divided into intervals (windows).
    *   Exactly **one random window** is selected per execution.
    *   The script fetches **N candidates** (`candidates_to_fetch`) from that specific period.
2.  **Phase 2: Metadata-only Fetch**:
    *   Candidates are deduplicated against the persistent archive and existing files.
    *   Eligible candidates are randomised.
    *   The script performs a deep extraction for up to **Y results** (`max_results_to_save`).
    *   Media format resolution is explicitly disabled to bypass YouTube SABR/blocking errors, ensuring only JSON metadata is saved.

## ⚙️ Configuration (`job.yaml`)

The module is entirely driven by `config/job.yaml`. 

### Sampling Control
*   `candidates_to_fetch` ($N$): The breadth of the search. Higher values increase discovery of "deep" content.
*   `max_results_to_save` ($Y$): The depth of the write operation. Limits disk I/O and Git noise.

### Temporal Slicing
*   `interval`: Can be `month`, `quarter`, or `year`.
*   `start_date` / `end_date`: Format `YYYY-MM-DD`.

### Pass-Through Options (`extra_ydl_opts`)
You can pass any valid [yt-dlp Python API option](https://github.com/yt-dlp/yt-dlp/blob/master/yt_dlp/YoutubeDL.py) directly.
*   **Global level**: Applied to all searches.
*   **Search level**: Applied only to a specific query.

## 🚀 Usage via DaGhE

### Installation
```bash
uv run bin/daghe install daghe-youtube-search-metadata
```

### Manual Test Run
```bash
uv run bin/daghe run daghe-youtube-search-metadata
```

### Checking Logs
Logs are rotated automatically and stored at `${BASE_DIR}/logs/daghe-youtube-search-metadata.log`.
```bash
uv run bin/daghe logs daghe-youtube-search-metadata
```

## 🛠 Prerequisites
*   **Deno**: Required as the JavaScript runtime for solving YouTube "n-parameter" challenges.
*   **python-dateutil**: Required for temporal window calculations (`uv add python-dateutil`).
