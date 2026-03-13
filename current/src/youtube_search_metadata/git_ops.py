import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def run_git_sync(config, new_count: int):
    if not config.get("git", "enabled"):
        return True, "Git sync disabled"

    if new_count == 0:
        return True, "No changes to commit"

    data_dir = config.data_dir
    query = config.get("search", "query")
    msg = config.get("git", "commit_message_template").format(
        new_count=new_count, query=query
    )

    try:
        # Check for changes
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=data_dir,
            capture_output=True,
            text=True,
        )
        if not status.stdout.strip():
            return True, "No actual changes in data dir"

        # Commit
        subprocess.run(["git", "add", "-A"], cwd=data_dir, check=True)
        subprocess.run(["git", "commit", "-m", msg], cwd=data_dir, check=True)

        # Push
        if config.get("git", "auto_push"):
            branch = config.get("git", "branch", default="main")
            subprocess.run(["git", "push", "origin", branch], cwd=data_dir, check=True)
            return True, "Committed and Pushed"

        return True, "Committed (push disabled)"
    except subprocess.CalledProcessError as e:
        logger.error(f"Git operation failed: {e}")
        return False, str(e)
