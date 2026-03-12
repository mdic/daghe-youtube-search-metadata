import logging
import subprocess

logger = logging.getLogger(__name__)


def send_notification(config, level: str, message: str):
    if not config.get("telegram", "enabled"):
        return

    helper = config.telegram_helper
    try:
        subprocess.run([helper, level.lower(), message], check=True)
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {e}")
