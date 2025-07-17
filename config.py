# Telegram API credentials (get from my.telegram.org)
API_ID = "123456"
API_HASH = "hash here"
BOT_TOKEN = "bot token here"

# Rate limiting settings (in seconds)
ADMIN_DELAY = 1.5      # Delay between admin checks
FORWARD_DELAY = 2.0    # Delay between message forwards
BATCH_DELAY = 0.5      # Delay between files in a batch
DELETE_DELAY = 0.3     # Delay between message deletions

# Database settings
DATABASE_PATH = "autoposter.db"

# Job limits
MAX_BATCH_SIZE = 20
MAX_RECURRING_TIME = 1440  # 24 hours in minutes
MAX_DELETE_TIME = 10080    # 7 days in minutes

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
ADMIN_IDS = [6082136901]
FORCE_SUB_CHANNEL_ID = "-1002581367215"
