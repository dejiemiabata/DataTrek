import os

from dotenv import load_dotenv

load_dotenv()

news_api_config = {
    "newscatcher": {
        "api_key": os.getenv("NEWSCATCHER_API_KEY", ""),
        "api_base_url": os.getenv("NEWSCATCHER_API_BASE_URL", ""),
    }
}
