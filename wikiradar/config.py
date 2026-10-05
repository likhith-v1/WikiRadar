import os

from dotenv import load_dotenv

load_dotenv()

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = os.getenv("WIKI_USER_AGENT", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki-edits")
