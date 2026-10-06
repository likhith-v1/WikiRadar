import os

from dotenv import load_dotenv

load_dotenv()

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = os.getenv("WIKI_USER_AGENT", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wiki-edits")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "wikiradar-processor")
POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
