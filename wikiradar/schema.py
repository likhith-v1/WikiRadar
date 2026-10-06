import ipaddress
import json
import re
from dataclasses import asdict, dataclass, fields
from datetime import datetime

REVERT_RE = re.compile(
    r"\b(undid revision|reverted \d+ edits?|reverted edits by|rv[ v]|rollback|revert(ed|ing)?)\b",
    re.IGNORECASE,
)


@dataclass
class Edit:
    event_id: str
    dt: datetime
    title: str
    title_url: str
    user: str
    is_bot: bool
    is_anon: bool
    is_new: bool
    byte_delta: int
    is_revert: bool
    rev_id: int | None
    synthetic: bool = False


def _is_anon(user: str) -> bool:
    # "~" prefix marks MediaWiki temporary accounts, which replaced IP editing.
    if user.startswith("~"):
        return True
    try:
        ipaddress.ip_address(user)
    except ValueError:
        return False
    return True


def normalize(raw: dict) -> Edit | None:
    """None when a required field is missing or malformed."""
    try:
        length = raw["length"]
        return Edit(
            event_id=raw["meta"]["id"],
            dt=datetime.fromisoformat(raw["meta"]["dt"]),
            title=raw["title"],
            title_url=raw["title_url"],
            user=raw["user"],
            is_bot=raw["bot"],
            is_anon=_is_anon(raw["user"]),
            is_new=raw["type"] == "new",
            byte_delta=length["new"] - (length.get("old") or 0),
            is_revert=bool(REVERT_RE.search(raw.get("comment") or "")),
            rev_id=raw["revision"]["new"],
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def to_json(edit: Edit) -> str:
    return json.dumps({**asdict(edit), "dt": edit.dt.isoformat()})


def from_json(text: str | bytes) -> Edit:
    record = json.loads(text)
    record["dt"] = datetime.fromisoformat(record["dt"])
    return Edit(**{f.name: record[f.name] for f in fields(Edit) if f.name in record})
