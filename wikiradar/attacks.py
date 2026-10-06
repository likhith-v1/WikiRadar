"""Synthetic attack generators, shared by inject.py (live) and evaluate.py (offline)."""

import random
import uuid
from datetime import datetime, timedelta

from wikiradar.features import WINDOW, window_start
from wikiradar.schema import Edit


def _temp_account(rng: random.Random) -> str:
    return f"~2026-{rng.randint(10000, 99999)}-{rng.randint(10, 99)}"


def _title(rng: random.Random) -> str:
    return f"Synthetic page {rng.randint(1, 10**6)}"


def vandal_wave(rng: random.Random, i: int) -> list[dict]:
    """Many anonymous users blanking chunks of many pages."""
    return [dict(title=_title(rng), user=_temp_account(rng), is_anon=True, byte_delta=-rng.randint(1000, 8000))
            for _ in range(6 * i)]


def edit_war(rng: random.Random, i: int) -> list[dict]:
    """Two or three users reverting each other on one page."""
    title, users = _title(rng), [f"Warrior{rng.randint(1, 999)}{c}" for c in "abc"[: rng.randint(2, 3)]]
    size = rng.randint(500, 5000)
    return [dict(title=title, user=users[k % len(users)], is_revert=k > 0, byte_delta=size if k % 2 else -size)
            for k in range(4 * i)]


def bot_flood(rng: random.Random, i: int) -> list[dict]:
    """One bot account editing at a very high rate."""
    bot = f"FloodBot{rng.randint(1, 999)}"
    return [dict(title=_title(rng), user=bot, is_bot=True, byte_delta=rng.randint(-50, 50)) for _ in range(15 * i)]


def page_blank(rng: random.Random, i: int) -> list[dict]:
    """A few edits removing 20 kB to 100 kB each."""
    return [dict(title=_title(rng), user=_temp_account(rng), is_anon=True,
                 byte_delta=-rng.randint(20_000 * i, min(100_000, 20_000 * i + 20_000)))
            for _ in range(1 + i // 2)]


def spam_pages(rng: random.Random, i: int) -> list[dict]:
    """A burst of new pages from anonymous or brand-new accounts."""
    return [dict(title=_title(rng), user=_temp_account(rng) if k % 2 else f"NewUser{rng.randint(1, 10**6)}",
                 is_anon=bool(k % 2), is_new=True, byte_delta=rng.randint(500, 3000))
            for k in range(4 * i)]


ATTACKS = {f.__name__: f for f in (vandal_wave, edit_war, bot_flood, page_blank, spam_pages)}


def fit_in_window(now: datetime, duration: timedelta) -> timedelta:
    """Shorten the attack so it ends inside the current window instead of being split across two."""
    return min(duration, window_start(now) + WINDOW - now)


def generate(name: str, start: datetime, duration: timedelta, intensity: int, seed: int | None = None) -> list[Edit]:
    if not 1 <= intensity <= 5:
        raise ValueError(f"intensity must be 1 to 5, got {intensity}")
    rng = random.Random(seed)
    specs = ATTACKS[name](rng, intensity)
    offsets = sorted(rng.random() * duration for _ in specs)
    return [
        Edit(
            event_id=str(uuid.UUID(int=rng.getrandbits(128))),
            dt=start + offset,
            title=spec["title"],
            title_url="https://en.wikipedia.org/wiki/" + spec["title"].replace(" ", "_"),
            user=spec["user"],
            is_bot=spec.get("is_bot", False),
            is_anon=spec.get("is_anon", False),
            is_new=spec.get("is_new", False),
            byte_delta=spec["byte_delta"],
            is_revert=spec.get("is_revert", False),
            rev_id=None,
            synthetic=True,
        )
        for spec, offset in zip(specs, offsets)
    ]
