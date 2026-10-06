from datetime import datetime, timedelta, timezone

import pytest

from wikiradar.attacks import ATTACKS, fit_in_window, generate

START = datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc)
DURATION = timedelta(seconds=5)


@pytest.mark.parametrize("name", sorted(ATTACKS))
@pytest.mark.parametrize("intensity", [1, 5])
def test_attack_edits_are_synthetic_and_inside_the_window(name, intensity):
    edits = generate(name, START, DURATION, intensity, seed=1)
    assert edits
    assert all(e.synthetic for e in edits)
    assert all(START <= e.dt < START + DURATION for e in edits)
    assert len({e.event_id for e in edits}) == len(edits)


@pytest.mark.parametrize("name", sorted(ATTACKS))
def test_higher_intensity_is_not_smaller(name):
    assert len(generate(name, START, DURATION, 5, seed=1)) >= len(generate(name, START, DURATION, 1, seed=1))


def test_same_seed_gives_same_attack():
    a = generate("vandal_wave", START, DURATION, 3, seed=7)
    b = generate("vandal_wave", START, DURATION, 3, seed=7)
    assert [(e.dt, e.user, e.title, e.byte_delta) for e in a] == [(e.dt, e.user, e.title, e.byte_delta) for e in b]


def test_attack_shapes():
    vandal = generate("vandal_wave", START, DURATION, 3, seed=1)
    assert all(e.is_anon and e.byte_delta < 0 for e in vandal)
    assert len({e.title for e in vandal}) > len(vandal) // 2

    war = generate("edit_war", START, DURATION, 3, seed=1)
    assert len({e.title for e in war}) == 1
    assert 2 <= len({e.user for e in war}) <= 3
    assert sum(e.is_revert for e in war) >= len(war) // 2

    flood = generate("bot_flood", START, DURATION, 3, seed=1)
    assert len({e.user for e in flood}) == 1 and all(e.is_bot for e in flood)

    blank = generate("page_blank", START, DURATION, 5, seed=1)
    assert len(blank) <= 3 and all(-100_000 <= e.byte_delta <= -20_000 for e in blank)

    spam = generate("spam_pages", START, DURATION, 3, seed=1)
    assert all(e.is_new and e.byte_delta > 0 for e in spam)


def test_unknown_attack_or_intensity_raises():
    with pytest.raises(KeyError):
        generate("nope", START, DURATION, 3)
    with pytest.raises(ValueError):
        generate("edit_war", START, DURATION, 6)


@pytest.mark.parametrize(
    "name, count",
    [("vandal_wave", 18), ("edit_war", 12), ("bot_flood", 45), ("page_blank", 2), ("spam_pages", 12)],
)
def test_counts_at_intensity_3(name, count):
    assert len(generate(name, START, DURATION, 3, seed=1)) == count


def test_attack_keeps_its_duration_when_it_fits_in_the_current_window():
    assert fit_in_window(START + timedelta(seconds=4), DURATION) == DURATION


def test_attack_is_squeezed_into_the_rest_of_the_window_instead_of_straddling():
    assert fit_in_window(START + timedelta(seconds=8), DURATION) == timedelta(seconds=2)
