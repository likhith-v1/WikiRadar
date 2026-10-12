import subprocess
import sys


def test_reset_db_help_does_not_touch_the_database():
    out = subprocess.run([sys.executable, "scripts/reset_db.py", "--help"], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0
    assert "usage" in out.stdout
    assert "truncated" not in out.stdout


def test_evaluate_rejects_a_contamination_outside_the_sweep(tmp_path):
    missing = str(tmp_path / "missing.jsonl")  # so a regression fails fast instead of running the evaluation
    out = subprocess.run([sys.executable, "scripts/evaluate.py", "--contamination", "0.03",
                          "--train", missing, "--holdout", missing], capture_output=True, text=True, timeout=60)
    assert out.returncode == 2
    assert "invalid choice" in out.stderr
