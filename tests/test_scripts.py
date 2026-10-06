import subprocess
import sys


def test_reset_db_help_does_not_touch_the_database():
    out = subprocess.run([sys.executable, "scripts/reset_db.py", "--help"], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0
    assert "usage" in out.stdout
    assert "truncated" not in out.stdout
