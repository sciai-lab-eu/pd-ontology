#!/usr/bin/env python3
"""Shared helpers for the SciAI maintenance automation stack.

This module is imported by all scripts under /55a11/maintenance/. It is kept
free of heavy third-party dependencies so it can be imported quickly by any
venv (sciai/.venv or .venv-vllm). Heavy imports are deferred inside functions.
"""
from __future__ import annotations

import fcntl
import json
import logging
import os
import subprocess
import sys
from contextlib import contextmanager
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote_plus

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path("/55a11")
SCIAI_NEXT = REPO_ROOT / "sciai_next"
LOG_DIR = REPO_ROOT / "logs"
PROGRESS_DIR = LOG_DIR / "automation_progress"
TOPIC_REGISTRY_PATH = REPO_ROOT / "config" / "sciai_harvesting_topics.yaml"
DEFAULT_SCHEMA = "universal_pd_v3"

# Python interpreters used by cron
VLLM_PYTHON = REPO_ROOT / ".venv-vllm" / "bin" / "python"
SCIAI_PYTHON = REPO_ROOT / "sciai" / ".venv" / "bin" / "python"


# ---------------------------------------------------------------------------
# Environment / DSN
# ---------------------------------------------------------------------------
def ensure_sciai_next_on_path() -> None:
    """Make sciai_next importable for scripts running in maintenance/."""
    if str(SCIAI_NEXT) not in sys.path:
        sys.path.insert(0, str(SCIAI_NEXT))


def load_dotenv_repo() -> None:
    """Load /55a11/.env if dotenv is available."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=False)


def get_dsn() -> str:
    """Resolve a PostgreSQL DSN from environment variables."""
    for var in ("DATABASE_URL", "PG_DSN_SCIAI", "PG_DSN"):
        dsn = os.getenv(var)
        if dsn:
            if dsn.startswith("postgresql+psycopg2://"):
                dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")
            return dsn

    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "sciai")
    password = os.getenv("POSTGRES_PASSWORD", "")
    db = os.getenv("POSTGRES_DB", "sciai")
    pw = quote_plus(password) if password else ""
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


@contextmanager
def db_cursor():
    """Yield a psycopg cursor with automatic commit/rollback."""
    import psycopg

    conn = psycopg.connect(get_dsn(), autocommit=False)
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging(name: str, log_file: Optional[Path] = None) -> logging.Logger:
    """Configure a logger writing to stdout and an optional file."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Avoid duplicate handlers if called multiple times in the same process.
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(fmt)
    logger.addHandler(stdout)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Topic registry
# ---------------------------------------------------------------------------
class TopicRegistry:
    """Lightweight wrapper around sciai_harvesting_topics.yaml."""

    def __init__(self, path: Optional[Path] = None):
        self.path = path or TOPIC_REGISTRY_PATH
        self._data: Optional[Dict[str, Any]] = None

    def _load(self) -> Dict[str, Any]:
        if self._data is None:
            import yaml

            if not self.path.exists():
                raise FileNotFoundError(f"Topic registry not found: {self.path}")
            with open(self.path, "r", encoding="utf-8") as f:
                self._data = yaml.safe_load(f) or {}
        return self._data

    def enabled_topics(self) -> List[Dict[str, Any]]:
        """Return all enabled topics."""
        return [
            t
            for t in self._load().get("topics", [])
            if t.get("enabled", False)
        ]

    def get_topic(self, key: str) -> Optional[Dict[str, Any]]:
        """Return a topic by key."""
        for t in self._load().get("topics", []):
            if t.get("key") == key:
                return t
        return None


# ---------------------------------------------------------------------------
# Locking
# ---------------------------------------------------------------------------
class LockFile:
    """Process-wide advisory lock based on a pid file."""

    def __init__(self, name: str):
        PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
        self.path = PROGRESS_DIR / f"{name}.lock"
        self._fd: Optional[int] = None

    def acquire(self, timeout: float = 0.0) -> bool:
        """Try to acquire the lock. Return True on success."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR)
        try:
            if timeout > 0:
                import time

                deadline = time.time() + timeout
                while time.time() < deadline:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        self._fd = fd
                        os.write(fd, f"{os.getpid()}\n".encode())
                        return True
                    except BlockingIOError:
                        time.sleep(0.5)
                os.close(fd)
                return False
            else:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._fd = fd
                os.write(fd, f"{os.getpid()}\n".encode())
                return True
        except BlockingIOError:
            os.close(fd)
            return False

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except Exception:
                pass
            self._fd = None
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError(f"Another process holds the lock: {self.path}")
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False


# ---------------------------------------------------------------------------
# Progress / reporting
# ---------------------------------------------------------------------------
def progress_file(name: str) -> Path:
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    return PROGRESS_DIR / f"{name}.json"


def load_progress(name: str) -> Dict[str, Any]:
    path = progress_file(name)
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_progress(name: str, data: Dict[str, Any]) -> None:
    path = progress_file(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    merged = load_progress(name)
    merged.update(data)
    merged["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, default=str)


def iso_week(dt: Optional[datetime] = None) -> str:
    """Return ISO calendar week string YYYY-WNN."""
    dt = dt or datetime.now(timezone.utc)
    return f"{dt.year}-W{dt.isocalendar().week:02d}"


def write_markdown_report(
    week: str,
    sections: List[Tuple[str, str]],
    path: Optional[Path] = None,
) -> Path:
    """Write a markdown summary and return its path."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = path or LOG_DIR / f"sciai_automation_summary_{week}.md"
    lines = [f"# SciAI Automation Summary — {week}", ""]
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()} UTC")
    lines.append("")
    for heading, body in sections:
        lines.append(f"## {heading}")
        lines.append("")
        lines.append(body)
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Time guards
# ---------------------------------------------------------------------------
def now_cet() -> datetime:
    """Return current time in CET/CEST."""
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Europe/Berlin"))


def in_night_window(
    start: dt_time = dt_time(20, 0),
    end: dt_time = dt_time(5, 0),
    tz: str = "Europe/Berlin",
) -> bool:
    """Return True if current time in `tz` is between start and end (crosses midnight)."""
    from zoneinfo import ZoneInfo

    now = datetime.now(ZoneInfo(tz)).time()
    if start <= end:
        return start <= now <= end
    return now >= start or now <= end


def last_n_days(n: int, today: Optional[date] = None) -> Tuple[date, date]:
    """Return (mindate, maxdate) for the last n days inclusive."""
    today = today or date.today()
    return today - timedelta(days=n - 1), today


# ---------------------------------------------------------------------------
# Process / GPU checks
# ---------------------------------------------------------------------------
def is_process_running(pattern: str) -> bool:
    try:
        out = subprocess.run(
            ["pgrep", "-f", pattern],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        return bool(out.stdout.strip())
    except Exception:
        return False


def is_vllm_process_running() -> bool:
    """Best-effort check that a bulk vLLM extraction is active."""
    return is_process_running("bulk_extract_pd_vllm.py")


def is_ollama_model_loaded() -> bool:
    """Check whether Ollama currently has a model loaded in VRAM."""
    try:
        out = subprocess.run(
            ["ollama", "ps"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if out.stdout.strip() and "NAME" not in out.stdout:
            return True
    except Exception:
        pass
    return False


def gpu_vram_ratio() -> Optional[float]:
    """Return used/total GPU memory ratio, or None if unavailable."""
    try:
        out = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if out.returncode == 0:
            used, total = out.stdout.strip().split(",")
            total_mb = float(total)
            if total_mb > 0:
                return float(used) / total_mb
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------
def run_subprocess(
    cmd: List[str],
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
    logger: Optional[logging.Logger] = None,
) -> subprocess.CompletedProcess:
    """Run a subprocess and log command/output succinctly."""
    if logger:
        logger.info("Running: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(cwd) if cwd else None,
        env=env,
    )
    if logger:
        if proc.returncode != 0:
            logger.error(
                "Command failed (rc=%d)\nstdout:\n%s\nstderr:\n%s",
                proc.returncode,
                proc.stdout[-2000:],
                proc.stderr[-2000:],
            )
        else:
            logger.info(
                "Command succeeded (rc=0)\nstdout tail:\n%s",
                proc.stdout[-2000:],
            )
    return proc


# ---------------------------------------------------------------------------
# Date formatting for PubMed
# ---------------------------------------------------------------------------
def ncbi_date_range(days: int = 7, sep: str = "/") -> Tuple[str, str]:
    """Return (mindate, maxdate) strings suitable for NCBI ESearch."""
    mindate, maxdate = last_n_days(days)
    return mindate.strftime("%Y/%m/%d"), maxdate.strftime("%Y/%m/%d")
