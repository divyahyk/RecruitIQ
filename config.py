# config.py — RecruitIQ Central Configuration
# Merges: branding, DB (Supabase REST + PostgreSQL pooler), LLM, auth, email,
#         pipeline stages, and pg_dsn resolution logic.

from __future__ import annotations

import os
import re
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# DSN resolution (module-level, used by psycopg2 connection pool)
# ─────────────────────────────────────────────────────────────────────────────

def get_pg_dsn() -> str:
    """
    Return the best available PostgreSQL DSN for psycopg2.

    Priority order
    ──────────────
    1. PG_CONN_STRING  — Supabase Session Pooler (aws-1-...pooler.supabase.com:5432)
                         Works from local machines, cloud deployments, and CI.
                         ✅ Preferred.

    2. DATABASE_URL    — Direct host (db.xxx.supabase.co:5432)
                         May fail outside Supabase infra (DNS blocked).
                         ⚠️  Fallback only.

    Raises
    ──────
    EnvironmentError  if neither variable is set or both are empty.
    """
    dsn = (
        os.getenv("PG_CONN_STRING", "").strip()
        or os.getenv("DATABASE_URL", "").strip()
    )

    if not dsn:
        raise EnvironmentError(
            "No PostgreSQL DSN found.\n\n"
            "Set PG_CONN_STRING in your .env file:\n"
            "  PG_CONN_STRING=postgresql://postgres.<ref>:<password>"
            "@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres\n\n"
            "Find it:\n"
            "  Supabase Dashboard → Settings → Database\n"
            "  → Connection Pooling → Mode: Session → copy string"
        )

    return dsn


def redact_dsn(dsn: str) -> str:
    """Return DSN with password replaced by **** (safe for logging / UI)."""
    return re.sub(r":([^:@]+)@", ":****@", dsn)


def friendly_conn_error(raw: str, dsn: str) -> str:
    """
    Convert a raw psycopg2 OperationalError message into an actionable
    markdown string suitable for display in Streamlit error panels.
    """
    safe = redact_dsn(dsn)

    patterns: list[tuple[str, str]] = [
        (
            "could not translate host name|Name or service not known",
            (
                "🌐 **DNS resolution failed** — the database host cannot be reached.\n\n"
                f"**DSN used:** `{safe}`\n\n"
                "**Fix:** Set `PG_CONN_STRING` to the **Supabase Session Pooler** URL:\n"
                "```\n"
                "PG_CONN_STRING=postgresql://postgres.<ref>:<password>"
                "@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres\n"
                "```\n"
                "**Where to find it:**  \n"
                "Supabase Dashboard → Settings → Database → "
                "Connection Pooling → Mode: **Session** → copy string"
            ),
        ),
        (
            "password authentication failed",
            (
                "🔑 **Wrong database password.**\n\n"
                f"**DSN used:** `{safe}`\n\n"
                "Reset it:  \n"
                "Supabase Dashboard → Settings → Database → "
                "**Reset database password**, then update `PG_CONN_STRING` in `.env`."
            ),
        ),
        (
            "SSL",
            (
                "🔒 **SSL negotiation failed.**\n\n"
                f"**DSN used:** `{safe}`\n\n"
                "Try appending `?sslmode=require` to your `PG_CONN_STRING`:\n"
                "```\n"
                "PG_CONN_STRING=postgresql://...postgres?sslmode=require\n"
                "```"
            ),
        ),
        (
            "too many connections|connection pool",
            (
                "🔄 **Connection pool exhausted.**\n\n"
                "Too many concurrent sessions. Refresh after a few seconds.  \n"
                "If this persists, increase `maxconn` in `_get_pool()` or switch "
                "to **Transaction Pooler** mode (port 6543)."
            ),
        ),
        (
            "timeout|timed out",
            (
                "⏱️ **Connection timed out.**\n\n"
                f"**DSN used:** `{safe}`\n\n"
                "Possible causes:\n"
                "- Supabase project is paused (free tier auto-pauses after 1 week)\n"
                "- Network firewall blocking outbound port 5432\n\n"
                "Check: Supabase Dashboard → Project → **Resume project** if paused."
            ),
        ),
    ]

    for pattern, message in patterns:
        if re.search(pattern, raw, re.IGNORECASE):
            return message

    # Generic fallback
    return (
        "🔌 **Database connection failed.**\n\n"
        f"**DSN used:** `{safe}`\n\n"
        f"**Raw error:**\n```\n{raw}\n```"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main Config class
# ─────────────────────────────────────────────────────────────────────────────

class Config:
    """
    Central configuration for RecruitIQ.

    All settings are class attributes resolved from environment variables at
    import time.  Never instantiate this class; use Config.ATTR directly.
    """

    # ── Branding ──────────────────────────────────────────────────────────────
    APP_NAME             : str = "RecruitIQ"
    APP_TAGLINE          : str = "Intelligent Recruitment Platform"
    APP_VERSION          : str = "1.0.0"
    APP_LOGO             : str = "🧠"
    APP_COLOR_PRIMARY    : str = "#6C63FF"
    APP_COLOR_SECONDARY  : str = "#FF6584"
    APP_COLOR_ACCENT     : str = "#43E8D8"

    # ── Supabase REST API ─────────────────────────────────────────────────────
    # Used by: supabase-py client, PostgREST calls
    SUPABASE_URL         : str = os.getenv("SUPABASE_URL",      "").strip()
    SUPABASE_ANON_KEY    : str = os.getenv("SUPABASE_ANON_KEY", "").strip()

    # Alias: your .env uses SUPABASE_KEY (not SUPABASE_ANON_KEY)
    # We try both so neither .env format breaks anything.
    if not SUPABASE_ANON_KEY:
        SUPABASE_ANON_KEY = os.getenv("SUPABASE_KEY", "").strip()

    # ── PostgreSQL / psycopg2 ─────────────────────────────────────────────────
    # Raw strings stored here for inspection / diagnostics.
    # Always call get_pg_dsn() at runtime — it applies priority logic.
    PG_CONN_STRING       : str = os.getenv("PG_CONN_STRING", "").strip()
    DATABASE_URL         : str = os.getenv("DATABASE_URL",   "").strip()

    # ── Local fallback ────────────────────────────────────────────────────────
    SQLITE_PATH          : str = os.getenv("SQLITE_PATH", "recruitiq_local.db").strip()

    # ── LLM Providers ────────────────────────────────────────────────────────
    GROQ_API_KEY         : str = os.getenv("GROQ_API_KEY",    "").strip()
    GROQ_MODEL           : str = os.getenv("GROQ_MODEL",      "llama-3.1-8b-instant").strip()

    GOOGLE_API_KEY       : str = os.getenv("GOOGLE_API_KEY",  "").strip()
    GEMINI_MODEL         : str = os.getenv("GEMINI_MODEL",    "gemini-1.5-flash-8b").strip()

    MISTRAL_API_KEY      : str = os.getenv("MISTRAL_API_KEY", "").strip()
    MISTRAL_MODEL        : str = os.getenv("MISTRAL_MODEL",   "open-mistral-nemo").strip()

    HF_API_TOKEN         : str = os.getenv("HF_API_TOKEN",    "").strip()
    HF_MODEL             : str = os.getenv(
        "HF_MODEL", "mistralai/Mistral-7B-Instruct-v0.2"
    ).strip()

    OLLAMA_BASE_URL      : str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").strip()
    OLLAMA_MODEL         : str = os.getenv("OLLAMA_MODEL",    "llama3.1:8b").strip()

    LLM_PROVIDER         : str = os.getenv("LLM_PROVIDER",    "groq").strip()

    # OpenAI (optional, not in your .env but kept for future use)
    OPENAI_API_KEY       : str = os.getenv("OPENAI_API_KEY",  "").strip()

    # ── Auth ──────────────────────────────────────────────────────────────────
    APP_PASSWORD         : str = os.getenv("APP_PASSWORD", "").strip()

    # ── Email / SMTP ─────────────────────────────────────────────────────────
    SMTP_HOST            : str = os.getenv("SMTP_HOST",  "smtp.gmail.com").strip()
    SMTP_PORT            : int = int(os.getenv("SMTP_PORT", "587").strip())
    SMTP_USER            : str = os.getenv("SMTP_USER",  "").strip()
    SMTP_PASS            : str = os.getenv("SMTP_PASS",  "").strip()
    FROM_EMAIL           : str = os.getenv("FROM_EMAIL", "careers@terragig.com").strip()

    # ── Recruitment Pipeline ──────────────────────────────────────────────────
    PIPELINE_STAGES: List[str] = [
        "New",
        "Screening",
        "Shortlisted",
        "Submitted",
        "Interview L1",
        "Interview L2",
        "Final",
        "Offered",
        "Joined",
        "Rejected",
    ]

    STAGE_COLORS: Dict[str, str] = {
        "New"         : "#6C63FF",
        "Screening"   : "#4ECDC4",
        "Shortlisted" : "#45B7D1",
        "Submitted"   : "#96CEB4",
        "Interview L1": "#FFEAA7",
        "Interview L2": "#DDA0DD",
        "Final"       : "#FF8C69",
        "Offered"     : "#98FB98",
        "Joined"      : "#2ECC71",
        "Rejected"    : "#E74C3C",
    }

    # ── Class-level helpers ───────────────────────────────────────────────────

    @classmethod
    def get_pg_dsn(cls) -> str:
        """
        Instance-style accessor — delegates to module-level get_pg_dsn().
        Allows callers to write either:
            config.get_pg_dsn()          # module-level
            Config.get_pg_dsn()          # class-level
        """
        return get_pg_dsn()

    @classmethod
    def get_available_providers(cls) -> List[str]:
        """
        Return ordered list of configured LLM providers.

        Order: groq → gemini → mistral → huggingface (always present as fallback)
        """
        providers: List[str] = []
        if cls.GROQ_API_KEY:
            providers.append("groq")
        if cls.GOOGLE_API_KEY:
            providers.append("gemini")
        if cls.MISTRAL_API_KEY:
            providers.append("mistral")
        if cls.OPENAI_API_KEY:
            providers.append("openai")
        providers.append("huggingface")   # always available (no key needed for public models)
        return providers

    @classmethod
    def get_best_provider(cls) -> str:
        """
        Return the single best available provider, preferring the env-specified
        LLM_PROVIDER if it is actually configured.
        """
        available = cls.get_available_providers()
        if cls.LLM_PROVIDER in available:
            return cls.LLM_PROVIDER
        return available[0] if available else "none"

    @classmethod
    def db_mode(cls) -> str:
        """
        Return the active database mode string.

        'supabase_pg'   — psycopg2 via pooler (PG_CONN_STRING set)
        'supabase_rest' — supabase-py REST only (no PG_CONN_STRING)
        'sqlite'        — no Supabase config at all
        """
        if cls.PG_CONN_STRING or cls.DATABASE_URL:
            return "supabase_pg"
        if cls.SUPABASE_URL:
            return "supabase_rest"
        return "sqlite"

    @classmethod
    def validate(cls) -> Dict[str, object]:
        """
        Return a diagnostics dictionary consumed by the app health-check panel.

        Keys
        ────
        database        str   — active db mode
        pg_dsn_source   str   — which env var is providing the DSN
        pg_dsn_safe     str   — redacted DSN (safe to display)
        llm_primary     str   — best available LLM provider
        llm_fallbacks   list  — remaining providers
        email_configured bool
        warnings        list[str]
        """
        providers = cls.get_available_providers()
        warnings: List[str] = []

        # LLM warnings
        if not cls.GROQ_API_KEY and not cls.GOOGLE_API_KEY and not cls.MISTRAL_API_KEY:
            warnings.append("No LLM API keys configured — AI features disabled.")

        # DB warnings
        mode = cls.db_mode()
        if mode == "sqlite":
            warnings.append("No Supabase config — using local SQLite fallback.")
        elif mode == "supabase_rest":
            warnings.append(
                "PG_CONN_STRING not set — running on REST API only "
                "(bulk operations will be slow)."
            )

        # Supabase key alias detection
        if not os.getenv("SUPABASE_ANON_KEY") and os.getenv("SUPABASE_KEY"):
            warnings.append(
                "Using SUPABASE_KEY as SUPABASE_ANON_KEY alias — "
                "consider renaming in .env for clarity."
            )

        # Determine DSN source
        if cls.PG_CONN_STRING:
            pg_source = "PG_CONN_STRING (session pooler ✅)"
            pg_safe   = redact_dsn(cls.PG_CONN_STRING)
        elif cls.DATABASE_URL:
            pg_source = "DATABASE_URL (direct host ⚠️ — may fail locally)"
            pg_safe   = redact_dsn(cls.DATABASE_URL)
            warnings.append(
                "DATABASE_URL uses direct host which may fail outside Supabase. "
                "Set PG_CONN_STRING to the Session Pooler URL instead."
            )
        else:
            pg_source = "not configured"
            pg_safe   = "—"

        # Email warnings
        if not cls.SMTP_USER or not cls.SMTP_PASS:
            warnings.append("SMTP credentials not set — email features disabled.")

        return {
            "database"        : mode,
            "pg_dsn_source"   : pg_source,
            "pg_dsn_safe"     : pg_safe,
            "llm_primary"     : providers[0] if providers else "none",
            "llm_fallbacks"   : providers[1:] if len(providers) > 1 else [],
            "email_configured": bool(cls.SMTP_USER and cls.SMTP_PASS),
            "warnings"        : warnings,
        }

    @classmethod
    def print_summary(cls) -> None:
        """Print a startup diagnostic to stdout (useful in dev / CI logs)."""
        v = cls.validate()
        print(f"\n{'═' * 55}")
        print(f"  {cls.APP_LOGO}  {cls.APP_NAME} {cls.APP_VERSION} — Config Summary")
        print(f"{'═' * 55}")
        print(f"  DB mode      : {v['database']}")
        print(f"  PG DSN from  : {v['pg_dsn_source']}")
        print(f"  PG DSN       : {v['pg_dsn_safe']}")
        print(f"  LLM primary  : {v['llm_primary']}")
        print(f"  LLM fallbacks: {', '.join(v['llm_fallbacks']) or '—'}")
        print(f"  Email ready  : {v['email_configured']}")
        if v["warnings"]:
            print(f"\n  ⚠️  Warnings:")
            for w in v["warnings"]:
                print(f"     • {w}")
        print(f"{'═' * 55}\n")
