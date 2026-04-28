# app.py – RecruitIQ Main Entry Point
# Python 3.14 | Streamlit | Supabase + psycopg2

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env BEFORE anything else touches os.getenv ─────────────────────────
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import streamlit as st

# ── Page config (must be first Streamlit call) ────────────────────────────────
st.set_page_config(
    page_title="RecruitIQ",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── UI helpers ────────────────────────────────────────────────────────────────
from ui.styles import apply_global_styles
from ui.auth   import check_auth

apply_global_styles()

if not check_auth():
    st.stop()

# ── Page module imports ───────────────────────────────────────────────────────
# Each module exposes only a render_* function; no widgets run at import time.
from ui.pages.dashboard   import render_dashboard
from ui.pages.profiles    import render_profile_database
from ui.pages.jd_manager  import render_jd_manager
from ui.pages.matching    import render_matching          # ← was render_jd_matching
from ui.pages.tracker     import render_candidate_tracker
from ui.pages.interviews  import render_interview_scheduler
from ui.pages.social      import render_social_media
from ui.pages.ai_sourcing import render_ai_sourcing
from ui.pages.upload      import render_upload_page

# ── Page registry ─────────────────────────────────────────────────────────────
_PAGES: dict[str, object] = {
    "🏠 Dashboard"    : render_dashboard,
    "👥 Profiles"     : render_profile_database,
    "📄 JD Manager"   : render_jd_manager,
    "🎯 JD Matching"  : render_matching,                 # ← was render_jd_matching
    "📊 Tracker"      : render_candidate_tracker,
    "📅 Interviews"   : render_interview_scheduler,
    "📱 Social Media" : render_social_media,
    "🤖 AI Sourcing"  : render_ai_sourcing,
    "⬆️  Upload"      : render_upload_page,
}

# ── NAV SESSION STATE KEY ─────────────────────────────────────────────────────
# IMPORTANT: Use "nav_page" exclusively for navigation state.
# Never use bare "page" — page modules (e.g. profiles) use "prof_page",
# other modules may use their own prefixed keys. A bare "page" key is
# reserved for no one to avoid cross-module collisions.
_NAV_KEY = "nav_page"


# ── Service factory ───────────────────────────────────────────────────────────
@st.cache_resource
def build_services() -> dict:
    """
    Builds and caches all heavy singleton services.

    Supabase key resolution order (first non-empty wins):
      SUPABASE_KEY  →  SUPABASE_ANON_KEY  →  SUPABASE_SERVICE_KEY
      →  SUPABASE_SERVICE_ROLE_KEY
    """
    from database.supabase_manager             import SupabaseManager
    from modules.jd_engine.jd_parser           import JDParser
    from modules.jd_engine.scoring_engine      import ProfileScoringEngine
    from modules.ai_engine.llm_handler         import LLMHandler
    from modules.social_media.banner_generator import SocialBannerGenerator

    SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
    SUPABASE_KEY = (
        os.getenv("SUPABASE_KEY",              "").strip()
        or os.getenv("SUPABASE_ANON_KEY",      "").strip()
        or os.getenv("SUPABASE_SERVICE_KEY",   "").strip()
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    )

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "Supabase credentials missing — "
            f"URL={'SET' if SUPABASE_URL else 'MISSING'}, "
            f"KEY={'SET' if SUPABASE_KEY else 'MISSING'}"
        )

    db         = SupabaseManager(url=SUPABASE_URL, key=SUPABASE_KEY)
    llm        = LLMHandler()
    jd_parser  = JDParser(llm=llm)
    scorer     = ProfileScoringEngine(llm=llm)
    banner_gen = SocialBannerGenerator(llm=llm)

    return {
        "db"        : db,
        "llm"       : llm,
        "jd_parser" : jd_parser,
        "scorer"    : scorer,
        "banner_gen": banner_gen,
    }


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar(pages: dict) -> str:
    """
    Renders the sidebar navigation and returns the selected page key.

    Navigation state is stored under st.session_state[_NAV_KEY] ("nav_page").
    This key is intentionally distinct from any page-level state keys
    (e.g. "prof_page", "tracker_page") to prevent cross-module collisions.
    """
    with st.sidebar:
        if os.path.exists("assets/logo.png"):
            st.image("assets/logo.png", use_container_width=True)
        else:
            st.title("🎯 RecruitIQ")

        st.markdown("---")

        page_keys    = list(pages.keys())
        default_page = "🏠 Dashboard"

        # ── Safe index resolution ─────────────────────────────────────────────
        # _NAV_KEY may hold a stale value after hot-reload or logout;
        # fall back to Dashboard if the stored key is no longer valid.
        current_nav = st.session_state.get(_NAV_KEY, default_page)

        if current_nav not in page_keys:
            current_nav = default_page
            st.session_state[_NAV_KEY] = current_nav

        safe_index = page_keys.index(current_nav)

        # st.radio writes its own value into key="nav_radio" automatically;
        # we separately persist the result under _NAV_KEY so it survives reruns.
        selection = st.radio(
            "Navigation",
            page_keys,
            index=safe_index,
            label_visibility="collapsed",
            key="nav_radio",         # widget key — not used outside sidebar
        )

        # Persist selection under our explicit nav key
        st.session_state[_NAV_KEY] = selection

        st.markdown("---")

        # ── Quick stats ───────────────────────────────────────────────────────
        try:
            services = build_services()
            db       = services["db"]

            total_candidates = (
                db.get_candidate_count()
                if hasattr(db, "get_candidate_count") else "—"
            )
            active_jds = (
                db.get_active_jd_count()
                if hasattr(db, "get_active_jd_count") else "—"
            )
            open_positions = (
                db.get_open_position_count()
                if hasattr(db, "get_open_position_count") else "—"
            )

            st.markdown("**📊 Quick Stats**")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Candidates", total_candidates)
                st.metric("Active JDs",  active_jds)
            with col2:
                st.metric("Open Roles",  open_positions)

            st.markdown("---")

        except Exception:
            pass  # sidebar stats must never crash the app

        # ── Logout ────────────────────────────────────────────────────────────
        if st.button("🚪 Logout", use_container_width=True):
            # Wipe ALL session state so every page module starts fresh
            st.session_state.clear()
            st.rerun()

        st.caption("RecruitIQ v1.0 · AI-Powered Recruiting")

    return selection


# ── Router ────────────────────────────────────────────────────────────────────
def route(selection: str, pages: dict, services: dict) -> None:
    """
    Dispatch to the correct page render function.

    Argument contract
    -----------------
    Dashboard  → fn(db)       owns its own layout, needs only the db handle
    Profiles   → fn()         manages its own psycopg2 pool, needs nothing
    All others → fn(services) receive the full services dict
    """
    fn = pages.get(selection)
    if fn is None:
        st.error(f"Page '{selection}' not found.")
        return

    if selection == "🏠 Dashboard":
        fn(services["db"])
    elif selection == "👥 Profiles":
        fn()                    # render_profile_database() — no args needed
    else:
        try:
            fn(services)
        except TypeError as exc:
            # Graceful fallback: some pages may not yet accept services
            st.warning(f"Page called without services dict ({exc}) — retrying bare call.")
            fn()


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    # ── Build services ────────────────────────────────────────────────────────
    try:
        services = build_services()
    except ValueError as exc:
        st.error(f"⚠️ Configuration error: {exc}")
        st.info(
            "Add your credentials to the `.env` file:\n"
            "```\n"
            "SUPABASE_URL=https://xxxx.supabase.co\n"
            "SUPABASE_KEY=your-anon-or-service-key\n"
            "OPENAI_API_KEY=sk-...\n"
            "```"
        )
        st.stop()
    except Exception as exc:
        st.error(f"⚠️ Unexpected startup error: {exc}")
        st.exception(exc)
        st.stop()

    # ── Render sidebar & route ────────────────────────────────────────────────
    selection = render_sidebar(_PAGES)

    try:
        route(selection, _PAGES, services)
    except Exception as exc:
        st.error(f"⚠️ Error rendering **{selection}**: {exc}")
        st.exception(exc)
        if st.button("🔄 Reload page"):
            st.rerun()


if __name__ == "__main__":
    main()
