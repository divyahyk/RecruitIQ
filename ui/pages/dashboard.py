# ui/pages/dashboard.py
"""
Dashboard page — real-time recruitment intelligence overview.

Receives: db (SupabaseManager instance, may be None)
Called by app.py: render_dashboard(services["db"])
"""

import streamlit as st


# ─────────────────────────────────────────────────────────────────────────────
#  PAGE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def render_dashboard(db) -> None:
    """
    db → SupabaseManager instance (may be None if DB not connected).
    """
    st.title("🏠 Dashboard")
    st.markdown("### 📊 Real-time recruitment intelligence overview")

    # ── Guard: no DB connection ───────────────────────────────────────────────
    if db is None:
        st.warning(
            "⚠️ **Database not connected.** "
            "Check your `.env` file for `SUPABASE_URL` and `SUPABASE_KEY`, "
            "then restart the app.",
            icon="🔌",
        )
        _render_empty_kpis()
        return

    # ── Live KPIs ─────────────────────────────────────────────────────────────
    _render_kpis(db)
    st.divider()

    col_left, col_right = st.columns([3, 2])
    with col_left:
        _render_pipeline_funnel(db)
    with col_right:
        _render_recent_activity(db)


# ─────────────────────────────────────────────────────────────────────────────
#  KPI HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _kpi_card(col, icon: str, label: str, value, color: str) -> None:
    """Render a single KPI card into a Streamlit column."""
    display = f"{value:,}" if isinstance(value, int) else str(value)
    with col:
        st.markdown(
            f"""
            <div style="
                background:#FFFFFF;
                border:1px solid #E5E7EB;
                border-radius:12px;
                padding:20px 16px;
                text-align:center;
                box-shadow:0 1px 3px rgba(0,0,0,.06);
            ">
                <div style="font-size:1.8rem;">{icon}</div>
                <div style="font-size:1.9rem;font-weight:800;color:{color};
                            margin:6px 0 2px;letter-spacing:-1px;">
                    {display}
                </div>
                <div style="font-size:0.72rem;color:#6B7280;font-weight:500;
                            text-transform:uppercase;letter-spacing:.05em;">
                    {label}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_empty_kpis() -> None:
    """Placeholder KPI row when DB is unavailable."""
    items = [
        ("👥", "Total Candidates", "—", "#4F46E5"),
        ("📋", "Open JDs",         "—", "#10B981"),
        ("📨", "Applications",     "—", "#3B82F6"),
        ("📅", "Interviews Today", "—", "#F59E0B"),
        ("✅", "Hired (30d)",      "—", "#10B981"),
    ]
    cols = st.columns(5)
    for col, (icon, label, val, color) in zip(cols, items):
        _kpi_card(col, icon, label, val, color)


def _get_kpi_data(db) -> dict:
    """Fetch KPI numbers from Supabase. Returns safe defaults on any error."""
    data = {
        "candidates"      : 0,
        "open_jds"        : 0,
        "applications"    : 0,
        "interviews_today": 0,
        "hired_30d"       : 0,
    }

    try:
        r = db.client.table("candidates").select("id", count="exact").execute()
        data["candidates"] = r.count or 0
    except Exception:
        pass

    try:
        r = (
            db.client.table("job_descriptions")
            .select("id", count="exact")
            .eq("status", "Open")
            .execute()
        )
        data["open_jds"] = r.count or 0
    except Exception:
        pass

    try:
        r = db.client.table("applications").select("id", count="exact").execute()
        data["applications"] = r.count or 0
    except Exception:
        pass

    try:
        from datetime import date
        today = date.today().isoformat()
        r = (
            db.client.table("interviews")
            .select("id", count="exact")
            .eq("interview_date", today)
            .execute()
        )
        data["interviews_today"] = r.count or 0
    except Exception:
        pass

    try:
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        r = (
            db.client.table("applications")
            .select("id", count="exact")
            .eq("stage", "Hired")
            .gte("updated_at", cutoff)
            .execute()
        )
        data["hired_30d"] = r.count or 0
    except Exception:
        pass

    return data


def _render_kpis(db) -> None:
    kpi = _get_kpi_data(db)
    items = [
        ("👥", "Total Candidates", kpi["candidates"],        "#4F46E5"),
        ("📋", "Open JDs",         kpi["open_jds"],           "#10B981"),
        ("📨", "Applications",     kpi["applications"],       "#3B82F6"),
        ("📅", "Interviews Today", kpi["interviews_today"],   "#F59E0B"),
        ("✅", "Hired (30d)",      kpi["hired_30d"],          "#10B981"),
    ]
    cols = st.columns(5)
    for col, (icon, label, value, color) in zip(cols, items):
        _kpi_card(col, icon, label, value, color)


# ─────────────────────────────────────────────────────────────────────────────
#  PIPELINE FUNNEL
# ─────────────────────────────────────────────────────────────────────────────

def _render_pipeline_funnel(db) -> None:
    st.markdown("#### 🔽 Hiring Pipeline")

    stages = ["Applied", "Screening", "Interview", "Offer", "Hired"]
    colors = ["#4F46E5", "#6366F1", "#818CF8", "#A5B4FC", "#10B981"]
    counts = []

    for stage in stages:
        try:
            r = (
                db.client.table("applications")
                .select("id", count="exact")
                .eq("stage", stage)
                .execute()
            )
            counts.append(r.count or 0)
        except Exception:
            counts.append(0)

    total = max(counts[0], 1)   # avoid div/0; Applied is the widest bar

    for stage, count, color in zip(stages, counts, colors):
        pct = (count / total) * 100
        st.markdown(
            f"""
            <div style="margin-bottom:12px;">
                <div style="display:flex;justify-content:space-between;
                            font-size:0.78rem;font-weight:600;color:#374151;
                            margin-bottom:4px;">
                    <span>{stage}</span>
                    <span style="color:{color};">{count:,}</span>
                </div>
                <div style="background:#F3F4F6;border-radius:6px;height:10px;">
                    <div style="background:{color};width:{pct:.1f}%;
                                height:10px;border-radius:6px;
                                transition:width .4s ease;"></div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
#  RECENT ACTIVITY FEED
# ─────────────────────────────────────────────────────────────────────────────

def _render_recent_activity(db) -> None:
    st.markdown("#### 🕐 Recent Activity")

    # ── Recent applications ───────────────────────────────────────────────────
    try:
        r = (
            db.client.table("applications")
            .select("candidate_name, stage, updated_at")
            .order("updated_at", desc=True)
            .limit(8)
            .execute()
        )
        rows = r.data or []
    except Exception:
        rows = []

    if not rows:
        st.info("No recent activity to display.")
        return

    stage_colors = {
        "Applied"   : "#6366F1",
        "Screening" : "#3B82F6",
        "Interview" : "#F59E0B",
        "Offer"     : "#8B5CF6",
        "Hired"     : "#10B981",
        "Rejected"  : "#EF4444",
    }

    for row in rows:
        name      = row.get("candidate_name") or "Unknown"
        stage     = row.get("stage", "—")
        updated   = (row.get("updated_at") or "")[:10]   # YYYY-MM-DD
        dot_color = stage_colors.get(stage, "#9CA3AF")

        st.markdown(
            f"""
            <div style="display:flex;align-items:center;gap:10px;
                        padding:8px 0;border-bottom:1px solid #F3F4F6;">
                <div style="width:9px;height:9px;border-radius:50%;
                            background:{dot_color};flex-shrink:0;"></div>
                <div style="flex:1;font-size:0.82rem;color:#111827;
                            font-weight:500;white-space:nowrap;
                            overflow:hidden;text-overflow:ellipsis;">
                    {name}
                </div>
                <div style="font-size:0.75rem;color:{dot_color};
                            font-weight:600;white-space:nowrap;">
                    {stage}
                </div>
                <div style="font-size:0.70rem;color:#9CA3AF;
                            white-space:nowrap;">
                    {updated}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
