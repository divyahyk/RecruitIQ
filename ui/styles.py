import streamlit as st


# ── Colour & font tokens ─────────────────────────────────────────────────────
PRIMARY      = "#4F46E5"   # indigo
PRIMARY_DARK = "#3730A3"
SUCCESS      = "#10B981"   # emerald
WARNING      = "#F59E0B"   # amber
DANGER       = "#EF4444"   # red
INFO         = "#3B82F6"   # blue
SURFACE      = "#FFFFFF"
BACKGROUND   = "#F8F9FB"
BORDER       = "#E5E7EB"
TEXT_MAIN    = "#111827"
TEXT_MUTED   = "#6B7280"

# Pipeline stage colours
STAGE_COLOURS: dict[str, str] = {
    "Applied":    "#6366F1",
    "Screening":  "#8B5CF6",
    "Interview":  "#3B82F6",
    "Assessment": "#F59E0B",
    "Offer":      "#10B981",
    "Hired":      "#059669",
    "Rejected":   "#EF4444",
    "Withdrawn":  "#9CA3AF",
}


# ── Global CSS injection ──────────────────────────────────────────────────────
def apply_global_styles() -> None:
    """Inject global CSS — call once at the top of app.py."""
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        html, body, [class*="css"] {{
            font-family: 'Inter', sans-serif;
            background-color: {BACKGROUND};
            color: {TEXT_MAIN};
        }}

        /* ── Sidebar ── */
        section[data-testid="stSidebar"] {{
            background: {SURFACE};
            border-right: 1px solid {BORDER};
        }}
        section[data-testid="stSidebar"] .stRadio label {{
            font-size: 0.9rem;
            font-weight: 500;
            padding: 0.4rem 0;
        }}

        /* ── Metric cards ── */
        div[data-testid="metric-container"] {{
            background: {SURFACE};
            border: 1px solid {BORDER};
            border-radius: 12px;
            padding: 1rem 1.25rem;
            box-shadow: 0 1px 3px rgba(0,0,0,.06);
        }}

        /* ── Buttons ── */
        .stButton > button {{
            background: {PRIMARY};
            color: #fff;
            border: none;
            border-radius: 8px;
            font-weight: 500;
            padding: 0.45rem 1.1rem;
            transition: background 0.2s;
        }}
        .stButton > button:hover {{
            background: {PRIMARY_DARK};
        }}

        /* ── Tabs ── */
        .stTabs [data-baseweb="tab-list"] {{
            gap: 4px;
            border-bottom: 2px solid {BORDER};
        }}
        .stTabs [data-baseweb="tab"] {{
            border-radius: 6px 6px 0 0;
            font-weight: 500;
            font-size: 0.875rem;
            padding: 0.5rem 1rem;
        }}
        .stTabs [aria-selected="true"] {{
            background: {SURFACE};
            color: {PRIMARY};
        }}

        /* ── Cards ── */
        .riq-card {{
            background: {SURFACE};
            border: 1px solid {BORDER};
            border-radius: 12px;
            padding: 1.25rem 1.5rem;
            box-shadow: 0 1px 4px rgba(0,0,0,.05);
            margin-bottom: 1rem;
        }}

        /* ── DataFrames ── */
        .stDataFrame {{ border-radius: 10px; overflow: hidden; }}

        /* ── Hide Streamlit branding ── */
        #MainMenu, footer {{ visibility: hidden; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ── Reusable components ───────────────────────────────────────────────────────

def page_header(title: str, subtitle: str = "", icon: str = "") -> None:
    """Render a consistent page title + optional subtitle."""
    icon_html = (
        f"<span style='margin-right:10px;font-size:1.6rem'>{icon}</span>"
        if icon else ""
    )
    sub_html = (
        f"<p style='color:{TEXT_MUTED};font-size:0.95rem;margin:0.2rem 0 0'>{subtitle}</p>"
        if subtitle else ""
    )
    st.markdown(
        f"""
        <div style='margin-bottom:1.5rem'>
            <h2 style='margin:0;font-size:1.6rem;font-weight:700;color:{TEXT_MAIN}'>
                {icon_html}{title}
            </h2>
            {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def stage_badge(stage: str) -> str:
    """Return an HTML badge string for a pipeline stage."""
    colour = STAGE_COLOURS.get(stage, "#9CA3AF")
    return (
        f"<span style='"
        f"background:{colour}20;"
        f"color:{colour};"
        f"border:1px solid {colour}60;"
        f"border-radius:999px;"
        f"padding:2px 10px;"
        f"font-size:0.75rem;"
        f"font-weight:600;"
        f"white-space:nowrap;"
        f"'>{stage}</span>"
    )


def score_badge(score: int | float, show_label: bool = True) -> str:
    """
    Return an HTML badge for a numeric match/fit score (0-100).

    Colour tiers
    ------------
    90-100  → emerald  (Excellent)
    75-89   → indigo   (Strong)
    60-74   → amber    (Good)
    40-59   → orange   (Fair)
    0-39    → red      (Weak)

    Usage
    -----
        st.markdown(score_badge(87), unsafe_allow_html=True)
    """
    score = int(score) if score is not None else 0
    score = max(0, min(100, score))  # clamp 0-100

    if score >= 90:
        colour = "#059669"
        label  = "Excellent"
    elif score >= 75:
        colour = PRIMARY
        label  = "Strong"
    elif score >= 60:
        colour = WARNING
        label  = "Good"
    elif score >= 40:
        colour = "#F97316"
        label  = "Fair"
    else:
        colour = DANGER
        label  = "Weak"

    label_html = (
        f"<span style='font-size:0.7rem;font-weight:500;"
        f"color:{colour};margin-left:5px'>{label}</span>"
        if show_label else ""
    )

    return (
        f"<span style='"
        f"display:inline-flex;align-items:center;"
        f"background:{colour}18;"
        f"color:{colour};"
        f"border:1px solid {colour}50;"
        f"border-radius:999px;"
        f"padding:3px 10px;"
        f"font-size:0.8rem;"
        f"font-weight:700;"
        f"white-space:nowrap;"
        f"'>{score}%{label_html}</span>"
    )


def skill_pills(skills: list[str], max_show: int = 8) -> str:
    """
    Return an HTML string of pill badges for a list of skills.

    Parameters
    ----------
    skills   : list of skill strings
    max_show : cap how many pills to render (remaining shown as +N more)

    Usage
    -----
        st.markdown(skill_pills(["Python", "SQL", "AWS"]), unsafe_allow_html=True)
    """
    if not skills:
        return f"<span style='color:{TEXT_MUTED};font-size:0.8rem'>—</span>"

    visible  = skills[:max_show]
    overflow = len(skills) - max_show
    pills_html = ""

    for skill in visible:
        pills_html += (
            f"<span style='"
            f"display:inline-block;"
            f"background:{PRIMARY}15;"
            f"color:{PRIMARY};"
            f"border:1px solid {PRIMARY}40;"
            f"border-radius:999px;"
            f"padding:2px 10px;"
            f"font-size:0.75rem;"
            f"font-weight:500;"
            f"margin:2px 3px 2px 0;"
            f"white-space:nowrap;"
            f"'>{skill}</span>"
        )

    if overflow > 0:
        pills_html += (
            f"<span style='"
            f"display:inline-block;"
            f"background:#F3F4F6;"
            f"color:{TEXT_MUTED};"
            f"border:1px solid {BORDER};"
            f"border-radius:999px;"
            f"padding:2px 10px;"
            f"font-size:0.75rem;"
            f"font-weight:500;"
            f"margin:2px 0;"
            f"'>+{overflow} more</span>"
        )

    return f"<div style='line-height:2'>{pills_html}</div>"


def kpi_card(label: str, value: str, delta: str = "", delta_positive: bool = True) -> str:
    """
    Return HTML for a standalone KPI card (use inside st.markdown).

    Parameters
    ----------
    label          : metric label
    value          : main value string
    delta          : optional change string e.g. "+12%"
    delta_positive : controls delta colour (green vs red)
    """
    delta_colour = SUCCESS if delta_positive else DANGER
    delta_html = (
        f"<span style='font-size:0.8rem;color:{delta_colour};font-weight:600'>{delta}</span>"
        if delta else ""
    )
    return f"""
    <div class='riq-card' style='text-align:center;padding:1rem'>
        <p style='margin:0;font-size:0.8rem;color:{TEXT_MUTED};font-weight:500;
                  text-transform:uppercase;letter-spacing:.05em'>{label}</p>
        <p style='margin:4px 0 2px;font-size:2rem;font-weight:700;
                  color:{TEXT_MAIN}'>{value}</p>
        {delta_html}
    </div>
    """


def empty_state(message: str, icon: str = "🔍") -> None:
    """Render a centred empty-state message."""
    st.markdown(
        f"""
        <div style='text-align:center;padding:3rem 1rem;color:{TEXT_MUTED}'>
            <div style='font-size:3rem;margin-bottom:0.75rem'>{icon}</div>
            <p style='font-size:1rem;font-weight:500'>{message}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_header(title: str, action_label: str = "") -> None:
    """
    Render a section divider with optional right-aligned action label text.
    (Wire up the actual button separately with st.button.)
    """
    st.markdown(
        f"""
        <div style='display:flex;justify-content:space-between;
                    align-items:center;margin:1.5rem 0 0.75rem'>
            <h4 style='margin:0;font-size:1rem;font-weight:600;
                       color:{TEXT_MAIN}'>{title}</h4>
            <span style='font-size:0.8rem;color:{PRIMARY};
                         font-weight:500'>{action_label}</span>
        </div>
        <hr style='margin:0 0 1rem;border:none;border-top:1px solid {BORDER}'/>
        """,
        unsafe_allow_html=True,
    )


# ── Aliases & __all__ ────────────────────────────────────────────────────────
inject_styles = apply_global_styles   # backwards-compat alias

__all__ = [
    # Functions
    "apply_global_styles",
    "inject_styles",
    "page_header",
    "stage_badge",
    "score_badge",
    "skill_pills",
    "kpi_card",
    "empty_state",
    "section_header",
    # Constants
    "PRIMARY",
    "PRIMARY_DARK",
    "SUCCESS",
    "WARNING",
    "DANGER",
    "INFO",
    "SURFACE",
    "BACKGROUND",
    "BORDER",
    "TEXT_MAIN",
    "TEXT_MUTED",
    "STAGE_COLOURS",
]
