# ui/auth.py

import os
import streamlit as st
from ui.styles import inject_styles


def check_auth() -> bool:
    pwd = os.getenv("APP_PASSWORD", "")

    # No password configured → open access
    if not pwd:
        return True

    # Already authenticated
    if st.session_state.get("authenticated"):
        return True

    # ── Render login page ─────────────────────────────────────────────────────
    inject_styles()
    _render_login(pwd)
    return False


def _render_login(pwd: str) -> None:
    """Renders the full-page login UI."""

    # Hero splash (pure HTML — no widgets, safe to use markdown)
    st.markdown(
        """
        <div style="display:flex;flex-direction:column;align-items:center;
                    justify-content:center;padding-top:80px;padding-bottom:32px;">
          <div style="font-size:4rem;">🧠</div>
          <div style="font-size:2rem;font-weight:900;letter-spacing:3px;
               background:linear-gradient(135deg,#6C63FF,#43E8D8);
               -webkit-background-clip:text;-webkit-text-fill-color:transparent;">
            RecruitIQ
          </div>
          <div style="color:#8892b0;font-size:0.9rem;margin-top:6px;
                      letter-spacing:1px;">
            INTELLIGENT RECRUITMENT PLATFORM
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Centre column for the form
    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        # Card styling applied to the column container via CSS
        st.markdown(
            """
            <style>
            /* target the centre column and give it card styling */
            div[data-testid="column"]:nth-of-type(2) > div:first-child {
                background: #112240;
                border: 1px solid #1e3a5f;
                border-radius: 12px;
                padding: 28px !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            "<p style='color:#ccd6f6;font-weight:700;font-size:1.1rem;"
            "text-align:center;margin-bottom:8px;'>🔐 Sign In</p>",
            unsafe_allow_html=True,
        )

        with st.form("riq_login", clear_on_submit=False):
            password = st.text_input(
                "Access Password",
                type="password",
                placeholder="Enter your password",
            )
            submitted = st.form_submit_button(
                "Login to RecruitIQ →",
                use_container_width=True,
                type="primary",
            )

        # Handle submission OUTSIDE the form block
        # (inside also works, but this avoids re-render timing issues)
        if submitted:
            if password == pwd:
                st.session_state["authenticated"] = True
                st.session_state.setdefault("page", "dashboard")
                st.rerun()
            else:
                st.error("Incorrect password. Please try again.")
