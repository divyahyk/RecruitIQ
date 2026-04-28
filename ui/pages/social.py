# ui/pages/social.py (UPDATED v2.2 - JPG Logo Support)
"""
RecruitIQ – Social Media Post & Banner Generator
TERRAGIG Edition – JPG Logo + Branding
"""

from __future__ import annotations

import json
import io
from typing import Any, Dict, Optional
from datetime import datetime
from pathlib import Path

import streamlit as st
from PIL import Image

from ui.styles import page_header
from modules.ai_engine.prompt_templates import PromptTemplates


# ── TERRAGIG Branding Config ──────────────────────────────────────────────────
_COMPANY_NAME = "TERRAGIG"
_CONTACT_EMAIL = "balkis.begam@terragig.in"
_LOGO_PATH = Path("C:/TG/terragig_consulting_logo.jpg")  # JPG format

# Fallback logo path (if above not found)
_LOGO_FALLBACK_PATHS = [
    Path("assets/terragig_logo.jpg"),
    Path("assets/logo.jpg"),
    Path("terragig_logo.jpg"),
]

_PLATFORMS = ["LinkedIn", "Instagram", "Twitter", "WhatsApp"]
_STYLES = ["Professional", "Casual", "Question", "Announcement"]

_CHAR_LIMITS: Dict[str, int] = {
    "LinkedIn":  3000,
    "Instagram": 2200,
    "Twitter":   280,
    "WhatsApp":  1024,
}

# Platform colors + branding
_PLATFORM_CONFIG: Dict[str, Dict] = {
    "LinkedIn": {
        "icon": "💼",
        "gradient": "linear-gradient(135deg,#0077B5,#00a0dc)",
        "size": "1200×627 px",
        "cta": "Send Resume",
        "accent": "#0077B5",
    },
    "Instagram": {
        "icon": "📸",
        "gradient": "linear-gradient(135deg,#833ab4,#fd1d1d,#fcb045)",
        "size": "1080×1080 px",
        "cta": "Send Resume",
        "accent": "#E1306C",
    },
    "Twitter": {
        "icon": "🐦",
        "gradient": "linear-gradient(135deg,#1DA1F2,#0d8bd9)",
        "size": "1200×675 px",
        "cta": "Send Resume",
        "accent": "#1DA1F2",
    },
    "WhatsApp": {
        "icon": "💬",
        "gradient": "linear-gradient(135deg,#25D366,#128C7E)",
        "size": "800×800 px",
        "cta": "Send Resume",
        "accent": "#25D366",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
#  LOGO LOADING & CACHING
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def _load_logo(max_width: int = 200) -> Optional[Image.Image]:
    """
    Load TERRAGIG logo (JPG) with caching.
    Auto-resizes to max_width while preserving aspect ratio.
    
    Args:
        max_width: Max width in pixels (height scales proportionally)
    
    Returns:
        PIL Image or None if not found
    """
    
    # Try primary path
    paths_to_try = [_LOGO_PATH] + _LOGO_FALLBACK_PATHS
    
    for logo_path in paths_to_try:
        try:
            path = Path(logo_path)
            if path.exists() and path.suffix.lower() in {".jpg", ".jpeg"}:
                logo = Image.open(path)
                
                # Convert RGBA if needed (JPG often has no alpha)
                if logo.mode == "RGBA":
                    rgb_img = Image.new("RGB", logo.size, (255, 255, 255))
                    rgb_img.paste(logo, mask=logo.split()[3])
                    logo = rgb_img
                elif logo.mode != "RGB":
                    logo = logo.convert("RGB")
                
                # Resize maintaining aspect ratio
                if logo.width > max_width:
                    ratio = max_width / logo.width
                    new_height = int(logo.height * ratio)
                    logo = logo.resize((max_width, new_height), Image.Resampling.LANCZOS)
                
                return logo
        except Exception as e:
            continue
    
    return None


# ── Key namespacing ───────────────────────────────────────────────────────────
def _wkey(kind: str, platform: str, extra: str = "") -> str:
    """Widget key — used as key= in st.* calls."""
    suffix = f"_{extra}" if extra else ""
    return f"btn_{kind}_{platform}{suffix}"


def _dkey(kind: str, platform: str, extra: str = "") -> str:
    """Data key — used only for st.session_state reads/writes."""
    suffix = f"_{extra}" if extra else ""
    return f"dat_{kind}_{platform}{suffix}"


# ═════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def render_social_media(services: dict) -> None:
    """
    Main entry point for social media page.
    
    Args:
        services: Dict with keys:
            - "db": Database service (Supabase)
            - "llm": LLMHandler instance
            - "banner_gen": SocialBannerGenerator instance
    """
    
    page_header(
        "Social Media",
        f"Generate {_COMPANY_NAME} job posts and banners for all platforms",
        "📱",
    )

    # ── Service validation ────────────────────────────────────────────────────
    llm = services.get("llm")
    banner_gen = services.get("banner_gen")
    db = services.get("db")
    
    if not llm:
        st.error("❌ LLM service not initialized. Check your API keys in .env")
        return
    
    if not db:
        st.error("❌ Database service not initialized.")
        return
    
    # ── Logo status check ─────────────────────────────────────────────────────
    logo = _load_logo()
    if not logo:
        st.warning(
            f"⚠️ Logo not found at {_LOGO_PATH}. "
            f"Please place terragig_consulting_logo.jpg in that location."
        )
    
    # ── JD selector ───────────────────────────────────────────────────────────
    try:
        jds = db.get_all_jds(status="Active")
    except Exception as e:
        st.error(f"❌ Failed to fetch JDs: {e}")
        return
    
    if not jds:
        st.info("ℹ️ No active JDs. Create one in JD Manager first.")
        return

    jd_map = {
        f"{j.get('role_name', '?')}":  j  # Hide req_no from display
        for j in jds
    }
    sel_label = st.selectbox(
        "Select Job Description",
        list(jd_map.keys()),
        key="social_jd_select",
    )
    selected_jd = jd_map[sel_label]

    # ── Platform tabs ──────────────────────────────────────────────────────────
    tabs = st.tabs(["💼 LinkedIn", "📸 Instagram", "🐦 Twitter/X", "💬 WhatsApp"])

    for tab, platform in zip(tabs, _PLATFORMS):
        with tab:
            _render_platform_tab(
                selected_jd,
                platform,
                llm=llm,
                banner_gen=banner_gen,
                db=db,
                logo=logo,
            )


# ═════════════════════════════════════════════════════════════════════════════
#  PLATFORM TAB
# ═════════════════════════════════════════════════════════════════════════════

def _render_platform_tab(
    jd: dict,
    platform: str,
    llm: Any,
    banner_gen: Any,
    db: Any,
    logo: Optional[Image.Image] = None,
) -> None:
    """Render post + banner generation for a platform."""
    
    col_gen, col_prev = st.columns([1.2, 1])

    # ──────────────────────────────────────────────────────────────────────
    #  LEFT: POST GENERATOR
    # ──────────────────────────────────────────────────────────────────────
    with col_gen:
        st.markdown(
            f"<div class='riq-section-title'>Generate {platform} Post</div>",
            unsafe_allow_html=True,
        )

        # ── Style selector ────────────────────────────────────────────────────
        selected_style = st.selectbox(
            "Post Style",
            _STYLES,
            help="Choose tone: Professional, Casual, Question, or Announcement",
            key=_wkey("style", platform),
        )

        # ── Extra context ─────────────────────────────────────────────────────
        extra_info = st.text_area(
            "Additional context (optional)",
            height=80,
            placeholder="Any extra info for the post... e.g., 'Urgently hiring', 'Great benefits', etc.",
            key=_wkey("extra", platform),
        )

        # ── Generate button ───────────────────────────────────────────────────
        if st.button(
            f"🧠 Generate {platform} Post",
            type="primary",
            use_container_width=True,
            key=_wkey("gen", platform),
        ):
            _generate_social_post(
                jd=jd,
                platform=platform,
                style=selected_style,
                extra_context=extra_info,
                llm=llm,
                db=db,
            )

        # ── Display & edit generated post ─────────────────────────────────────
        post_text = st.session_state.get(_dkey("post", platform), "")
        if post_text:
            st.divider()
            
            edited = st.text_area(
                "📝 Edit post",
                value=post_text,
                height=180,
                key=_wkey("edit", platform),
            )

            # ── Character counter with color coding ──────────────────────────
            char_count = len(edited)
            limit = _CHAR_LIMITS.get(platform, 1000)
            
            if char_count <= limit:
                color = "#2ecc71"  # Green
                status = f"✅ {char_count}/{limit}"
            elif char_count <= limit * 1.1:
                color = "#f39c12"  # Orange
                status = f"⚠️ {char_count}/{limit}"
            else:
                color = "#e74c3c"  # Red
                status = f"❌ {char_count}/{limit}"
            
            st.markdown(
                f"<div style='color:{color};font-size:0.9rem;text-align:right;font-weight:600'>"
                f"{status} characters</div>",
                unsafe_allow_html=True,
            )

            # ── CTA: Send Resume / Contact Email ──────────────────────────────
            _render_contact_cta(platform)

            # ── Download button ───────────────────────────────────────────────
            st.download_button(
                "⬇️ Download Post",
                data=edited,
                file_name=f"{platform.lower()}_post_{jd.get('role_name', 'post').lower().replace(' ', '_')}.txt",
                use_container_width=True,
                key=_wkey("dl", platform),
            )
            
            # ── Save to history (optional) ────────────────────────────────────
            if st.button(
                "💾 Save to History",
                key=_wkey("save", platform),
                use_container_width=True,
            ):
                if db and jd.get("id"):
                    try:
                        post_id = db.save_social_post(
                            jd_id=jd.get("id"),
                            platform=platform,
                            content=edited,
                            style=selected_style,
                            generated_by=st.session_state.get("user_email", "RecruitIQ"),
                        )
                        if post_id:
                            st.success(f"✅ Saved to history ({post_id})")
                        else:
                            st.warning("⚠️ Failed to save (check logs)")
                    except Exception as e:
                        st.error(f"❌ Save error: {e}")
                else:
                    st.warning("⚠️ Cannot save without active JD")

    # ──────────────────────────────────────────────────────────────────────
    #  RIGHT: BANNER GENERATOR
    # ──────────────────────────────────────────────────────────────────────
    with col_prev:
        st.markdown(
            "<div class='riq-section-title'>Banner Preview</div>",
            unsafe_allow_html=True,
        )

        if banner_gen is None:
            st.caption("🎨 Banner generator not configured.")
            _platform_mockup(platform, jd, logo)
        else:
            # ── Generate banner button ────────────────────────────────────────
            if st.button(
                "🎨 Generate Banner",
                use_container_width=True,
                key=_wkey("banner", platform),
            ):
                _generate_banner(banner_gen, db, jd, platform, logo)

            # ── Display banner preview ────────────────────────────────────────
            _render_banner_preview(platform, jd, db, logo)
        
        # ── Recent posts ──────────────────────────────────────────────────────
        st.divider()
        if st.button(
            "📋 View History",
            key=_wkey("history", platform),
            use_container_width=True,
        ):
            _show_post_history(db, jd, platform)


# ─────────────────────────────────────────────────────────────────────────────
#  CONTACT CTA
# ─────────────────────────────────────────────────────────────────────────────

def _render_contact_cta(platform: str) -> None:
    """Render contact info box for sending resumes."""
    
    cfg = _PLATFORM_CONFIG.get(platform, {})
    accent = cfg.get("accent", "#0077B5")
    
    st.markdown(
        f"""
        <div style='
            background:linear-gradient(135deg, {accent}22, {accent}11);
            border-left:4px solid {accent};
            padding:12px 16px;
            border-radius:8px;
            margin:12px 0;
        '>
          <div style='font-weight:600;color:{accent};font-size:0.95rem'>
            📨 Send your resume to {_COMPANY_NAME}
          </div>
          <div style='
            font-size:0.85rem;
            color:#666;
            margin-top:4px;
            font-family:monospace;
          '>
            <a href='mailto:{_CONTACT_EMAIL}?subject=Resume%20-%20{platform}%20Application' 
               style='color:{accent};text-decoration:none;font-weight:600'>
              {_CONTACT_EMAIL}
            </a>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  POST GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def _generate_social_post(
    jd: dict,
    platform: str,
    style: str,
    extra_context: str,
    llm: Any,
    db: Any,
) -> None:
    """Generate social media post using LLMHandler."""
    
    data_key = _dkey("post", platform)
    
    try:
        # Build prompt with PromptTemplates
        # Include TERRAGIG branding + contact info
        prompt = PromptTemplates.social_post(
            jd=jd,
            platform=platform,
            style=style,
            extra_context=extra_context,
            company_name=_COMPANY_NAME,
            contact_email=_CONTACT_EMAIL,
        )
        
        # Call LLMHandler.complete() for text generation
        result = llm.complete(
            prompt=prompt,
            max_tokens=800,
            temperature=0.6,
        )
        
        # Extract text from result
        if isinstance(result, dict):
            post_text = result.get("text", "").strip()
        else:
            post_text = str(result).strip()
        
        # Clean up markdown artifacts if present
        if post_text.startswith("```"):
            lines = post_text.split("```")
            if len(lines) >= 2:
                post_text = lines[1]
                post_text = post_text.lstrip("json\ntext\nmarkdown\n").strip()
        
        if not post_text:
            st.error("❌ LLM returned empty response. Try again.")
            return
        
        # Store in session state
        st.session_state[data_key] = post_text
        st.session_state[_dkey("style", platform)] = style
        st.session_state[_dkey("timestamp", platform)] = datetime.utcnow().isoformat()
        
        # Auto-save to DB if available
        if db and jd.get("id"):
            try:
                db.save_social_post(
                    jd_id=jd.get("id"),
                    platform=platform,
                    content=post_text,
                    style=style,
                    generated_by=st.session_state.get("user_email", "RecruitIQ"),
                )
                st.success(f"✅ Generated & saved ({len(post_text)} chars)")
            except Exception as e:
                st.success(f"✅ Generated ({len(post_text)} chars)")
                st.warning(f"⚠️ Could not save to DB: {e}")
        else:
            st.success(f"✅ Generated ({len(post_text)} chars)")

    except Exception as exc:
        st.error(f"❌ Generation failed: {type(exc).__name__}")
        st.error(str(exc))
        with st.expander("🔧 Details"):
            st.exception(exc)


# ─────────────────────────────────────────────────────────────────────────────
#  BANNER GENERATION & DISPLAY
# ─────────────────────────────────────────────────────────────────────────────

def _generate_banner(
    banner_gen: Any,
    db: Any,
    jd: dict,
    platform: str,
    logo: Optional[Image.Image] = None,
) -> None:
    """Generate banner and store in session state + optional DB."""
    
    data_key = _dkey("banner", platform)
    
    try:
        # Generate PNG bytes via SocialBannerGenerator.generate()
        # Pass logo to banner generator
        result = banner_gen.generate(
            jd, 
            platform, 
            company_name=_COMPANY_NAME,
            logo=logo,
        )

        # Normalize to bytes
        if isinstance(result, io.BytesIO):
            result = result.getvalue()

        if not isinstance(result, (bytes, bytearray)):
            st.warning(f"❌ Unexpected banner type: {type(result).__name__}")
            return

        if len(result) < 100:
            st.warning("❌ Banner generator returned empty data.")
            return

        # Store in session state
        st.session_state[data_key] = bytes(result)
        st.success(f"✅ Banner created ({len(result)/1024:.1f}KB)")

        # Save to DB if available
        if db and jd.get("id"):
            try:
                banner_url = db.get_banner_url(
                    jd_id=jd.get("id"),
                    platform=platform,
                    fallback_image_bytes=result,
                )
                if banner_url:
                    st.info(f"📤 Uploaded to: {banner_url[:50]}...")
            except Exception as e:
                st.warning(f"⚠️ Could not save banner to DB: {e}")

    except NotImplementedError:
        st.info("ℹ️ Banner generation not yet implemented for this platform.")
    except Exception as exc:
        st.error(f"❌ Banner error: {type(exc).__name__}")
        st.error(str(exc))
        with st.expander("🔧 Details"):
            st.exception(exc)


def _render_banner_preview(
    platform: str, 
    jd: dict, 
    db: Any,
    logo: Optional[Image.Image] = None,
) -> None:
    """Display banner or fallback mockup."""
    
    data_key = _dkey("banner", platform)
    banner_bytes = st.session_state.get(data_key)

    # Try to get from DB cache if not in session
    if not banner_bytes and db and jd.get("id"):
        try:
            cached = db.get_banner(jd.get("id"), platform)
            if cached and cached.get("image_url"):
                st.image(cached.get("image_url"), use_container_width=True)
                st.caption(
                    f"📦 {cached.get('width', 1200)}×{cached.get('height', 628)} "
                    f"| Cached: {cached.get('created_at', 'Unknown')[:10]}"
                )
                return
        except Exception:
            pass

    # Display session banner or mockup
    if banner_bytes and isinstance(banner_bytes, (bytes, bytearray)):
        st.image(banner_bytes, use_container_width=True)
        
        col_dl, col_info = st.columns([3, 1])
        with col_dl:
            st.download_button(
                "⬇️ Download Banner",
                data=banner_bytes,
                file_name=f"recruitiq_{platform.lower()}_{jd.get('role_name', 'banner').lower().replace(' ', '_')}.png",
                mime="image/png",
                use_container_width=True,
                key=_wkey("dlbanner", platform),
            )
        with col_info:
            size_kb = len(banner_bytes) / 1024
            st.caption(f"📦 {size_kb:.1f}KB")
    else:
        _platform_mockup(platform, jd, logo)


def _platform_mockup(
    platform: str, 
    jd: dict,
    logo: Optional[Image.Image] = None,
) -> None:
    """
    Display HTML mockup when no real banner exists (WITH TERRAGIG BRANDING).
    Can include logo image if available.
    """
    
    cfg = _PLATFORM_CONFIG.get(platform, {})
    grad = cfg.get("gradient", "linear-gradient(135deg,#6C63FF,#43E8D8)")
    icon = cfg.get("icon", "📱")
    size = cfg.get("size", "1200×630 px")
    cta_text = cfg.get("cta", "Send Resume")
    accent = cfg.get("accent", "#0077B5")

    # Parse skills
    req_skills = jd.get("skillset_required") or []
    if isinstance(req_skills, str):
        try:
            req_skills = json.loads(req_skills)
        except Exception:
            req_skills = []
    
    skills_str = " • ".join(str(s) for s in req_skills[:3])  # Top 3 skills

    st.markdown(
        f"""
        <div style='
            background:{grad};
            border-radius:12px;
            padding:28px;
            text-align:center;
            min-height:240px;
            display:flex;
            flex-direction:column;
            justify-content:space-between;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        '>
          <!-- TOP: Logo/Company -->
          <div style='font-size:0.85rem;color:rgba(255,255,255,.6);font-weight:600;letter-spacing:1px'>
            {_COMPANY_NAME}
          </div>
          
          <!-- MIDDLE: Role Info -->
          <div>
            <div style='font-size:2.5rem;margin-bottom:12px'>{icon}</div>
            <div style='color:#fff;font-weight:800;font-size:1.3rem;margin:8px 0'>
              WE'RE HIRING
            </div>
            <div style='color:rgba(255,255,255,.95);font-size:1rem;font-weight:700;margin:8px 0'>
              {jd.get("role_name", "Open Position")}
            </div>
            <div style='color:rgba(255,255,255,.85);font-size:0.85rem;font-weight:500;margin:8px 0'>
              {jd.get("location", "Remote")} &nbsp;•&nbsp; {jd.get("work_mode", "Hybrid")}
            </div>
            {f"<div style='color:rgba(255,255,255,.75);font-size:0.8rem;margin-top:8px;font-style:italic'>{skills_str}</div>" if skills_str else ""}
          </div>
          
          <!-- BOTTOM: CTA + Footer -->
          <div>
            <div style='
              background:rgba(255,255,255,.2);
              border:2px solid rgba(255,255,255,.4);
              color:#fff;
              padding:10px 16px;
              border-radius:6px;
              font-weight:700;
              font-size:0.9rem;
              margin:12px 0;
            '>
              {cta_text} 📨
            </div>
            <div style='color:rgba(255,255,255,.6);font-size:0.7rem;font-weight:500'>
              {size} &nbsp;•&nbsp; RecruitIQ
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    # If logo available, show it below mockup
    if logo:
        st.divider()
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image(logo, caption=f"{_COMPANY_NAME} Logo", use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
#  POST HISTORY
# ─────────────────────────────────────────────────────────────────────────────

def _show_post_history(db: Any, jd: dict, platform: str) -> None:
    """Display recent social posts for this JD + platform."""
    
    if not db or not jd.get("id"):
        st.warning("⚠️ Cannot fetch history without active JD.")
        return
    
    try:
        posts = db.get_social_posts(
            jd_id=jd.get("id"),
            platform=platform,
            limit=10,
        )
        
        if not posts:
            st.info(f"ℹ️ No {platform} posts yet.")
            return
        
        st.subheader(f"Recent {platform} Posts")
        
        for i, post in enumerate(posts, 1):
            with st.expander(
                f"#{i} • {post.get('style', 'Unknown')} "
                f"• {post.get('created_at', 'Unknown')[:10]}",
                expanded=(i == 1),
            ):
                st.markdown("**Content:**")
                st.text(post.get("content", "(empty)"))
                
                col_copy, col_use = st.columns(2)
                with col_copy:
                    st.button(
                        "📋 Copy",
                        on_click=lambda: st.write(post.get("content")),
                        key=f"copy_{post.get('id')}",
                        use_container_width=True,
                    )
                with col_use:
                    if st.button(
                        "♻️ Use This",
                        key=f"use_{post.get('id')}",
                        use_container_width=True,
                    ):
                        st.session_state[_dkey("post", platform)] = post.get("content")
                        st.session_state[_dkey("style", platform)] = post.get("style")
                        st.rerun()
    
    except Exception as e:
        st.error(f"❌ Could not load history: {e}")
