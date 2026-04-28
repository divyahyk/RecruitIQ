# modules/social_media/banner_generator.py
"""
RecruitIQ – High-Quality Social Media Banner Generator
Produces platform-native PNG images using Pillow.
Integrated with JD Parser and Post Generator.
Returns bytes — compatible with st.image() and st.download_button().
"""

from __future__ import annotations

import io
import json
import math
from typing import Any, Optional
from enum import Enum

from PIL import Image, ImageDraw, ImageFont


# ─────────────────────────────────────────────────────────────────────────────
#  PLATFORM SPECS
# ─────────────────────────────────────────────────────────────────────────────

class Platform(Enum):
    """Supported social media platforms."""
    LINKEDIN   = "linkedin"
    INSTAGRAM  = "instagram"
    TWITTER    = "twitter"
    WHATSAPP   = "whatsapp"
    FACEBOOK   = "facebook"  # NEW


_SPECS: dict[str, dict] = {
    "linkedin": {
        "size":    (1200, 628),
        "palette": {
            "bg_top":     (10,  20,  60),
            "bg_bottom":  (6,   12,  40),
            "accent1":    (108, 99,  255),
            "accent2":    (67,  232, 216),
            "accent3":    (0,   119, 181),
            "card":       (20,  30,  80),
            "text_h":     (255, 255, 255),
            "text_b":     (200, 210, 240),
            "text_muted": (140, 150, 190),
            "pill_bg":    (30,  45,  120),
            "pill_text":  (67,  232, 216),
            "cta_bg":     (108, 99,  255),
            "cta_text":   (255, 255, 255),
        },
    },
    "instagram": {
        "size":    (1080, 1080),
        "palette": {
            "bg_top":     (20,   8,  50),
            "bg_bottom":  (80,   8,  60),
            "accent1":    (253,  29,  29),
            "accent2":    (252, 176,  69),
            "accent3":    (131,  58, 180),
            "card":       (35,   10,  60),
            "text_h":     (255, 255, 255),
            "text_b":     (240, 220, 255),
            "text_muted": (180, 150, 220),
            "pill_bg":    (60,   15,  90),
            "pill_text":  (252, 176,  69),
            "cta_bg":     (253,  29,  29),
            "cta_text":   (255, 255, 255),
        },
    },
    "twitter": {
        "size":    (1200, 675),
        "palette": {
            "bg_top":     (5,   15,  30),
            "bg_bottom":  (10,  30,  55),
            "accent1":    (29,  161, 242),
            "accent2":    (0,   200, 200),
            "accent3":    (100, 200, 255),
            "card":       (15,  30,  60),
            "text_h":     (255, 255, 255),
            "text_b":     (200, 225, 255),
            "text_muted": (130, 160, 200),
            "pill_bg":    (20,  50,  100),
            "pill_text":  (0,   200, 200),
            "cta_bg":     (29,  161, 242),
            "cta_text":   (255, 255, 255),
        },
    },
    "whatsapp": {
        "size":    (800, 800),
        "palette": {
            "bg_top":     (5,   30,  20),
            "bg_bottom":  (2,   15,  10),
            "accent1":    (37,  211, 102),
            "accent2":    (18,  140, 126),
            "accent3":    (100, 240, 160),
            "card":       (10,  45,  30),
            "text_h":     (255, 255, 255),
            "text_b":     (200, 245, 220),
            "text_muted": (130, 200, 160),
            "pill_bg":    (15,  60,  40),
            "pill_text":  (37,  211, 102),
            "cta_bg":     (37,  211, 102),
            "cta_text":   (5,   30,  20),
        },
    },
    "facebook": {
        "size":    (1200, 628),
        "palette": {
            "bg_top":     (23,  119, 242),
            "bg_bottom":  (11,  85,  204),
            "accent1":    (0,   170, 255),
            "accent2":    (255, 193, 7),
            "accent3":    (76,  175, 80),
            "card":       (30,  60,  120),
            "text_h":     (255, 255, 255),
            "text_b":     (220, 230, 255),
            "text_muted": (150, 170, 210),
            "pill_bg":    (40,  80,  160),
            "pill_text":  (255, 193, 7),
            "cta_bg":     (0,   170, 255),
            "cta_text":   (255, 255, 255),
        },
    },
}
_SPECS["default"] = _SPECS["linkedin"]


# ─────────────────────────────────────────────────────────────────────────────
#  SAFE BOX HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _box(x0: float, y0: float, x1: float, y1: float) -> tuple:
    """
    Return a Pillow-safe bounding box tuple.
    Ensures x1 >= x0 and y1 >= y0 by enforcing a minimum 1-pixel size.
    """
    x0, y0, x1, y1 = int(x0), int(y0), int(x1), int(y1)
    if x1 <= x0:
        x1 = x0 + 1
    if y1 <= y0:
        y1 = y0 + 1
    return (x0, y0, x1, y1)


# ─────────────────────────────────────────────────────────────────────────────
#  FONT LOADER
# ─────────────────────────────────────────────────────────────────────────────

def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Try common system fonts; always falls back to Pillow built-in."""
    size = max(8, int(size))
    candidates = (
        [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "C:/Windows/Fonts/arialbd.ttf",
        ]
        if bold else
        [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "C:/Windows/Fonts/arial.ttf",
        ]
    )
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except (OSError, IOError):
            continue
    return ImageFont.load_default()


# ─────────────────────────────────────────────────────────────────────────────
#  DRAWING PRIMITIVES
# ─────────────────────────────────────────────────────────────────────────────

def _rgba(rgb: tuple, alpha: int = 255) -> tuple:
    """Return RGBA tuple."""
    return (*rgb[:3], max(0, min(255, alpha)))


def _lerp_color(c1: tuple, c2: tuple, t: float) -> tuple:
    """Linear interpolation between two colors."""
    return tuple(int(c1[i] + (c2[i] - c1[i]) * t) for i in range(3))


def _gradient_bg(
    draw: ImageDraw.ImageDraw,
    w: int, h: int,
    top: tuple,
    bottom: tuple,
) -> None:
    """Draw a gradient background from top to bottom."""
    for y in range(h):
        draw.line([(0, y), (w, y)], fill=_lerp_color(top, bottom, y / h))


def _noise_overlay(img: Image.Image, intensity: int = 8) -> None:
    """Add subtle noise for texture."""
    import random
    rng  = random.Random(42)
    pix  = img.load()
    w, h = img.size
    for y in range(0, h, 2):
        for x in range(0, w, 2):
            n        = rng.randint(-intensity, intensity)
            r, g, b, a = pix[x, y]
            pix[x, y] = (
                max(0, min(255, r + n)),
                max(0, min(255, g + n)),
                max(0, min(255, b + n)),
                a,
            )


def _rounded_rect(
    draw:          ImageDraw.ImageDraw,
    box:           tuple,
    radius:        int,
    fill:          tuple,
    outline:       Optional[tuple] = None,
    outline_width: int = 1,
) -> None:
    """Draw a rounded rectangle safely."""
    x0, y0, x1, y1 = _box(*box)
    max_r  = max(1, min((x1 - x0) // 2, (y1 - y0) // 2))
    radius = max(1, min(radius, max_r))
    draw.rounded_rectangle(
        (x0, y0, x1, y1),
        radius=radius,
        fill=fill,
        outline=outline,
        width=outline_width,
    )


def _draw_circle(
    draw: ImageDraw.ImageDraw,
    cx: int, cy: int, r: int,
    fill: tuple,
) -> None:
    """Draw a circle."""
    r = max(1, r)
    draw.ellipse(_box(cx - r, cy - r, cx + r, cy + r), fill=fill)


def _text_block(
    draw:   ImageDraw.ImageDraw,
    text:   str,
    x: int, y: int,
    font:   ImageFont.FreeTypeFont,
    fill:   tuple,
    max_w:  int,
    line_h: int = 0,
) -> int:
    """Word-wrap text; return y after last line."""
    words   = text.split()
    lines:  list[str] = []
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        try:
            tw = draw.textlength(test, font=font)
        except Exception:
            tw = len(test) * (font.size // 2)
        if tw <= max_w:
            current = test
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)

    lh = line_h or int(font.size * 1.35)
    for line in lines:
        draw.text((x, y), line, font=font, fill=fill)
        y += lh
    return y


def _pill(
    draw:    ImageDraw.ImageDraw,
    text:    str,
    x: int, y: int,
    font:    ImageFont.FreeTypeFont,
    bg:      tuple,
    fg:      tuple,
    h:       int  = 34,
    pad_x:   int  = 18,
    border:  Optional[tuple] = None,
) -> int:
    """Draw a pill tag; return right-edge x."""
    h     = max(16, h)
    try:
        tw = int(draw.textlength(text, font=font))
    except Exception:
        tw = len(text) * (font.size // 2)
    w     = tw + pad_x * 2
    _rounded_rect(
        draw,
        _box(x, y, x + w, y + h),
        radius=h // 2,
        fill=bg,
        outline=border or bg,
        outline_width=1,
    )
    ty = y + max(0, (h - int(font.size)) // 2) - 1
    draw.text((x + pad_x, ty), text, font=font, fill=fg)
    return x + w


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN CLASS (ENHANCED)
# ─────────────────────────────────────────────────────────────────────────────

class SocialBannerGenerator:
    """
    Generates platform-native PNG recruitment banners.
    
    Enhanced to work with:
    - JD data (parsed or raw dict)
    - Multiple platforms (LinkedIn, Instagram, Twitter, WhatsApp, Facebook)
    - LLM integration (optional, for smart text generation)
    - Streamlit UI (image display + download)

    Usage:
        gen = SocialBannerGenerator(llm=llm_handler)
        
        # From parsed JD
        png_bytes = gen.generate(jd_dict, "LinkedIn")
        
        # From raw JD text
        png_bytes = gen.generate_from_text(jd_text, "Instagram")
        
        # Generate all platforms at once
        all_banners = gen.generate_all(jd_dict)
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize banner generator.
        
        Args:
            llm: Optional LLM handler for smart text generation
            jd_parser: Optional JD parser for raw text input
        """
        self.llm: Any = kwargs.get("llm", None)
        self.jd_parser: Any = kwargs.get("jd_parser", None)

    # ── PUBLIC API ────────────────────────────────────────────────────────────

    def generate(self, jd: dict, platform: str) -> bytes:
        """
        Render a banner PNG for *platform* from JD data.
        
        Args:
            jd: Parsed JD dictionary with keys like:
                - role_name or title
                - client_name or company
                - location
                - work_mode
                - experience_min, experience_max
                - skillset_required (list or JSON string)
                - jd_code (optional)
            platform: "LinkedIn", "Instagram", "Twitter", "WhatsApp", or "Facebook"
        
        Returns:
            bytes: PNG image data (always bytes, never None)
        """
        platform_key = platform.lower().strip()
        spec         = _SPECS.get(platform_key, _SPECS["default"])
        w, h         = spec["size"]
        pal          = spec["palette"]

        # ── Parse JD fields ───────────────────────────────────────
        role      = str(jd.get("role_name") or jd.get("title") or "Open Position")
        client    = str(jd.get("client_name") or jd.get("company") or "")
        location  = str(jd.get("location") or "Remote")
        work_mode = str(jd.get("work_mode") or "Hybrid")
        exp_min   = jd.get("experience_min") or 0
        exp_max   = jd.get("experience_max") or 0
        jd_code   = str(jd.get("jd_code") or "")

        skills_raw = jd.get("skillset_required") or jd.get("skills") or []
        if isinstance(skills_raw, str):
            try:
                skills_raw = json.loads(skills_raw)
            except Exception:
                skills_raw = []
        skills: list[str] = [str(s).strip() for s in skills_raw if s][:8]

        # ── Build canvas ──────────────────────────────────────────
        img  = Image.new("RGBA", (w, h), (0, 0, 0, 255))
        draw = ImageDraw.Draw(img)

        # ── Render layers (back → front) ──────────────────────────
        _Layers.gradient(draw, w, h, pal)
        _Layers.geometry(draw, w, h, pal)
        _noise_overlay(img, intensity=6)
        draw = ImageDraw.Draw(img)

        _Layers.card(draw, w, h, pal)
        _Layers.header(draw, w, h, pal, platform_key, client, jd_code)
        _Layers.role(draw, w, h, pal, role)
        _Layers.meta(draw, w, h, pal, location, work_mode, exp_min, exp_max)
        _Layers.skills(draw, w, h, pal, skills)
        _Layers.cta(draw, w, h, pal, platform_key)
        _Layers.footer(draw, w, h, pal, jd_code)
        img = _Layers.vignette(img, w, h)

        # ── Encode ────────────────────────────────────────────────
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="PNG")
        return buf.getvalue()

    def generate_from_text(self, jd_text: str, platform: str) -> bytes:
        """
        Generate banner from raw JD text.
        Uses jd_parser if available, else fallback to basic extraction.
        """
        if self.jd_parser:
            jd_data = self.jd_parser.parse(jd_text)
        else:
            # Minimal fallback parsing
            jd_data = self._simple_parse(jd_text)
        
        return self.generate(jd_data, platform)

    def generate_all(self, jd: dict) -> dict[str, bytes]:
        """
        Generate banners for ALL supported platforms.
        
        Returns:
            {
                "LinkedIn": <PNG bytes>,
                "Instagram": <PNG bytes>,
                "Twitter": <PNG bytes>,
                "WhatsApp": <PNG bytes>,
                "Facebook": <PNG bytes>,
            }
        """
        platforms = [p.value for p in Platform]
        return {
            p.replace("_", " ").title(): self.generate(jd, p)
            for p in platforms
        }

    def platforms(self) -> list[str]:
        """Return list of supported platform names."""
        return ["LinkedIn", "Instagram", "Twitter", "WhatsApp", "Facebook"]

    def get_spec(self, platform: str) -> dict:
        """Get platform specs (size, colors, etc.)."""
        key = platform.lower().strip()
        return _SPECS.get(key, _SPECS["default"])

    # ── PRIVATE HELPERS ───────────────────────────────────────────────────────

    def _simple_parse(self, text: str) -> dict:
        """Minimal JD parsing fallback."""
        lines = text.split('\n')
        title = lines[0] if lines else "Position"
        
        return {
            "role_name": title,
            "client_name": "Our Company",
            "location": "Remote",
            "work_mode": "Hybrid",
            "experience_min": 3,
            "experience_max": 7,
            "skills": ["Python", "AI", "SQL"],
        }


# ─────────────────────────────────────────────────────────────────────────────
#  LAYER RENDERERS (UNCHANGED - Full Implementation)
# ─────────────────────────────────────────────────────────────────────────────

class _Layers:
    """Static layer renderers."""

    @staticmethod
    def gradient(draw, w, h, pal) -> None:
        _gradient_bg(draw, w, h, pal["bg_top"], pal["bg_bottom"])

    @staticmethod
    def geometry(draw, w, h, pal) -> None:
        a1, a2 = pal["accent1"], pal["accent2"]
        r_big  = max(1, int(min(w, h) * 0.28))
        r_med  = max(1, int(min(w, h) * 0.18))
        r_sml  = max(1, int(min(w, h) * 0.22))
        r_xs   = max(1, int(min(w, h) * 0.12))

        _draw_circle(draw, int(w * 0.88), int(h * 0.12), r_big, _rgba(a1, 35))
        _draw_circle(draw, int(w * 0.88), int(h * 0.12), r_med, _rgba(a1, 55))
        _draw_circle(draw, int(w * 0.08), int(h * 0.88), r_sml, _rgba(a2, 30))
        _draw_circle(draw, int(w * 0.08), int(h * 0.88), r_xs,  _rgba(a2, 50))

        bar = max(2, int(h * 0.012))
        draw.rectangle(_box(0, 0, w, bar), fill=_rgba(a1, 200))
        draw.rectangle(_box(0, h - bar - 1, w, h), fill=_rgba(a2, 120))

        dot_col = _rgba(a1, 45)
        gx_start = int(w * 0.62)
        gx_end   = int(w * 0.96)
        gy_start = int(h * 0.06)
        gy_end   = int(h * 0.46)
        step     = max(18, int(min(w, h) * 0.028))
        for gx in range(gx_start, gx_end, step):
            for gy in range(gy_start, gy_end, step):
                draw.ellipse(_box(gx - 2, gy - 2, gx + 3, gy + 3), fill=dot_col)

    @staticmethod
    def card(draw, w, h, pal) -> None:
        pad  = int(w * 0.05)
        y0   = int(h * 0.08)
        y1   = h - int(h * 0.08)
        _rounded_rect(
            draw,
            _box(pad, y0, w - pad, y1),
            radius=24,
            fill=_rgba(pal["card"], 160),
            outline=_rgba(pal["accent1"], 60),
            outline_width=1,
        )

    @staticmethod
    def header(draw, w, h, pal, platform_key, client, jd_code) -> None:
        pad    = int(w * 0.08)
        y_top  = int(h * 0.13)
        fsmall = _font(max(8, int(h * 0.022)))
        fbadge = _font(max(8, int(h * 0.020)), bold=True)

        labels = {
            "linkedin":  "LinkedIn",
            "instagram": "Instagram",
            "twitter":   "Twitter / X",
            "whatsapp":  "WhatsApp",
            "facebook":  "Facebook",
        }
        label = labels.get(platform_key, "Social")
        pill_h = max(16, int(h * 0.05))
        _pill(draw, label, pad, y_top,
              fbadge,
              _rgba(pal["accent1"], 80),
              pal["accent2"],
              h=pill_h, pad_x=14,
              border=_rgba(pal["accent1"], 140))

        if client:
            try:
                tw = int(draw.textlength(f"  {client}", font=fsmall))
            except Exception:
                tw = len(client) * 8
            draw.text(
                (w - pad - tw, y_top + 6),
                f"  {client}", font=fsmall, fill=pal["text_muted"],
            )

        if jd_code:
            draw.text(
                (pad, y_top + pill_h + int(h * 0.018)),
                jd_code, font=fsmall,
                fill=_rgba(pal["text_muted"], 160),
            )

    @staticmethod
    def role(draw, w, h, pal, role) -> None:
        pad     = int(w * 0.08)
        y_label = int(h * 0.28)
        f_label = _font(max(8, int(h * 0.025)), bold=True)
        f_role  = _font(max(8, int(h * 0.072)), bold=True)

        draw.text(
            (pad, y_label),
            "WE'RE HIRING",
            font=f_label,
            fill=_rgba(pal["accent2"], 210),
        )

        _text_block(
            draw,
            role.upper(),
            pad,
            y_label + int(h * 0.045),
            f_role,
            pal["text_h"],
            max_w=int(w * 0.72),
            line_h=int(h * 0.082),
        )

    @staticmethod
    def meta(draw, w, h, pal, location, work_mode, exp_min, exp_max) -> None:
        pad    = int(w * 0.08)
        y      = int(h * 0.555)
        f_meta = _font(max(8, int(h * 0.028)))

        sep_y = max(y - 10, 0)
        draw.rectangle(
            _box(pad, sep_y, int(w * 0.55), sep_y + 2),
            fill=_rgba(pal["accent1"], 80),
        )

        items = [f"  {location}", f"  {work_mode}"]
        if exp_min or exp_max:
            items.append(f"  {exp_min}-{exp_max} yrs")

        x = pad
        for item in items:
            draw.text((x, y), item, font=f_meta, fill=pal["text_b"])
            try:
                x += int(draw.textlength(item, font=f_meta)) + int(w * 0.032)
            except Exception:
                x += len(item) * 9 + int(w * 0.032)

    @staticmethod
    def skills(draw, w, h, pal, skills) -> None:
        if not skills:
            return

        pad      = int(w * 0.08)
        y        = int(h * 0.645)
        f_pill   = _font(max(8, int(h * 0.024)), bold=True)
        pill_h   = max(16, int(h * 0.055))
        pill_gap = max(6, int(w * 0.014))
        max_x    = int(w * 0.88)
        max_rows = 2
        row      = 0
        x        = pad

        for skill in skills:
            if not skill:
                continue
            try:
                tw = int(draw.textlength(skill, font=f_pill))
            except Exception:
                tw = len(skill) * (f_pill.size // 2)

            pill_w = tw + 28
            if x + pill_w > max_x:
                row += 1
                if row >= max_rows:
                    break
                x  = pad
                y += pill_h + max(4, int(h * 0.018))

            x = _pill(
                draw, skill, x, y, f_pill,
                _rgba(pal["pill_bg"], 200),
                pal["pill_text"],
                h=pill_h, pad_x=14,
                border=_rgba(pal["pill_text"], 70),
            )
            x += pill_gap

    @staticmethod
    def cta(draw, w, h, pal, platform_key) -> None:
        ctas = {
            "linkedin":  "Apply on LinkedIn  →",
            "instagram": "Link in Bio  →",
            "twitter":   "Apply Now  →",
            "whatsapp":  "Send Resume  →",
            "facebook":  "Apply Now  →",
        }
        text  = ctas.get(platform_key, "Apply Now  →")
        f_cta = _font(max(8, int(h * 0.030)), bold=True)
        pad   = int(w * 0.08)
        btn_h = max(24, int(h * 0.072))

        try:
            tw = int(draw.textlength(text, font=f_cta))
        except Exception:
            tw = len(text) * (f_cta.size // 2)

        btn_w = tw + int(w * 0.07)
        y_btn = int(h * 0.80)

        y_btn = min(y_btn, h - btn_h - int(h * 0.04))
        y_btn = max(0, y_btn)

        _rounded_rect(
            draw,
            _box(pad + 3, y_btn + 4, pad + btn_w + 3, y_btn + btn_h + 4),
            radius=btn_h // 2,
            fill=_rgba((0, 0, 0), 60),
        )
        _rounded_rect(
            draw,
            _box(pad, y_btn, pad + btn_w, y_btn + btn_h),
            radius=btn_h // 2,
            fill=pal["cta_bg"],
        )
        ty = y_btn + max(0, (btn_h - int(f_cta.size)) // 2) - 1
        draw.text((pad + int(w * 0.035), ty), text,
                  font=f_cta, fill=pal["cta_text"])

        f_tag = _font(max(8, int(h * 0.022)))
        tag_x = pad + btn_w + int(w * 0.025)
        ty2   = y_btn + max(0, (btn_h - int(f_tag.size)) // 2)
        draw.text((tag_x, ty2), "by RecruitIQ",
                  font=f_tag, fill=_rgba(pal["text_muted"], 150))

    @staticmethod
    def footer(draw, w, h, pal, jd_code) -> None:
        pad    = int(w * 0.08)
        y_foot = int(h * 0.91)
        f_foot = _font(max(8, int(h * 0.020)))

        if y_foot >= h:
            y_foot = h - int(f_foot.size) - 6

        sep_y = max(0, y_foot - 8)
        draw.rectangle(
            _box(pad, sep_y, w - pad, sep_y + 2),
            fill=_rgba(pal["accent1"], 50),
        )

        draw.text(
            (pad, y_foot),
            "RecruitIQ · AI-Powered Recruiting",
            font=f_foot,
            fill=_rgba(pal["text_muted"], 180),
        )

        if jd_code:
            try:
                tw = int(draw.textlength(jd_code, font=f_foot))
            except Exception:
                tw = len(jd_code) * 8
            draw.text(
                (w - pad - tw, y_foot),
                jd_code, font=f_foot,
                fill=_rgba(pal["text_muted"], 120),
            )

    @staticmethod
    def vignette(img: Image.Image, w: int, h: int) -> Image.Image:
        """Soft dark vignette."""
        half     = min(w, h) // 2
        max_margin = int(half * 0.90)
        steps    = 40
        stroke   = max(2, int(min(w, h) * 0.014))

        vig  = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        draw = ImageDraw.Draw(vig)

        for i in range(steps):
            t      = i / steps
            alpha  = int(180 * (1 - t) ** 2.5)
            margin = int(max_margin * t)
            draw.rectangle(
                _box(margin, margin, w - margin, h - margin),
                outline=(0, 0, 0, alpha),
                width=stroke,
            )

        return Image.alpha_composite(img, vig)


# ─────────────────────────────────────────────────────────────────────────────
#  BATCH GENERATOR (NEW)
# ─────────────────────────────────────────────────────────────────────────────

class BatchBannerGenerator:
    """
    Generate banners for multiple JDs in batch.
    Returns organized dict: {jd_id: {platform: bytes, ...}, ...}
    """
    
    def __init__(self, generator: SocialBannerGenerator):
        self.generator = generator
    
    def generate_batch(self, jds: list[dict]) -> dict[int, dict[str, bytes]]:
        """
        Generate banners for multiple JDs.
        
        Args:
            jds: List of JD dicts, each with 'id' field
        
        Returns:
            {
                jd_id_1: {
                    "LinkedIn": <PNG bytes>,
                    "Instagram": <PNG bytes>,
                    ...
                },
                jd_id_2: {...},
            }
        """
        result = {}
        for jd in jds:
            jd_id = jd.get("id", len(result))
            result[jd_id] = self.generator.generate_all(jd)
        return result


# ─────────────────────────────────────────────────────────────────────────────
#  EXAMPLE USAGE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Example JD data
    sample_jd = {
        "role_name": "Senior GenAI & Agentic AI Developer",
        "client_name": "TerraGig",
        "location": "Chennai, India",
        "work_mode": "5 Days WFO",
        "experience_min": 5,
        "experience_max": 7,
        "skillset_required": ["Python", "CrewAI", "GPT-4", "Claude", "PyTorch"],
        "jd_code": "TG-001-2024",
    }
    
    # Generate banners
    gen = SocialBannerGenerator()
    
    # Single platform
    linkedin_png = gen.generate(sample_jd, "LinkedIn")
    print(f"LinkedIn PNG: {len(linkedin_png)} bytes")
    
    # All platforms
    all_banners = gen.generate_all(sample_jd)
    for platform, png_bytes in all_banners.items():
        print(f"{platform}: {len(png_bytes)} bytes")
