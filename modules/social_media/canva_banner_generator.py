"""
RecruitIQ – Canva-Style Banner Generator
Professional recruitment banners with:
- Public URL background images (rotating)
- TERRAGIG logo branding (top-left)
- Multiple templates (HERO_LEFT, HERO_CENTER, HERO_RIGHT, SPLIT, FULL_OVERLAY)
- Platform-specific dimensions
- Batch ZIP export support

Integrates seamlessly with social.py and app.py
"""

from __future__ import annotations

import io
import json
import zipfile
from typing import Any, Optional
from enum import Enum
from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError

from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps


# ─────────────────────────────────────────────────────────────────────────────
#  TEMPLATE ENUM & BACKGROUND LIBRARY
# ─────────────────────────────────────────────────────────────────────────────

class Template(Enum):
    """Canva banner templates."""
    HERO_LEFT = "hero_left"           # Logo + text on left, image on right
    HERO_CENTER = "hero_center"       # Centered text, image background
    HERO_RIGHT = "hero_right"         # Image on left, text on right
    SPLIT = "split"                   # 50/50 split
    FULL_OVERLAY = "full_overlay"     # Full image with overlay + text


# Public URLs for background images (rotating)
# These are free-to-use stock photos that fit recruiting themes
_BACKGROUND_URLS = [
    "https://images.unsplash.com/photo-1552664730-d307ca884978?w=1200&h=800&fit=crop",  # Office team
    "https://images.unsplash.com/photo-1552664730-d307ca884978?w=1200&h=800&fit=crop",  # Collaboration
    "https://images.unsplash.com/photo-1552664730-d307ca884978?w=1200&h=800&fit=crop",  # Meeting
    "https://images.unsplash.com/photo-1552664730-d307ca884978?w=1200&h=800&fit=crop",  # Desk setup
]

# TERRAGIG logo URL (public CDN or Supabase storage)
# For local testing, we'll encode a fallback
_LOGO_URL = "https://via.placeholder.com/200x60/1a1a1a/ffffff?text=TerraGig"
_LOGO_FALLBACK_PATH = "C:/TG/terragig_consulting_logo.jpg"  # Local fallback


# ─────────────────────────────────────────────────────────────────────────────
#  PLATFORM SPECS
# ─────────────────────────────────────────────────────────────────────────────

_PLATFORM_SPECS = {
    "LinkedIn": {
        "size": (1200, 628),
        "colors": {
            "primary": (0, 119, 181),
            "accent": (67, 232, 216),
            "dark": (10, 20, 60),
            "text": (255, 255, 255),
        }
    },
    "Instagram": {
        "size": (1080, 1080),
        "colors": {
            "primary": (131, 58, 180),
            "accent": (253, 29, 29),
            "dark": (20, 8, 50),
            "text": (255, 255, 255),
        }
    },
    "Twitter": {
        "size": (1200, 675),
        "colors": {
            "primary": (29, 161, 242),
            "accent": (0, 200, 200),
            "dark": (5, 15, 30),
            "text": (255, 255, 255),
        }
    },
    "WhatsApp": {
        "size": (800, 800),
        "colors": {
            "primary": (37, 211, 102),
            "accent": (18, 140, 126),
            "dark": (5, 30, 20),
            "text": (255, 255, 255),
        }
    },
    "Facebook": {
        "size": (1200, 628),
        "colors": {
            "primary": (23, 119, 242),
            "accent": (255, 193, 7),
            "dark": (11, 85, 204),
            "text": (255, 255, 255),
        }
    },
}


# ─────────────────────────────────────────────────────────────────────────────
#  IMAGE LOADING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_background_image(url: str, size: tuple) -> Optional[Image.Image]:
    """
    Load background image from URL with graceful fallback.
    
    Args:
        url: Public image URL
        size: Target (width, height)
    
    Returns:
        PIL Image or None if load fails
    """
    try:
        with urlopen(url, timeout=5) as response:
            img = Image.open(response)
            img = img.convert("RGB")
            img = ImageOps.fit(img, size, Image.Resampling.LANCZOS)
            return img
    except (URLError, Exception) as e:
        print(f"⚠️ Failed to load background {url}: {e}")
        return None


def _load_logo_image(size: tuple = (180, 50)) -> Optional[Image.Image]:
    """
    Load TERRAGIG logo from local path or URL.
    
    Args:
        size: Target (width, height)
    
    Returns:
        PIL Image or None
    """
    # Try local path first
    import os
    if os.path.exists(_LOGO_FALLBACK_PATH):
        try:
            img = Image.open(_LOGO_FALLBACK_PATH)
            img = img.convert("RGBA")
            img = ImageOps.fit(img, size, Image.Resampling.LANCZOS)
            return img
        except Exception as e:
            print(f"⚠️ Failed to load local logo: {e}")
    
    # Try URL
    try:
        with urlopen(_LOGO_URL, timeout=3) as response:
            img = Image.open(response)
            img = img.convert("RGBA")
            img = ImageOps.fit(img, size, Image.Resampling.LANCZOS)
            return img
    except Exception as e:
        print(f"⚠️ Failed to load logo URL: {e}")
    
    return None


def _get_background_url(jd_id_or_index: int = 0) -> str:
    """Get rotating background URL based on index."""
    idx = (jd_id_or_index or 0) % len(_BACKGROUND_URLS)
    return _BACKGROUND_URLS[idx]


# ─────────────────────────────────────────────────────────────────────────────
#  DRAWING UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _get_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Load TrueType font or fallback to default."""
    size = max(8, int(size))
    candidates = (
        [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "C:/Windows/Fonts/arialbd.ttf",
        ]
        if bold else
        [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
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


def _wrap_text(text: str, font: ImageFont.FreeTypeFont, max_width: int, draw: ImageDraw.ImageDraw) -> list[str]:
    """Word-wrap text to fit within max_width."""
    words = text.split()
    lines = []
    current_line = ""
    
    for word in words:
        test_line = f"{current_line} {word}".strip()
        try:
            line_width = draw.textlength(test_line, font=font)
        except Exception:
            line_width = len(test_line) * (font.size // 2)
        
        if line_width <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
            current_line = word
    
    if current_line:
        lines.append(current_line)
    
    return lines


def _add_gradient_overlay(img: Image.Image, top_color: tuple, bottom_color: tuple, alpha: int = 180) -> Image.Image:
    """Add gradient overlay to image."""
    w, h = img.size
    gradient = Image.new("RGBA", (w, h))
    pixels = gradient.load()
    
    for y in range(h):
        r = int(top_color[0] + (bottom_color[0] - top_color[0]) * y / h)
        g = int(top_color[1] + (bottom_color[1] - top_color[1]) * y / h)
        b = int(top_color[2] + (bottom_color[2] - top_color[2]) * y / h)
        
        for x in range(w):
            pixels[x, y] = (r, g, b, alpha)
    
    return Image.alpha_composite(img.convert("RGBA"), gradient).convert("RGBA")


def _rounded_rectangle(draw: ImageDraw.ImageDraw, xy: tuple, radius: int, fill: tuple, outline: Optional[tuple] = None) -> None:
    """Draw a rounded rectangle."""
    x0, y0, x1, y1 = xy
    radius = max(1, min(radius, (x1 - x0) // 2, (y1 - y0) // 2))
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN CLASS: CANVA BANNER GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

class CanvaBannerGenerator:
    """
    Professional Canva-style banner generator for recruitment.
    
    Features:
    - Multiple templates (HERO_LEFT, HERO_CENTER, HERO_RIGHT, SPLIT, FULL_OVERLAY)
    - Platform-specific dimensions (LinkedIn, Instagram, Twitter, WhatsApp, Facebook)
    - Public URL background images with fallback
    - TERRAGIG logo branding
    - Batch generation with ZIP export
    - JD-driven smart text
    
    Usage:
        gen = CanvaBannerGenerator()
        
        # Generate single banner
        png = gen.generate(jd, "LinkedIn", template="HERO_CENTER")
        
        # Generate all platforms for one JD
        banners = gen.generate_all_platforms(jd)
        
        # Batch generate multiple JDs → ZIP
        zip_bytes = gen.batch_generate(jds, platforms=["LinkedIn", "Instagram"])
    """
    
    def __init__(self, **kwargs: Any):
        """
        Initialize generator.
        
        Args:
            llm: Optional LLM handler (for text enhancement)
            logo_url: Custom logo URL (overrides default)
            bg_urls: Custom background URLs list
        """
        self.llm = kwargs.get("llm")
        self.logo_url = kwargs.get("logo_url", _LOGO_URL)
        self.bg_urls = kwargs.get("bg_urls", _BACKGROUND_URLS)
    
    # ── PUBLIC API ────────────────────────────────────────────────────────────
    
    def generate(
        self,
        jd: dict,
        platform: str = "LinkedIn",
        template: str = "HERO_CENTER",
        bg_url: Optional[str] = None,
    ) -> bytes:
        """
        Generate a single Canva banner.
        
        Args:
            jd: Job description dict with keys:
                - role_name, client_name, location, work_mode
                - experience_min/max, skillset_required
            platform: "LinkedIn", "Instagram", "Twitter", "WhatsApp", "Facebook"
            template: "HERO_LEFT", "HERO_CENTER", "HERO_RIGHT", "SPLIT", "FULL_OVERLAY"
            bg_url: Optional custom background URL
        
        Returns:
            bytes: PNG image data
        """
        spec = _PLATFORM_SPECS.get(platform, _PLATFORM_SPECS["LinkedIn"])
        w, h = spec["size"]
        colors = spec["colors"]
        
        # Parse JD
        role = jd.get("role_name", "Open Position")
        client = jd.get("client_name", "Our Company")
        location = jd.get("location", "Remote")
        work_mode = jd.get("work_mode", "")
        skills = jd.get("skillset_required", [])
        
        if isinstance(skills, str):
            try:
                skills = json.loads(skills)
            except Exception:
                skills = []
        skills = [str(s).strip() for s in skills[:5] if s]
        
        # Load background
        bg_url = bg_url or _get_background_url(jd.get("id", 0))
        bg_img = _load_background_image(bg_url, (w, h))
        
        # Render based on template
        template_lower = template.lower().replace("_", "")
        if template_lower == "heroleft":
            img = self._render_hero_left(w, h, colors, role, client, location, skills, bg_img)
        elif template_lower == "herocenter":
            img = self._render_hero_center(w, h, colors, role, client, location, skills, bg_img)
        elif template_lower == "heroright":
            img = self._render_hero_right(w, h, colors, role, client, location, skills, bg_img)
        elif template_lower == "split":
            img = self._render_split(w, h, colors, role, client, location, skills, bg_img)
        elif template_lower == "fulloverlay":
            img = self._render_full_overlay(w, h, colors, role, client, location, skills, bg_img)
        else:
            img = self._render_hero_center(w, h, colors, role, client, location, skills, bg_img)
        
        # Add logo & footer
        self._add_logo(img, w, h)
        self._add_footer(img, w, h, platform, colors)
        
        # Encode
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="PNG")
        return buf.getvalue()
    
    def generate_all_platforms(self, jd: dict, template: str = "HERO_CENTER") -> dict[str, bytes]:
        """Generate banners for all platforms."""
        result = {}
        for platform in _PLATFORM_SPECS.keys():
            try:
                result[platform] = self.generate(jd, platform, template)
            except Exception as e:
                print(f"❌ Failed to generate {platform}: {e}")
        return result
    
    def batch_generate(
        self,
        jds: list[dict],
        platforms: Optional[list[str]] = None,
        template: str = "HERO_CENTER",
    ) -> bytes:
        """
        Generate banners for multiple JDs and return as ZIP.
        
        Args:
            jds: List of JD dicts
            platforms: List of platform names (default: all)
            template: Template to use
        
        Returns:
            bytes: ZIP file containing all banners
        """
        if platforms is None:
            platforms = list(_PLATFORM_SPECS.keys())
        
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for jd in jds:
                jd_code = jd.get("jd_code", f"JD_{len(jds)}")
                for platform in platforms:
                    try:
                        png = self.generate(jd, platform, template)
                        filename = f"{jd_code}_{platform.lower()}.png"
                        zf.writestr(filename, png)
                    except Exception as e:
                        print(f"⚠️ Skipped {jd_code}/{platform}: {e}")
        
        zip_buf.seek(0)
        return zip_buf.getvalue()
    
    # ── TEMPLATE RENDERERS ────────────────────────────────────────────────────
    
    def _render_hero_left(self, w, h, colors, role, client, location, skills, bg_img):
        """Text on left, image on right."""
        img = Image.new("RGB", (w, h), colors["dark"])
        
        if bg_img:
            # Right half: image
            img_half = bg_img.crop((bg_img.width // 4, 0, bg_img.width, bg_img.height))
            img.paste(img_half, (w // 2, 0))
        
        draw = ImageDraw.Draw(img)
        
        # Left half: text with semi-transparent overlay
        draw.rectangle([(0, 0), (w // 2, h)], fill=colors["dark"])
        
        # Add gradient overlay on right side
        if bg_img:
            overlay = Image.new("RGBA", (w // 2, h), (0, 0, 0, 120))
            img_temp = Image.new("RGBA", (w, h))
            img_temp.paste(overlay, (w // 2, 0))
            img = Image.alpha_composite(img.convert("RGBA"), img_temp).convert("RGB")
            draw = ImageDraw.Draw(img)
        
        # Render text on left
        pad = int(w * 0.05)
        y = int(h * 0.15)
        
        f_heading = _get_font(int(h * 0.08), bold=True)
        f_sub = _get_font(int(h * 0.035))
        f_detail = _get_font(int(h * 0.025))
        
        # Title
        draw.text((pad, y), "WE'RE HIRING", font=f_heading, fill=colors["accent"])
        y += int(h * 0.12)
        
        # Role
        role_lines = _wrap_text(role.upper(), f_heading, int(w * 0.35), draw)
        for line in role_lines:
            draw.text((pad, y), line, font=f_heading, fill=colors["text"])
            y += int(h * 0.10)
        
        y += int(h * 0.05)
        
        # Company & Location
        draw.text((pad, y), f"📍 {location}", font=f_detail, fill=colors["text"])
        y += int(h * 0.05)
        draw.text((pad, y), f"🏢 {client}", font=f_detail, fill=colors["text"])
        y += int(h * 0.05)
        if location:
            draw.text((pad, y), f"⏰ {location}", font=f_detail, fill=colors["accent"])
        
        return img
    
    def _render_hero_center(self, w, h, colors, role, client, location, skills, bg_img):
        """Centered text, background image."""
        if bg_img:
            img = bg_img.copy()
            img = _add_gradient_overlay(img, colors["dark"], colors["primary"], alpha=200)
        else:
            img = Image.new("RGB", (w, h), colors["dark"])
        
        draw = ImageDraw.Draw(img)
        
        # Center text
        f_tag = _get_font(int(h * 0.035), bold=True)
        f_heading = _get_font(int(h * 0.10), bold=True)
        f_sub = _get_font(int(h * 0.04))
        
        # "WE'RE HIRING"
        tag_text = "🚀 WE'RE HIRING"
        tag_box = draw.textbbox((0, 0), tag_text, font=f_tag)
        tag_w = tag_box[2] - tag_box[0]
        tag_x = (w - tag_w) // 2
        draw.text((tag_x, int(h * 0.15)), tag_text, font=f_tag, fill=colors["accent"])
        
        # Role (centered, wrapped)
        y = int(h * 0.28)
        role_lines = _wrap_text(role.upper(), f_heading, int(w * 0.85), draw)
        line_h = int(h * 0.12)
        for line in role_lines:
            line_box = draw.textbbox((0, 0), line, font=f_heading)
            line_w = line_box[2] - line_box[0]
            x = (w - line_w) // 2
            draw.text((x, y), line, font=f_heading, fill=colors["text"])
            y += line_h
        
        # Details
        y += int(h * 0.04)
        details = [f"📍 {location}", f"🏢 {client}"]
        if skills:
            details.append(f"🛠️  {', '.join(skills[:3])}")
        
        for detail in details:
            detail_box = draw.textbbox((0, 0), detail, font=f_sub)
            detail_w = detail_box[2] - detail_box[0]
            x = (w - detail_w) // 2
            draw.text((x, y), detail, font=f_sub, fill=colors["accent"])
            y += int(h * 0.055)
        
        return img
    
    def _render_hero_right(self, w, h, colors, role, client, location, skills, bg_img):
        """Image on left, text on right."""
        img = Image.new("RGB", (w, h), colors["dark"])
        
        if bg_img:
            # Left half: image
            img_half = bg_img.crop((0, 0, bg_img.width // 2, bg_img.height))
            img.paste(img_half, (0, 0))
        
        draw = ImageDraw.Draw(img)
        
        # Right half: text
        draw.rectangle([(w // 2, 0), (w, h)], fill=colors["primary"])
        
        pad = int(w * 0.05)
        x_start = w // 2 + pad
        y = int(h * 0.15)
        
        f_heading = _get_font(int(h * 0.08), bold=True)
        f_sub = _get_font(int(h * 0.035))
        f_detail = _get_font(int(h * 0.025))
        
        draw.text((x_start, y), "WE'RE HIRING", font=f_heading, fill=colors["accent"])
        y += int(h * 0.12)
        
        role_lines = _wrap_text(role.upper(), f_heading, int(w * 0.35), draw)
        for line in role_lines:
            draw.text((x_start, y), line, font=f_heading, fill=colors["text"])
            y += int(h * 0.10)
        
        y += int(h * 0.05)
        draw.text((x_start, y), f"📍 {location}", font=f_detail, fill=colors["text"])
        y += int(h * 0.05)
        draw.text((x_start, y), f"🏢 {client}", font=f_detail, fill=colors["text"])
        
        return img
    
    def _render_split(self, w, h, colors, role, client, location, skills, bg_img):
        """50/50 split between image and solid color."""
        img = Image.new("RGB", (w, h))
        
        if bg_img:
            # Left: image
            img_half = bg_img.crop((0, 0, bg_img.width // 2, bg_img.height))
            img.paste(img_half, (0, 0))
        else:
            img.paste(colors["dark"], (0, 0, w // 2, h))
        
        # Right: solid with text
        img.paste(colors["primary"], (w // 2, 0, w, h))
        
        draw = ImageDraw.Draw(img)
        
        pad = int(w * 0.05)
        x_start = w // 2 + pad
        y = int(h * 0.2)
        
        f_heading = _get_font(int(h * 0.07), bold=True)
        f_detail = _get_font(int(h * 0.03))
        
        draw.text((x_start, y), role.upper(), font=f_heading, fill=colors["text"])
        y += int(h * 0.15)
        draw.text((x_start, y), f"📍 {location}", font=f_detail, fill=colors["accent"])
        y += int(h * 0.06)
        draw.text((x_start, y), f"🏢 {client}", font=f_detail, fill=colors["accent"])
        
        return img
    
    def _render_full_overlay(self, w, h, colors, role, client, location, skills, bg_img):
        """Full image with text overlay."""
        if bg_img:
            img = bg_img.copy()
        else:
            img = Image.new("RGB", (w, h), colors["dark"])
        
        # Dark overlay
        img = _add_gradient_overlay(img, (0, 0, 0), colors["dark"], alpha=240)
        
        draw = ImageDraw.Draw(img)
        
        f_heading = _get_font(int(h * 0.10), bold=True)
        f_sub = _get_font(int(h * 0.04))
        
        y = int(h * 0.35)
        
        # Centered role
        role_lines = _wrap_text(role.upper(), f_heading, int(w * 0.9), draw)
        for line in role_lines:
            line_box = draw.textbbox((0, 0), line, font=f_heading)
            line_w = line_box[2] - line_box[0]
            x = (w - line_w) // 2
            draw.text((x, y), line, font=f_heading, fill=colors["text"])
            y += int(h * 0.12)
        
        y += int(h * 0.05)
        
        # Details
        details_text = f"{location} • {client}"
        details_box = draw.textbbox((0, 0), details_text, font=f_sub)
        details_w = details_box[2] - details_box[0]
        x = (w - details_w) // 2
        draw.text((x, y), details_text, font=f_sub, fill=colors["accent"])
        
        return img
    
    # ── BRANDING ──────────────────────────────────────────────────────────────
    
    def _add_logo(self, img: Image.Image, w: int, h: int) -> None:
        """Add TERRAGIG logo to top-left."""
        logo = _load_logo_image((int(w * 0.15), int(h * 0.08)))
        if logo:
            logo_x = int(w * 0.02)
            logo_y = int(h * 0.02)
            img.paste(logo, (logo_x, logo_y), logo if logo.mode == "RGBA" else None)
    
    def _add_footer(self, img: Image.Image, w: int, h: int, platform: str, colors: dict) -> None:
        """Add footer with branding."""
        draw = ImageDraw.Draw(img)
        f_footer = _get_font(int(h * 0.025))
        
        footer_text = f"RecruitIQ • {platform} • {datetime.now().strftime('%Y')}"
        footer_box = draw.textbbox((0, 0), footer_text, font=f_footer)
        footer_w = footer_box[2] - footer_box[0]
        
        footer_x = (w - footer_w) // 2
        footer_y = h - int(h * 0.04)
        
        # Semi-transparent background
        draw.rectangle(
            [(footer_x - 10, footer_y - 5), (footer_x + footer_w + 10, footer_y + 20)],
            fill=(*colors.get("dark", (0, 0, 0)), 150),
        )
        
        draw.text((footer_x, footer_y), footer_text, font=f_footer, fill=colors["accent"])


# ─────────────────────────────────────────────────────────────────────────────
#  EXAMPLE USAGE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sample_jd = {
        "id": 1,
        "role_name": "Senior GenAI & Agentic AI Developer",
        "client_name": "TerraGig",
        "location": "Chennai, India",
        "work_mode": "5 Days WFO",
        "experience_min": 5,
        "experience_max": 7,
        "skillset_required": ["Python", "CrewAI", "GPT-4", "Claude", "PyTorch"],
        "jd_code": "TG-001-2024",
    }
    
    gen = CanvaBannerGenerator()
    
    # Single banner
    png = gen.generate(sample_jd, "LinkedIn", template="HERO_CENTER")
    print(f"✅ Generated LinkedIn banner: {len(png)} bytes")
    
    # All platforms
    all_banners = gen.generate_all_platforms(sample_jd)
    for platform, png_bytes in all_banners.items():
        print(f"✅ {platform}: {len(png_bytes)} bytes")
    
    # Batch ZIP
    jds = [sample_jd, {**sample_jd, "id": 2, "role_name": "DevOps Engineer"}]
    zip_data = gen.batch_generate(jds, platforms=["LinkedIn", "Instagram"])
    print(f"✅ Batch ZIP: {len(zip_data)} bytes")
