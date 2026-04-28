"""
Database Handler for social posts & banners
Integrates with Supabase PostgreSQL & Storage
"""

from __future__ import annotations

import os
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

try:
    from supabase import create_client
    from supabase.client import Client
except ImportError:
    create_client = None
    Client = None


# ─────────────────────────────────────────────────────────────────────────────
#  DATABASE HANDLER
# ─────────────────────────────────────────────────────────────────────────────

class DatabaseHandler:
    """
    Manage social posts & banner cache in Supabase.
    
    Tables:
    - social_posts: Generated social media content
    - banner_cache: Generated banner metadata & URLs
    
    Storage:
    - social-banners: Public PNG images
    
    Usage:
        db = DatabaseHandler()
        
        # Save social post
        post_id = db.save_social_post(
            jd_id="jd_123",
            platform="LinkedIn",
            content="We're hiring...",
            style="professional"
        )
        
        # Save banner
        banner_id = db.save_banner(
            jd_id="jd_123",
            platform="LinkedIn",
            image_bytes=png_data,
            image_url="https://...",
            width=1200,
            height=628
        )
        
        # Retrieve posts
        posts = db.get_social_posts("jd_123", platform="LinkedIn")
    """
    
    def __init__(self):
        """Initialize Supabase connection."""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        
        if not self.url or not self.key:
            raise ValueError(
                "❌ SUPABASE_URL and SUPABASE_KEY environment variables not set.\n"
                "Get them from: https://app.supabase.com/project/[project-id]/settings/api"
            )
        
        if not create_client:
            raise ImportError(
                "❌ 'supabase' package not installed.\n"
                "Run: pip install supabase==2.15.3"
            )
        
        self.client: Client = create_client(self.url, self.key)
        print("✅ Supabase connection established")
    
    # ── SOCIAL POSTS ──────────────────────────────────────────────────────────
    
    def save_social_post(
        self,
        jd_id: str,
        platform: str,
        content: str,
        style: str,
        generated_by: str = "RecruitIQ",
        hashtags: Optional[List[str]] = None,
        cta: Optional[str] = None,
    ) -> Optional[str]:
        """
        Save generated social post to database.
        
        Args:
            jd_id: Job description ID
            platform: "LinkedIn", "Instagram", "Twitter", "WhatsApp", "Facebook"
            content: Social post text
            style: "professional", "casual", "urgent", "creative"
            generated_by: Source app name
            hashtags: Optional list of hashtags
            cta: Optional call-to-action text
        
        Returns:
            Post ID if successful, None otherwise
        """
        
        try:
            data = {
                "jd_id": str(jd_id),
                "platform": platform,
                "content": content,
                "style": style,
                "generated_by": generated_by,
                "hashtags": hashtags or [],
                "cta": cta or "",
                "created_at": datetime.utcnow().isoformat(),
            }
            
            response = self.client.table("social_posts").insert(data).execute()
            
            if response.data and len(response.data) > 0:
                post_id = response.data[0].get("id")
                print(f"✅ Social post saved: {post_id}")
                return str(post_id)
            
            print("❌ No data returned from insert")
            return None
        
        except Exception as e:
            print(f"❌ Error saving social post: {str(e)}")
            return None
    
    def get_social_posts(
        self,
        jd_id: str,
        platform: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve social posts for a JD.
        
        Args:
            jd_id: Job description ID
            platform: Optional filter by platform
            limit: Max records to return
        
        Returns:
            List of post records
        """
        
        try:
            query = self.client.table("social_posts").select("*").eq("jd_id", str(jd_id))
            
            if platform:
                query = query.eq("platform", platform)
            
            response = query.order("created_at", desc=True).limit(limit).execute()
            
            print(f"✅ Retrieved {len(response.data or [])} social posts")
            return response.data or []
        
        except Exception as e:
            print(f"❌ Error fetching social posts: {str(e)}")
            return []
    
    def delete_social_post(self, post_id: str) -> bool:
        """
        Delete a social post.
        
        Args:
            post_id: Post ID to delete
        
        Returns:
            True if successful
        """
        
        try:
            self.client.table("social_posts").delete().eq("id", post_id).execute()
            print(f"✅ Deleted social post: {post_id}")
            return True
        except Exception as e:
            print(f"❌ Error deleting post: {str(e)}")
            return False
    
    # ── BANNER CACHE ──────────────────────────────────────────────────────────
    
    def save_banner(
        self,
        jd_id: str,
        platform: str,
        image_bytes: bytes,
        image_url: str,
        width: int,
        height: int,
    ) -> Optional[str]:
        """
        Cache generated banner metadata.
        
        Args:
            jd_id: Job description ID
            platform: Platform name
            image_bytes: PNG image bytes (for size calculation)
            image_url: Public URL of banner
            width: Image width (pixels)
            height: Image height (pixels)
        
        Returns:
            Banner ID if successful
        """
        
        try:
            data = {
                "jd_id": str(jd_id),
                "platform": platform,
                "image_url": image_url,
                "width": width,
                "height": height,
                "size_bytes": len(image_bytes),
                "created_at": datetime.utcnow().isoformat(),
            }
            
            response = self.client.table("banner_cache").insert(data).execute()
            
            if response.data and len(response.data) > 0:
                banner_id = response.data[0].get("id")
                print(f"✅ Banner cached: {banner_id}")
                return str(banner_id)
            
            return None
        
        except Exception as e:
            print(f"❌ Error saving banner metadata: {str(e)}")
            return None
    
    def get_banner(
        self,
        jd_id: str,
        platform: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached banner for a JD + platform.
        
        Args:
            jd_id: Job description ID
            platform: Platform name
        
        Returns:
            Banner record or None
        """
        
        try:
            response = (
                self.client.table("banner_cache")
                .select("*")
                .eq("jd_id", str(jd_id))
                .eq("platform", platform)
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
            
            if response.data and len(response.data) > 0:
                return response.data[0]
            
            return None
        
        except Exception as e:
            print(f"❌ Error fetching banner: {str(e)}")
            return None
    
    def get_all_banners(self, jd_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Get all banners for a JD (all platforms).
        
        Returns:
            {
                "LinkedIn": {...},
                "Instagram": {...},
                ...
            }
        """
        
        try:
            response = (
                self.client.table("banner_cache")
                .select("*")
                .eq("jd_id", str(jd_id))
                .execute()
            )
            
            result = {}
            for banner in response.data or []:
                platform = banner.get("platform")
                result[platform] = banner
            
            return result
        
        except Exception as e:
            print(f"❌ Error fetching all banners: {str(e)}")
            return {}
    
    def delete_banner(self, banner_id: str) -> bool:
        """Delete a cached banner."""
        
        try:
            self.client.table("banner_cache").delete().eq("id", banner_id).execute()
            print(f"✅ Deleted banner: {banner_id}")
            return True
        except Exception as e:
            print(f"❌ Error deleting banner: {str(e)}")
            return False
    
    # ── STORAGE (IMAGES) ──────────────────────────────────────────────────────
    
    def upload_banner_image(
        self,
        jd_id: str,
        platform: str,
        image_bytes: bytes,
    ) -> Optional[str]:
        """
        Upload banner PNG to Supabase Storage.
        
        Args:
            jd_id: Job ID
            platform: Platform name
            image_bytes: PNG image data
        
        Returns:
            Public URL if successful
        """
        
        try:
            bucket_name = "social-banners"
            timestamp = datetime.utcnow().timestamp()
            file_path = f"{jd_id}/{platform}_{timestamp}.png"
            
            # Upload
            self.client.storage.from_(bucket_name).upload(
                file_path,
                image_bytes,
                {"content-type": "image/png"},
            )
            
            # Get public URL
            url = self.client.storage.from_(bucket_name).get_public_url(file_path)
            
            print(f"✅ Banner uploaded: {url}")
            return url
        
        except Exception as e:
            print(f"❌ Error uploading banner: {str(e)}")
            return None
    
    def delete_banner_image(self, jd_id: str, platform: str, filename: str) -> bool:
        """Delete banner image from storage."""
        
        try:
            bucket_name = "social-banners"
            file_path = f"{jd_id}/{filename}"
            
            self.client.storage.from_(bucket_name).remove([file_path])
            print(f"✅ Deleted image: {file_path}")
            return True
        
        except Exception as e:
            print(f"❌ Error deleting image: {str(e)}")
            return False
    
    # ── UTILITY METHODS ───────────────────────────────────────────────────────
    
    def get_jd_summary(self, jd_id: str) -> Dict[str, Any]:
        """Get summary of all social content for a JD."""
        
        try:
            posts = self.get_social_posts(jd_id)
            banners = self.get_all_banners(jd_id)
            
            platforms_with_posts = set(p.get("platform") for p in posts)
            platforms_with_banners = set(banners.keys())
            
            return {
                "jd_id": jd_id,
                "total_posts": len(posts),
                "total_banners": len(banners),
                "platforms_with_posts": list(platforms_with_posts),
                "platforms_with_banners": list(platforms_with_banners),
                "posts": posts,
                "banners": banners,
            }
        
        except Exception as e:
            print(f"❌ Error getting JD summary: {str(e)}")
            return {}
    
    def export_to_json(self, jd_id: str, filename: str = None) -> Optional[str]:
        """Export all content for a JD to JSON file."""
        
        try:
            summary = self.get_jd_summary(jd_id)
            
            filename = filename or f"jd_{jd_id}_export.json"
            
            with open(filename, "w") as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"✅ Exported to: {filename}")
            return filename
        
        except Exception as e:
            print(f"❌ Error exporting: {str(e)}")
            return None


# ─────────────────────────────────────────────────────────────────────────────
#  EXAMPLE USAGE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    db = DatabaseHandler()
    
    # Save social post
    post_id = db.save_social_post(
        jd_id="jd_001",
        platform="LinkedIn",
        content="We're hiring Senior AI Engineers!",
        style="professional",
        hashtags=["#hiring", "#ai", "#recruitment"],
        cta="Apply now"
    )
    
    # Get posts
    posts = db.get_social_posts("jd_001", platform="LinkedIn")
    print(f"✅ Found {len(posts)} posts")
    
    # Save banner
    png_data = b"fake png data"
    banner_id = db.save_banner(
        jd_id="jd_001",
        platform="LinkedIn",
        image_bytes=png_data,
        image_url="https://example.com/banner.png",
        width=1200,
        height=628
    )
    
    # Get summary
    summary = db.get_jd_summary("jd_001")
    print(f"✅ JD Summary: {summary}")
