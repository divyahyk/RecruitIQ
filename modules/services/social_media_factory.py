"""
Service Factory for Social Media Suite
Instantiates LLM, Banner Generator, and Database Handler
"""

import logging
from typing import Optional, Dict, Any

from modules.social_media.llm_handler import LLMHandler, Platform, PostStyle
from modules.social_media.banner_generator import SocialBannerGenerator
from modules.social_media.database_handler import SocialMediaDatabaseHandler

logger = logging.getLogger(__name__)


class SocialMediaServiceFactory:
    """
    Factory to instantiate and configure all social media components.
    
    Usage:
        factory = SocialMediaServiceFactory(
            groq_key=os.getenv("GROQ_API_KEY"),
            gemini_key=os.getenv("GEMINI_API_KEY"),
            supabase_manager=supabase_mgr
        )
        services = factory.build()
        # Returns: {"llm": LLMHandler, "banner_gen": SocialBannerGenerator, "db": ...}
    """
    
    def __init__(
        self,
        groq_key: Optional[str] = None,
        gemini_key: Optional[str] = None,
        mistral_key: Optional[str] = None,
        supabase_manager: Optional[Any] = None,
    ):
        self.groq_key = groq_key
        self.gemini_key = gemini_key
        self.mistral_key = mistral_key
        self.supabase = supabase_manager
        
        logger.info("✅ SocialMediaServiceFactory initialized")
    
    def build(self) -> Dict[str, Any]:
        """
        Build all services and return as dict.
        
        Returns:
            {
                "llm": LLMHandler instance,
                "banner_gen": SocialBannerGenerator instance,
                "db": SocialMediaDatabaseHandler instance,
            }
        """
        
        # 1. Initialize LLM Handler
        try:
            llm_handler = LLMHandler(
                groq_key=self.groq_key,
                gemini_key=self.gemini_key,
                mistral_key=self.mistral_key,
            )
            logger.info("✅ LLMHandler initialized")
        except ValueError as e:
            logger.warning(f"⚠️ LLMHandler init failed: {e}")
            llm_handler = None
        
        # 2. Initialize Banner Generator
        try:
            banner_generator = SocialBannerGenerator(llm=llm_handler)
            logger.info("✅ SocialBannerGenerator initialized")
        except Exception as e:
            logger.warning(f"⚠️ BannerGenerator init failed: {e}")
            banner_generator = None
        
        # 3. Initialize Database Handler
        try:
            if self.supabase:
                db_handler = SocialMediaDatabaseHandler(self.supabase)
                logger.info("✅ SocialMediaDatabaseHandler initialized")
            else:
                logger.warning("⚠️ Supabase not provided, DB handler unavailable")
                db_handler = None
        except Exception as e:
            logger.warning(f"⚠️ DBHandler init failed: {e}")
            db_handler = None
        
        return {
            "llm": llm_handler,
            "banner_gen": banner_generator,
            "db": db_handler,
        }
