"""
RecruitIQ Social Media Module v2.3
Exports: LLMHandler, SocialBannerGenerator, CanvaBannerGenerator, 
DatabaseHandler, BatchBannerGenerator
"""

from .llm_handler import (
    LLMHandler,
    BaseLLMProvider,
    GroqProvider,
    GeminiProvider,
    MistralProvider,
)

from .banner_generator import (
    SocialBannerGenerator,
    BatchBannerGenerator,
    Platform,
)

from .canva_banner_generator import (
    CanvaBannerGenerator,
    Template,
)

from .database_handler import DatabaseHandler

__all__ = [
    # LLM
    "LLMHandler",
    "BaseLLMProvider",
    "GroqProvider",
    "GeminiProvider",
    "MistralProvider",
    # Banners
    "SocialBannerGenerator",
    "BatchBannerGenerator",
    "CanvaBannerGenerator",
    # Database
    "DatabaseHandler",
    # Enums
    "Platform",
    "Template",
]

__version__ = "2.3.0"
