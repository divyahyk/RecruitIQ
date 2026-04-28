"""
LLM Handler - Multi-provider fallback for social content generation
TERRAGIG Edition with exponential backoff & retry logic
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod

try:
    import groq
except ImportError:
    groq = None

try:
    import google.generativeai as genai
except ImportError:
    genai = None

try:
    from mistralai import Mistral
except ImportError:
    Mistral = None


# ─────────────────────────────────────────────────────────────────────────────
#  ABSTRACT BASE PROVIDER
# ─────────────────────────────────────────────────────────────────────────────

class BaseLLMProvider(ABC):
    """Abstract base for LLM providers with retry logic."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.max_retries = 3
        self.base_retry_delay = 1
        self.name = self.__class__.__name__
    
    @abstractmethod
    def complete(
        self,
        prompt: str,
        max_tokens: int = 800,
        temperature: float = 0.6,
    ) -> Dict[str, Any]:
        """Generate text completion from prompt."""
        pass
    
    @abstractmethod
    def extract_json(
        self,
        prompt: str,
        schema: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Extract structured JSON from prompt."""
        pass
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff."""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait = self.base_retry_delay * (2 ** attempt)
                    time.sleep(wait)
                else:
                    raise


# ─────────────────────────────────────────────────────────────────────────────
#  GROQ PROVIDER
# ─────────────────────────────────────────────────────────────────────────────

class GroqProvider(BaseLLMProvider):
    """Groq API provider (fastest LLM, 7B-70B models)."""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(api_key)
        self.api_key = api_key or os.getenv("GROQ_API_KEY")
        
        if not self.api_key:
            raise ValueError(
                "❌ GROQ_API_KEY not set. "
                "Get it from https://console.groq.com/keys"
            )
        
        if not groq:
            raise ImportError(
                "❌ 'groq' package not installed. "
                "Run: pip install groq"
            )
        
        self.client = groq.Groq(api_key=self.api_key)
    
    def complete(
        self,
        prompt: str,
        max_tokens: int = 800,
        temperature: float = 0.6,
    ) -> Dict[str, Any]:
        """Call Groq API with exponential backoff."""
        
        def _call():
            response = self.client.chat.completions.create(
                model="mixtral-8x7b-32768",  # Fast, free tier
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=temperature,
            )
            return {
                "text": response.choices[0].message.content,
                "provider": "groq",
                "model": "mixtral-8x7b-32768",
                "usage": {
                    "input": response.usage.prompt_tokens,
                    "output": response.usage.completion_tokens,
                }
            }
        
        return self._retry_with_backoff(_call)
    
    def extract_json(
        self,
        prompt: str,
        schema: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Extract JSON with structured output."""
        
        json_prompt = (
            f"{prompt}\n\n"
            "CRITICAL: Respond with ONLY valid JSON, no markdown backticks, "
            "no explanations."
        )
        
        response = self.complete(json_prompt, max_tokens=2000)
        text = response.get("text", "").strip()
        
        # Clean markdown if present
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            return {
                "error": f"Failed to parse JSON: {str(e)}",
                "raw": text,
                "provider": "groq"
            }


# ─────────────────────────────────────────────────────────────────────────────
#  GEMINI PROVIDER
# ─────────────────────────────────────────────────────────────────────────────

class GeminiProvider(BaseLLMProvider):
    """Google Gemini API provider."""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(api_key)
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        
        if not self.api_key:
            raise ValueError(
                "❌ GEMINI_API_KEY not set. "
                "Get it from https://aistudio.google.com/app/apikeys"
            )
        
        if not genai:
            raise ImportError(
                "❌ 'google-generativeai' package not installed. "
                "Run: pip install google-generativeai"
            )
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel("gemini-pro")
    
    def complete(
        self,
        prompt: str,
        max_tokens: int = 800,
        temperature: float = 0.6,
    ) -> Dict[str, Any]:
        """Call Gemini API with exponential backoff."""
        
        def _call():
            response = self.model.generate_content(
                prompt,
                generation_config={
                    "max_output_tokens": max_tokens,
                    "temperature": temperature,
                }
            )
            return {
                "text": response.text,
                "provider": "gemini",
                "model": "gemini-pro",
            }
        
        return self._retry_with_backoff(_call)
    
    def extract_json(
        self,
        prompt: str,
        schema: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Extract JSON from Gemini."""
        
        json_prompt = (
            f"{prompt}\n\n"
            "CRITICAL: Respond with ONLY valid JSON, no markdown backticks."
        )
        response = self.complete(json_prompt, max_tokens=2000)
        text = response.get("text", "").strip()
        
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            return {
                "error": f"Failed to parse JSON: {str(e)}",
                "raw": text,
                "provider": "gemini"
            }


# ─────────────────────────────────────────────────────────────────────────────
#  MISTRAL PROVIDER
# ─────────────────────────────────────────────────────────────────────────────

class MistralProvider(BaseLLMProvider):
    """Mistral AI provider."""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(api_key)
        self.api_key = api_key or os.getenv("MISTRAL_API_KEY")
        
        if not self.api_key:
            raise ValueError(
                "❌ MISTRAL_API_KEY not set. "
                "Get it from https://console.mistral.ai/api-keys"
            )
        
        if not Mistral:
            raise ImportError(
                "❌ 'mistralai' package not installed. "
                "Run: pip install mistralai"
            )
        
        self.client = Mistral(api_key=self.api_key)
    
    def complete(
        self,
        prompt: str,
        max_tokens: int = 800,
        temperature: float = 0.6,
    ) -> Dict[str, Any]:
        """Call Mistral API with exponential backoff."""
        
        def _call():
            response = self.client.chat.complete(
                model="mistral-large-latest",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=temperature,
            )
            return {
                "text": response.choices[0].message.content,
                "provider": "mistral",
                "model": "mistral-large-latest",
            }
        
        return self._retry_with_backoff(_call)
    
    def extract_json(
        self,
        prompt: str,
        schema: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Extract JSON from Mistral."""
        
        json_prompt = (
            f"{prompt}\n\n"
            "CRITICAL: Respond with ONLY valid JSON, no markdown."
        )
        response = self.complete(json_prompt, max_tokens=2000)
        text = response.get("text", "").strip()
        
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            return {
                "error": f"Failed to parse JSON: {str(e)}",
                "raw": text,
                "provider": "mistral"
            }


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN LLM HANDLER (MULTI-PROVIDER FALLBACK)
# ─────────────────────────────────────────────────────────────────────────────

class LLMHandler:
    """
    Multi-provider LLM handler with intelligent fallback chain.
    
    Priority Order:
    1. Groq (fastest, free tier)
    2. Gemini (reliable, free tier)
    3. Mistral (good quality)
    
    Features:
    - Automatic fallback on provider failure
    - Exponential backoff retry logic
    - Usage tracking per provider
    - Structured JSON extraction
    - Social media prompt templates
    
    Usage:
        llm = LLMHandler()
        
        # Simple text generation
        result = llm.complete("Write a LinkedIn post about...")
        
        # Structured JSON extraction
        data = llm.extract_json("Generate social media content in JSON format...")
        
        # Get provider info
        info = llm.get_provider_info()
    """
    
    def __init__(self, prefer_provider: Optional[str] = None):
        """
        Initialize LLM handler with auto-discovery of providers.
        
        Args:
            prefer_provider: "groq", "gemini", or "mistral" (optional)
        
        Raises:
            ValueError: If no providers are configured
        """
        self.providers: list[BaseLLMProvider] = []
        self.current_provider: Optional[BaseLLMProvider] = None
        self.usage_stats: Dict[str, int] = {}
        
        # Auto-discover and initialize providers in priority order
        providers_config = [
            ("groq", GroqProvider),
            ("gemini", GeminiProvider),
            ("mistral", MistralProvider),
        ]
        
        for name, provider_class in providers_config:
            try:
                provider = provider_class()
                self.providers.append(provider)
                self.usage_stats[name] = 0
                print(f"✅ {name.upper()} provider initialized")
            except (ValueError, ImportError) as e:
                print(f"⚠️  {name.upper()} provider unavailable: {str(e)[:50]}")
        
        if not self.providers:
            raise ValueError(
                "❌ No LLM providers configured.\n"
                "Please set at least one API key:\n"
                "  - GROQ_API_KEY (recommended)\n"
                "  - GEMINI_API_KEY\n"
                "  - MISTRAL_API_KEY"
            )
        
        self.current_provider = self.providers[0]
        print(f"\n🚀 Using {self.current_provider.name} as primary provider")
    
    # ── PUBLIC API ────────────────────────────────────────────────────────────
    
    def complete(
        self,
        prompt: str,
        max_tokens: int = 800,
        temperature: float = 0.6,
    ) -> Dict[str, Any]:
        """
        Generate text with intelligent fallback.
        Tries each provider in order until one succeeds.
        
        Args:
            prompt: Input prompt
            max_tokens: Maximum output tokens
            temperature: Creativity (0.0-1.0)
        
        Returns:
            {
                "text": "Generated content",
                "provider": "groq",
                "model": "mixtral-8x7b-32768",
                "usage": {"input": 10, "output": 42}
            }
        
        Raises:
            RuntimeError: If all providers fail
        """
        last_error = None
        
        for provider in self.providers:
            try:
                self.current_provider = provider
                result = provider.complete(prompt, max_tokens, temperature)
                
                # Track usage
                provider_name = provider.name.lower()
                self.usage_stats[provider_name] += 1
                
                return result
            
            except Exception as e:
                last_error = e
                print(f"⚠️  {provider.name} failed, trying next provider...")
                continue
        
        # All providers failed
        raise RuntimeError(
            f"❌ All LLM providers failed.\n"
            f"Last error: {str(last_error)}\n"
            f"Please check your API keys and internet connection."
        )
    
    def extract_json(
        self,
        prompt: str,
        schema: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        Extract structured JSON with fallback chain.
        
        Args:
            prompt: Input prompt (should request JSON output)
            schema: Optional JSON schema for validation
        
        Returns:
            Parsed JSON dict
        
        Raises:
            RuntimeError: If all providers fail
        """
        last_error = None
        
        for provider in self.providers:
            try:
                self.current_provider = provider
                result = provider.extract_json(prompt, schema)
                
                # Track usage
                provider_name = provider.name.lower()
                self.usage_stats[provider_name] += 1
                
                # Check for errors in result
                if "error" not in result:
                    return result
            
            except Exception as e:
                last_error = e
                print(f"⚠️  {provider.name} JSON extraction failed...")
                continue
        
        raise RuntimeError(
            f"❌ All providers failed at JSON extraction.\n"
            f"Last error: {str(last_error)}"
        )
    
    def generate_social_copy(
        self,
        jd: Dict[str, Any],
        platform: str,
        style: str = "professional",
    ) -> Dict[str, Any]:
        """
        Generate social media copy from JD.
        
        Args:
            jd: Parsed JD dict
            platform: "LinkedIn", "Instagram", "Twitter", "WhatsApp", "Facebook"
            style: "professional", "casual", "urgent", "creative"
        
        Returns:
            {
                "platform": "LinkedIn",
                "style": "professional",
                "copy": "We're hiring...",
                "hashtags": ["#hiring", "#ai"],
                "cta": "Apply now",
                "provider": "groq"
            }
        """
        role = jd.get("role_name", "Position")
        company = jd.get("client_name", "Company")
        location = jd.get("location", "Remote")
        
        prompt = f"""
Generate a {style} social media post for {platform}.

Job Details:
- Role: {role}
- Company: {company}
- Location: {location}

Requirements:
- Keep it engaging and platform-appropriate
- Include relevant hashtags
- Add a clear call-to-action
- Tone: {style}

Respond with JSON:
{{
    "copy": "The main post text",
    "hashtags": ["#hiring", "#tech"],
    "cta": "Call-to-action text",
    "emoji": "Relevant emoji"
}}
"""
        
        result = self.extract_json(prompt)
        
        return {
            "platform": platform,
            "style": style,
            "copy": result.get("copy", ""),
            "hashtags": result.get("hashtags", []),
            "cta": result.get("cta", ""),
            "emoji": result.get("emoji", "🚀"),
            "provider": self.current_provider.name.lower(),
        }
    
    def get_provider_info(self) -> Dict[str, Any]:
        """Get information about available providers and current usage."""
        return {
            "current": self.current_provider.name if self.current_provider else None,
            "available": [p.name for p in self.providers],
            "usage_stats": self.usage_stats,
            "count": len(self.providers),
        }
    
    def switch_provider(self, provider_name: str) -> bool:
        """
        Manually switch to a specific provider.
        
        Args:
            provider_name: "groq", "gemini", or "mistral"
        
        Returns:
            True if switch successful, False otherwise
        """
        for provider in self.providers:
            if provider.name.lower() == provider_name.lower():
                self.current_provider = provider
                print(f"✅ Switched to {provider.name}")
                return True
        
        print(f"❌ Provider '{provider_name}' not available")
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  EXAMPLE USAGE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Initialize
    llm = LLMHandler()
    
    # Simple completion
    result = llm.complete("Write a short LinkedIn post about AI hiring")
    print(f"\n✅ Generated: {result['text'][:100]}...")
    print(f"Provider: {result['provider']}")
    
    # JSON extraction
    json_result = llm.extract_json(
        'Generate JSON: {"name": "Role", "skills": ["Python", "AI"]} for a DevOps position'
    )
    print(f"\n✅ JSON: {json_result}")
    
    # Social copy
    sample_jd = {
        "role_name": "Senior AI Engineer",
        "client_name": "TerraGig",
        "location": "Chennai",
    }
    copy = llm.generate_social_copy(sample_jd, "LinkedIn", "professional")
    print(f"\n✅ Social Copy: {copy['copy']}")
    
    # Provider info
    info = llm.get_provider_info()
    print(f"\n📊 Provider Info: {info}")
