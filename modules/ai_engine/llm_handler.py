# modules/ai_engine/llm_handler.py

import os
import json
import re
import time
import requests
from typing import Dict, List, Tuple, Optional, Any, Union, Set, Callable


class LLMHandler:
    """
    Multi-provider LLM with free-tier fallback chain.
    Priority: Groq → Gemini → Mistral → HuggingFace → Ollama
    """

    def __init__(self):
        self.fallback_chain = self._build_chain()

    def _build_chain(self) -> list:
        chain = []
        if os.getenv("GROQ_API_KEY"):
            chain.append("groq")
        if os.getenv("GOOGLE_API_KEY"):
            chain.append("gemini")
        if os.getenv("MISTRAL_API_KEY"):
            chain.append("mistral")
        chain.append("huggingface")
        try:
            r = requests.get("http://localhost:11434/api/tags", timeout=2)
            if r.status_code == 200:
                chain.append("ollama")
        except Exception:
            pass
        return chain

    def complete(self, prompt: str, max_tokens: int = 2048,
                 temperature: float = 0.3) -> str:
        for provider in self.fallback_chain:
            try:
                result = self._call(provider, prompt, max_tokens, temperature)
                if result and len(result.strip()) > 5:
                    return result
            except Exception as e:
                print(f"[RecruitIQ LLM] {provider} failed: {e}")
                time.sleep(0.3)
        return ""

    def _call(self, provider: str, prompt: str,
              max_tokens: int, temperature: float) -> str:

        if provider == "groq":
            from groq import Groq
            client = Groq(api_key=os.getenv("GROQ_API_KEY"))
            resp = client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"),
                max_tokens=max_tokens, temperature=temperature
            )
            return resp.choices[0].message.content

        if provider == "gemini":
            import google.generativeai as genai
            genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
            model = genai.GenerativeModel(
                os.getenv("GEMINI_MODEL", "gemini-1.5-flash-8b"),
                generation_config={
                    "max_output_tokens": max_tokens,
                    "temperature": temperature
                }
            )
            return model.generate_content(prompt).text

        if provider == "mistral":
            from mistralai import Mistral
            client = Mistral(api_key=os.getenv("MISTRAL_API_KEY"))
            resp = client.chat.complete(
                model=os.getenv("MISTRAL_MODEL", "open-mistral-nemo"),
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens, temperature=temperature
            )
            return resp.choices[0].message.content

        if provider == "huggingface":
            token = os.getenv("HF_API_TOKEN", "")
            model = os.getenv(
                "HF_MODEL", "mistralai/Mistral-7B-Instruct-v0.2"
            )
            headers = {"Content-Type": "application/json"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            r = requests.post(
                f"https://api-inference.huggingface.co/models/{model}",
                headers=headers,
                json={"inputs": f"[INST] {prompt} [/INST]",
                      "parameters": {"max_new_tokens": min(max_tokens, 1024),
                                     "return_full_text": False}},
                timeout=60
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and data:
                    return data[0].get("generated_text", "")
            raise Exception(f"HF {r.status_code}")

        if provider == "ollama":
            r = requests.post(
                f"{os.getenv('OLLAMA_BASE_URL','http://localhost:11434')}"
                f"/api/generate",
                json={"model": os.getenv("OLLAMA_MODEL", "llama3.1:8b"),
                      "prompt": prompt, "stream": False,
                      "options": {"num_predict": max_tokens,
                                  "temperature": temperature}},
                timeout=120
            )
            if r.status_code == 200:
                return r.json().get("response", "")
            raise Exception(f"Ollama {r.status_code}")

        raise ValueError(f"Unknown provider: {provider}")

    def extract_json(self, text: str) -> Optional[dict]:
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            pass
        for pattern in [
            r'```json\s*(.*?)\s*```',
            r'```\s*(.*?)\s*```',
            r'\{.*\}'
        ]:
            m = re.search(pattern, text, re.DOTALL)
            if m:
                try:
                    chunk = m.group(1) if '```' in pattern else m.group()
                    return json.loads(chunk)
                except Exception:
                    continue
        return None
