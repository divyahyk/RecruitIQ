from typing import Dict, List, Tuple, Optional, Any, Union, Set, Callable

class PromptTemplates:

    # ... existing methods ...

    @staticmethod
    def social_post(
        jd: dict,
        platform: str,
        style: str = "Professional",
        extra_context: str = ""
    ) -> str:
        """
        Generate a prompt for social media post generation.
        
        Args:
            jd: Job description dict with keys like role_name, location, skillset_required, etc.
            platform: "LinkedIn", "Instagram", "Twitter", "WhatsApp", or "Facebook"
            style: "Professional", "Casual", "Question", or "Announcement"
            extra_context: User-provided additional context
        
        Returns:
            Prompt string ready for LLM
        """
        
        # Extract JD fields with fallbacks
        role = jd.get("role_name") or jd.get("title") or "Open Position"
        company = jd.get("client_name") or jd.get("company") or "Our Company"
        location = jd.get("location") or "Remote"
        work_mode = jd.get("work_mode") or "Hybrid"
        exp_min = jd.get("experience_min") or 0
        exp_max = jd.get("experience_max") or 0
        budget_min = jd.get("budget_min") or 0
        budget_max = jd.get("budget_max") or 0
        budget_curr = jd.get("budget_currency") or "LPA"
        
        # Parse skills safely
        skills = jd.get("skillset_required") or []
        if isinstance(skills, str):
            try:
                skills = json.loads(skills)
            except Exception:
                skills = []
        skills_str = ", ".join(str(s) for s in skills[:8])
        
        # Extract description (with truncation)
        description = jd.get("jd_description") or jd.get("description") or ""
        if description and len(description) > 300:
            description = description[:300] + "..."
        
        # Platform-specific guidelines
        platform_guidelines = {
            "LinkedIn": (
                "LinkedIn is a professional B2B network. Focus on career growth, industry insights, "
                "and company culture. Use a professional, authoritative tone. Include relevant hashtags "
                "like #hiring #recruitment #careergrowth. Emojis are acceptable but use sparingly."
            ),
            "Instagram": (
                "Instagram is visual and lifestyle-focused. Use engaging emojis, trending hashtags, "
                "and conversational language. Add 5-8 relevant hashtags. Make it shareable and fun. "
                "Include call-to-actions like 'DM us' or 'Apply now'."
            ),
            "Twitter": (
                "Twitter is fast-paced and trend-driven. Be witty, punchy, and use 2-3 hashtags. "
                "Encourage retweets and replies. Use conversational tone. Include relevant keywords "
                "to boost reach."
            ),
            "WhatsApp": (
                "WhatsApp messages are personal and direct. Keep a warm, friendly tone. Be conversational "
                "and direct. Include a clear call-to-action. No hashtags needed."
            ),
            "Facebook": (
                "Facebook is community-focused. Tell a story, ask questions to spark engagement. "
                "Include relevant emojis. Use hashtags but sparingly. Encourage sharing and comments."
            ),
        }
        platform_guide = platform_guidelines.get(platform, platform_guidelines["LinkedIn"])
        
        # Style descriptions
        style_descriptions = {
            "Professional": (
                "Use formal, business-focused language with industry terminology. Highlight key "
                "achievements, responsibilities, and requirements. Sound authoritative and credible."
            ),
            "Casual": (
                "Use friendly, conversational, approachable tone. Make it relatable and engaging. "
                "Avoid jargon. Sound like a friendly recruiter."
            ),
            "Question": (
                "Start with an engaging question to spark discussion and interaction. Make it "
                "thought-provoking and relevant to your target audience."
            ),
            "Announcement": (
                "Use bold, exciting announcement language with strong action verbs. Create urgency "
                "and excitement. Make it sound like breaking news or an exclusive opportunity."
            ),
        }
        style_desc = style_descriptions.get(style, style_descriptions["Professional"])
        
        # Character limits
        char_limits = {
            "LinkedIn": 3000,
            "Instagram": 2200,
            "Twitter": 280,
            "WhatsApp": 1024,
            "Facebook": 63206,
        }
        char_limit = char_limits.get(platform, 3000)
        
        prompt = f"""You are an expert recruiter and social media strategist specializing in tech recruitment.

Your task: Generate a compelling {platform} post for a job opening.

═══ JOB DETAILS ═══
Role: {role}
Company: {company}
Location: {location}
Work Mode: {work_mode}
Experience Required: {exp_min}-{exp_max} years
Salary Range: {budget_curr} {budget_min}-{budget_max}
Key Skills: {skills_str}
Description: {description if description else "(See details above)"}

═══ PLATFORM SPECIFICS ═══
Target Platform: {platform}
Character Limit: {char_limit} characters (STRICTLY ENFORCED)
Platform Guidelines: {platform_guide}

═══ WRITING STYLE ═══
Post Style: {style}
Style Guidelines: {style_desc}

═══ ADDITIONAL CONTEXT ═══
{extra_context if extra_context.strip() else "(No additional context provided)"}

═══ REQUIREMENTS ═══
1. ✅ Stay EXACTLY within {char_limit} characters
2. ✅ Follow the "{style}" style guide precisely
3. ✅ Optimize for {platform} best practices and algorithm
4. ✅ Include relevant hashtags if appropriate for the platform
5. ✅ Include a clear, compelling call-to-action
6. ✅ Make it highly shareable and engaging
7. ✅ No explanations, metadata, or preamble
8. ✅ Return ONLY the post text, nothing else

Generate the {platform} post now:"""
        
        return prompt
