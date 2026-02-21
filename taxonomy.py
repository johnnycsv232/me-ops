import re
from typing import Dict, List, Tuple


TAXONOMY: Dict[str, Dict[str, List[str]]] = {
    "IronClad": {
        "Audience": [r"audience", r"persona", r"target market", r"prospect"],
        "Offer": [r"offer", r"pricing", r"revenue model", r"business model"],
        "Content": [r"content strategy", r"copywriting", r"article", r"post"],
        "Sales": [r"sales", r"crm", r"pipeline", r"deal"]
    },
    "GettUpp": {
        "Product": [r"roadmap", r"feature", r"product-market fit", r"mvp"],
        "UI/UX": [r"ui", r"ux", r"design", r"frontend", r"tailwind", r"figma"],
        "Infrastructure": [r"backend", r"db", r"schema", r"api", r"server", r"mcp"],
        "Marketing": [r"advertising", r"seo", r"campaign", r"growth"]
    },
    "AI Arbitrage": {
        "Automation": [r"n8n", r"zapier", r"workflow", r"automation"],
        "Prompts": [r"prompt engineering", r"llm prompt", r"chain of thought"],
        "Models": [r"openai", r"anthropic", r"gemini", r"llama", r"fine-tuning"],
        "Integration": [r"connector", r"plugin", r"extension", r"api link"]
    },
    "Productivity": {
        "Focus": [r"deep work", r"concentration", r"no-distraction", r"sprint"],
        "Switching": [r"context switch", r"multitasking", r"interruption"],
        "Energy": [r"burnout", r"fatigue", r"optimal time", r"peak performance"],
        "Recovery": [r"break", r"rest", r"sleep", r"health"]
    },
    "Cristina/KB-DS": {
        "Memory": [r"ltm", r"memory", r"vector store", r"chroma", r"pinecone"],
        "Pattern": [r"clustering", r"behavioral pattern", r"trend"],
        "Schema": [r"schema evolution", r"data structure", r"entity definition"],
        "Truth": [r"fact check", r"source of truth", r"validation", r"master truth"]
    }
}


def categorize_event(target_text: str | None, metadata_text: str | None) -> Tuple[str, str]:
    """Categorizes an event based on the combined text of target and metadata.

    Analyzes labels and metadata against the defined taxonomy to map 
    the event to a high-level theme and personal granular subcategory.

    Args:
        target_text: The primary target label of the event.
        metadata_text: Stringified JSON metadata or descriptive text.

    Returns:
        A tuple of (theme, subcategory). Defaults to ("Miscellaneous", "General").
    """
    combined = f"{target_text or ''} {metadata_text or ''}".lower()
    
    for theme, subcategories in TAXONOMY.items():
        for subcat, patterns in subcategories.items():
            for pattern in patterns:
                if re.search(pattern, combined):
                    return theme, subcat
    
    return "Miscellaneous", "General"

if __name__ == "__main__":
    # Test cases
    print(categorize_event("IronClad reach out", "target market mapping"))
    print(categorize_event("GettUpp redesign", "frontend cleanup with tailwind"))
    print(categorize_event("Stitch UI", "dashboard implementation"))
