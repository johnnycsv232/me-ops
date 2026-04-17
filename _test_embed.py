import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from core.config import require_env
from google import genai
from google.genai import types

client = genai.Client(api_key=require_env("GEMINI_API_KEY"))
result = client.models.embed_content(
    model="gemini-embedding-2-preview",
    contents=["IronClad ZIP API broken deployment"],
    config=types.EmbedContentConfig(
        task_type="RETRIEVAL_DOCUMENT",
        output_dimensionality=768
    )
)
v = result.embeddings[0].values
print(f"OK: {len(v)} dims, first3={v[:3]}")
