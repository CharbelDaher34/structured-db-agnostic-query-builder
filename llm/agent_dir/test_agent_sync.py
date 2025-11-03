import sys
import os
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
# Add it to sys.path
sys.path.insert(0, str(project_root))

from pydantic import BaseModel
from agent_dir.agent import agent
from PIL import Image
import json
import dotenv

dotenv.load_dotenv()


# Define result schema
class SentimentResult(BaseModel):
    sentiment: str
    summary: str
    confidence: float


# Initialize agent synchronously
sentiment_analyzer = agent(
    model="gemini-2.0-flash",
    output_type=SentimentResult,
    system_prompt="Analyze text sentiment and summarize key points",
    api_key=os.environ.get("API_KEY"),
    model_settings={"temperature": 0.2},
)

# Prepare payload with text and image
payload = [
    "User feedback:",
    "The new search functionality works incredibly well! Much faster than before and the results are more relevant. However, the dark mode could use more contrast.",
    Image.open("./agent_dir/Screenshot 2025-03-10 165014.png"),  # Image for context
]

# Run synchronous analysis
result = sentiment_analyzer.run_sync(payload)
print(json.dumps(result, indent=2))

# Example output:
# {
#   "sentiment": "positive",
#   "summary": "Praised improved search functionality but suggested dark mode contrast improvements",
#   "confidence": 0.89
# }
