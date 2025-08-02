## Testing the class with iamges and text
import os
from pathlib import Path
import sys

project_root = Path(__file__).resolve().parents[1]
# Add it to sys.path
sys.path.insert(0, str(project_root))

from pydantic import BaseModel
from PIL import Image
from agent_dir.agent import agent
import json
import asyncio


# Define your result type as a Pydantic model
class ExampleResult(BaseModel):
    sentiment: str
    description: str


image = Image.open("./agent_dir/1735033015921.jpg")
image2 = Image.open("./agent_dir/Screenshot 2025-03-10 165014.png")


async def main():
    # Initialize the Agent WITHIN the async context
    gemini_agent = agent(
        model="gemini-2.0-flash",
        result_type=ExampleResult,
        system_prompt="extract the sex and description and sentiment from the text",
        api_key=os.environ.get("API_KEY"),
        name="GeminiSummarizer",
        model_settings={"temperature": 0.4, "top_p": 0.95},
    )

    inputs = [
        "i love this image so much, so handsome and big muscles",
        "marina is a beautiful woman",
        "tomatoooooo is bad",
    ]

    # Schedule the three async calls concurrently
    tasks = [
        asyncio.create_task(model_inference(data, gemini_agent)) for data in inputs
    ]

    # Await their completion and collect results
    results = await asyncio.gather(*tasks)

    for result in results:
        print(json.dumps(result, indent=4))


async def model_inference(input_data, agent):
    # Pass agent instance as parameter
    print(f"Starting inference for: {input_data}")
    if input_data == "i love this image so much, so handsome and big muscles":
        await asyncio.sleep(10)
    result = await agent.run(input_data)
    print(f"Completed inference for: {input_data}")
    return result


# Run the asynchronous main function
if __name__ == "__main__":
    asyncio.run(main())
