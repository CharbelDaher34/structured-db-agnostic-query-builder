from pydantic_ai import Agent, BinaryContent
import os
from pydantic import BaseModel
import io
from typing import Optional, Any, Dict, List
from PIL import Image
import traceback
import time
import asyncio
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider


class agent:
    def __init__(
        self,
        model: str,
        system_prompt: str="",
        result_type: Optional[type[BaseModel]] = None,
        api_key: str="",
        name: Optional[str] = None,
        model_settings: Optional[Dict[str, Any]] = {"temperature": 0.2, "top_p": 0.95},
        retries: int = 3,
        tools: Optional[List[Any]] = None,
    ):
        self.model = model
        self.result_type = result_type
        self.system_prompt = system_prompt
        self.name = name
        self.model_settings = model_settings
        self.retries = retries
        self.tools = tools or []

        # Handle API keys based on model provider prefix
        if model.startswith("ollama/"):
            self.model = OpenAIModel(
                model_name=model.split("ollama/")[1],
                provider=OpenAIProvider(base_url="http://localhost:11434/v1"),
            )
            print(f"ollama model: {model.split('ollama/')[1]}")
        elif model.startswith(("gpt", "openai")):
            os.environ["OPENAI_API_KEY"] = api_key
        elif model.startswith(("anthropic", "claude")):
            os.environ["ANTHROPIC_API_KEY"] = api_key
        elif model.startswith(("google", "gemini")):
            os.environ["GEMINI_API_KEY"] = api_key
        elif model.startswith("cohere:"):
            os.environ["COHERE_API_KEY"] = api_key
        elif model.startswith("groq:"):
            os.environ["GROQ_API_KEY"] = api_key
        elif model.startswith("deepseek:"):
            os.environ["DEEPSEEK_API_KEY"] = api_key
        elif model.startswith("mistral:"):
            os.environ["MISTRAL_API_KEY"] = api_key
        elif model.startswith("bedrock:"):
            # AWS Bedrock requires different credentials setup
            os.environ["AWS_ACCESS_KEY_ID"] = (
                api_key.split(":")[0] if ":" in api_key else api_key
            )
            os.environ["AWS_SECRET_ACCESS_KEY"] = (
                api_key.split(":")[1] if ":" in api_key else ""
            )
        else:
            os.environ["OPENAI_API_KEY"] = api_key

    async def run(self, payload, result_type: Optional[type[BaseModel]] = None):
        agent = Agent(
            model=self.model,
            result_type=result_type or self.result_type or None,  # type: ignore[arg-type]
            system_prompt=self.system_prompt,
            name=self.name,
            model_settings=self.model_settings or None,  # type: ignore[arg-type]
            retries=self.retries,
        )
        print(f"agent created for {agent.model}")
        # print("agent created for",payload)
        # Add tools if provided
        if self.tools:
            for tool in self.tools:
                agent.tool(tool)

        for i, load in enumerate(payload):
            if isinstance(load, Image.Image):
                img_byte_arr = io.BytesIO()
                load.save(img_byte_arr, format="PNG")
                payload[i] = BinaryContent(
                    data=img_byte_arr.getvalue(), media_type="image/png"
                )

        attempts = 0
        while attempts < 3:
            try:
                result = await agent.run(payload)
                return (
                    result.output.model_dump(mode="json")  # type: ignore[attr-defined]
                    if hasattr(result.output, "model_dump")
                    else result.output
                )
            except Exception as e:
                attempts += 1
                if hasattr(e, "status_code"):
                    if e.status_code == 503:  # type: ignore[attr-defined]
                        print("503 error, retrying...")
                        time.sleep(10)
                    elif e.status_code == 429:  # type: ignore[attr-defined]
                        print("429 error, retrying...")
                        time.sleep(10)
                print(e)
                print(traceback.format_exc())

                if attempts >= 3:
                    raise Exception(
                        f"Failed to run agent after {attempts} attempts: {str(e)}"
                    )

        raise Exception("Failed to run agent")

    # async def run_stream(self, payload):
    #     """Run the agent with streaming response"""
    #     agent = Agent(
    #         model=self.model,
    #         result_type=self.result_type,
    #         system_prompt=self.system_prompt,
    #         name=self.name,
    #         model_settings=self.model_settings,
    #         retries=self.retries,
    #     )

    #     # Add tools if provided
    #     if self.tools:
    #         for tool in self.tools:
    #             agent.tool(tool)

    #     # Process payload: Convert any images to BinaryContent
    #     for i, load in enumerate(payload):
    #         if isinstance(load, Image.Image):
    #             img_byte_arr = io.BytesIO()
    #             load.save(img_byte_arr, format='PNG')
    #             payload[i] = BinaryContent(
    #                 data=img_byte_arr.getvalue(), media_type='image/png'
    #             )

    #     # Use streaming: properly use the async context manager
    #     async with agent.run_stream(payload) as result:
    #         async for message in result.stream():
    #             yield message
    def run_sync(self, payload):
        try:
            """Run the agent synchronously"""
            agent = Agent(
                model=self.model,
                result_type=self.result_type,  # type: ignore[arg-type]
                system_prompt=self.system_prompt,
                name=self.name,
                model_settings=self.model_settings,  # type: ignore[arg-type]
                retries=self.retries,
            )
            print(f"agent created for {agent.model}")
            # Add tools if provided
            if self.tools:
                for tool in self.tools:
                    agent.tool(tool)

            for i, load in enumerate(payload):
                if isinstance(load, Image.Image):
                    img_byte_arr = io.BytesIO()
                    load.save(img_byte_arr, format="PNG")
                    payload[i] = BinaryContent(
                        data=img_byte_arr.getvalue(), media_type="image/png"
                    )

            result = agent.run_sync(payload)

            return (
                result.output.model_dump(mode="json")  # type: ignore[attr-defined]
                if hasattr(result.output, "model_dump")
                else result.output
            )
        except Exception as e:
            if "429" in str(e):
                # wait for 10 seconds
                time.sleep(10)
                return self.run_sync(payload)
            print(e)
            print(traceback.format_exc())
            return None

    async def batch(self, batch_inputs: list[tuple[list[Any], BaseModel]]):
        agent_instance = self

        async def run_single(payload, result_type):
            return await agent_instance.run(payload, result_type)

        tasks = [
            run_single(payload, result_type) for payload, result_type in batch_inputs
        ]
        results = await asyncio.gather(*tasks)
        return results
