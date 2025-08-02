import os
import sys
from pathlib import Path
from typing import Union, List, Any, Optional
import io
import traceback

from pydantic import BaseModel
from PIL import Image

# import fitz  # PyMuPDF for PDF handling
import pymupdf as fitz
import asyncio

from llm.agent_dir.agent import agent
import dotenv

dotenv.load_dotenv()


class LLM:
    def __init__(
        self,
        system_prompt: str,
        result_type: type[BaseModel],
        model_settings: dict = {"temperature": 0.2, "top_p": 0.95},
        api_key: str = "",
        model: str = "gemini-2.0-flash",
    ):
        """
        Initialize the LLM with the specified AI model.

        Args:
            model: Name of the LLM model to use
            api_key: API key for the model provider (if not provided, will use environment variable)
            model_settings: Additional settings for the model
        """
        self.api_key = api_key or os.environ.get("API_KEY")
        if not self.api_key:
            raise ValueError(
                "API key must be provided either in constructor or as API_KEY environment variable"
            )

        self.model_settings = model_settings

        self.result_type = result_type
        # Initialize the agent with Candidate as the result type
        if self.result_type:
            self.llm_agent = agent(
                model=model,
                result_type=self.result_type,
                system_prompt=system_prompt,
                api_key=self.api_key,
                model_settings=self.model_settings,
            )
        else:
            self.llm_agent = agent(
                model=model,
                system_prompt=system_prompt,
                api_key=self.api_key,
            )

    # def _extract_text_from_pdf(self, pdf_path: str) -> str:
    #     """Extract text content from a PDF file."""
    #     text = ""
    #     try:
    #         doc = fitz.open(pdf_path)
    #         for page in doc:
    #             text += page.get_text()
    #         doc.close()
    #         return text
    #     except Exception as e:
    #         print(f"Error extracting text from PDF: {e}")
    #         return ""

    def _extract_images_from_pdf(self, pdf_path: str) -> List[Image.Image]:
        """Extract images from a PDF file."""
        images = []
        try:
            doc = fitz.open(pdf_path)
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                image_list = page.get_images(full=True)

                for img_index, img in enumerate(image_list):
                    xref = img[0]
                    base_image = doc.extract_image(xref)
                    image_bytes = base_image["image"]

                    # Convert to PIL Image
                    pil_image = Image.open(io.BytesIO(image_bytes))
                    images.append(pil_image)

            doc.close()
            return images
        except Exception as e:
            print(f"Error extracting images from PDF: {e}")
            return []

    def _render_pdf_pages_as_images(self, pdf_path: str) -> List[Image.Image]:
        """Render each page of the PDF as an image."""
        images = []
        try:
            doc = fitz.open(pdf_path)
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                pix = page.get_pixmap(  # type: ignore[attr-defined]
                    matrix=fitz.Matrix(2, 2)
                )  # 2x scaling for better resolution
                img_bytes = pix.tobytes("png")
                pil_image = Image.open(io.BytesIO(img_bytes))
                images.append(pil_image)

            doc.close()
            return images
        except Exception as e:
            print(f"Error rendering PDF pages as images: {e}")
            return []

    # def parse(self, input_data: list[Union[str, Image.Image, List[Any]]]) -> BaseModel:
    #     """
    #     Parse resume data synchronously from various input types.

    #     Args:
    #         input_data: Can be one of:
    #             - Path to a PDF file
    #             - Raw text string
    #             - PIL Image object
    #             - List containing text and/or images

    #     Returns:
    #         BaseModel: Parsed data as a Pydantic model
    #     """
    #     if not isinstance(input_data, list):
    #         input_data = [input_data]

    #     payload = []

    #     for item in input_data:
    #         # Handle different input types
    #         if isinstance(item, str):
    #             # Check if it's a path to a PDF file
    #             if item.lower().endswith('.pdf'):
    #                 # For PDFs, convert each page to an image
    #                 page_images = self._render_pdf_pages_as_images(item)
    #                 payload.extend(page_images)
    #             else:
    #                 # For raw text, send as is
    #                 payload.append(item)

    #         elif isinstance(item, Image.Image):
    #             # Single image input
    #             payload.append(item)

    #         elif isinstance(item, list):
    #             # List of mixed inputs
    #             payload.extend(item)

    #         else:
    #             continue
    #             # raise ValueError("Unsupported input type. Must be a string, PIL Image, or list.")

    #     if len(payload) == 0:
    #         raise ValueError("No valid input data provided.")

    #     # Process the payload synchronously
    #     try:
    #         result = self.llm_agent.run_sync(payload)
    #         return result
    #     except Exception as e:
    #         print(f"Error parsing resume: {e}")
    #         print(traceback.format_exc())
    #         raise

    async def parse_async(
        self, input_data: list[Union[str, Image.Image, List[Any]]]
    ) -> BaseModel:
        """
        Parse resume data asynchronously from various input types.

        Args:
            input_data: Can be one of:
                - Path to a PDF file
                - Raw text string
                - PIL Image object
                - List containing text and/or images

        Returns:
            BaseModel: Parsed data as a Pydantic model
        """
        if not isinstance(input_data, list):
            input_data = [input_data]

        payload = []

        for item in input_data:
            # Handle different input types (same as sync version)
            if isinstance(item, str):
                if item.lower().endswith(".pdf"):
                    # For PDFs, convert each page to an image
                    page_images = self._render_pdf_pages_as_images(item)
                    payload.extend(page_images)
                else:
                    # For raw text, send as is
                    payload.append(item)

            elif isinstance(item, Image.Image):
                payload.append(item)

            elif isinstance(item, list):
                payload.extend(item)

            else:
                continue
                # raise ValueError("Unsupported input type. Must be a string, PIL Image, or list.")

        if len(payload) == 0:
            raise ValueError("No valid input data provided.")
        # Process the payload asynchronously
        try:
            result = await self.llm_agent.run(payload)
            return result  # type: ignore[return-value]
        except Exception as e:
            print(f"Error parsing resume asynchronously: {e}")
            print(traceback.format_exc())
            raise

    async def parse_batch_async(self, list_of_inputs: list) -> list:
        """
        Parse a batch of resumes asynchronously. Each input is processed as a separate resume.
        Args:
            list_of_inputs: List of inputs (each can be a string, image, or PDF path)
        Returns:
            List of parsed results (one per input)
        """
        batch_inputs = []
        for input_data in list_of_inputs:
            if not isinstance(input_data, list):
                input_data = [input_data]
            payload = []
            for item in input_data:
                if isinstance(item, str) and item.lower().endswith(".pdf"):
                    payload.extend(self._render_pdf_pages_as_images(item))
                else:
                    payload.append(item)
            batch_inputs.append((payload, self.result_type))
        results = await self.llm_agent.batch(batch_inputs)
        return results
