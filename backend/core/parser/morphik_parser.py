import os
import tempfile
import subprocess
import shutil
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableStructureOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from httpx import AsyncClient, Timeout

from core.config import get_settings
from core.models.chunk import Chunk
from core.parser.base_parser import BaseParser
from core.parser.video.parse_video import VideoParser, load_config
from core.parser.xml_chunker import XMLChunker
from core.storage.utils_file_extensions import detect_content_type

# Custom RecursiveCharacterTextSplitter replaces langchain's version


logger = logging.getLogger(__name__)


class BaseChunker(ABC):
    """Base class for text chunking strategies"""

    @abstractmethod
    def split_text(self, text: str) -> List[Chunk]:
        """Split text into chunks"""
        pass


class StandardChunker(BaseChunker):
    """Standard chunking using langchain's RecursiveCharacterTextSplitter"""

    def __init__(self, chunk_size: int, chunk_overlap: int):
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ". ", " ", ""],
        )

    def split_text(self, text: str) -> List[Chunk]:
        return self.text_splitter.split_text(text)


class RecursiveCharacterTextSplitter:
    def __init__(self, chunk_size: int, chunk_overlap: int, length_function=len, separators=None):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.length_function = length_function
        self.separators = separators or ["\n\n", "\n", ". ", " ", ""]

    def split_text(self, text: str) -> list[str]:
        chunks = self._split_recursive(text, self.separators)
        return [Chunk(content=chunk, metadata={}) for chunk in chunks]

    def _split_recursive(self, text: str, separators: list[str]) -> list[str]:
        if self.length_function(text) <= self.chunk_size:
            return [text] if text else []
        if not separators:
            # No separators left, split at chunk_size boundaries
            return [text[i : i + self.chunk_size] for i in range(0, len(text), self.chunk_size)]
        sep = separators[0]
        if sep:
            splits = text.split(sep)
        else:
            # Last fallback: split every character
            splits = list(text)
        chunks = []
        current = ""
        for part in splits:
            add_part = part + (sep if sep and part != splits[-1] else "")
            if self.length_function(current + add_part) > self.chunk_size:
                if current:
                    chunks.append(current)
                current = add_part
            else:
                current += add_part
        if current:
            chunks.append(current)
        # If any chunk is too large, recurse further
        final_chunks = []
        for chunk in chunks:
            if self.length_function(chunk) > self.chunk_size and len(separators) > 1:
                final_chunks.extend(self._split_recursive(chunk, separators[1:]))
            else:
                final_chunks.append(chunk)
        # Handle overlap
        if self.chunk_overlap > 0 and len(final_chunks) > 1:
            overlapped = []
            for i in range(len(final_chunks)):
                chunk = final_chunks[i]
                if i > 0:
                    prev = final_chunks[i - 1]
                    overlap = prev[-self.chunk_overlap :]
                    chunk = overlap + chunk
                overlapped.append(chunk)
            return overlapped
        return final_chunks


class ContextualChunker(BaseChunker):
    """Contextual chunking using LLMs to add context to each chunk"""

    DOCUMENT_CONTEXT_PROMPT = """
    <document>
    {doc_content}
    </document>
    """

    CHUNK_CONTEXT_PROMPT = """
    Here is the chunk we want to situate within the whole document
    <chunk>
    {chunk_content}
    </chunk>

    Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk.
    Answer only with the succinct context and nothing else.
    """

    def __init__(self, chunk_size: int, chunk_overlap: int, anthropic_api_key: str):
        self.standard_chunker = StandardChunker(chunk_size, chunk_overlap)

        # Get the config for contextual chunking
        config = load_config()
        parser_config = config.get("parser", {})
        self.model_key = parser_config.get("contextual_chunking_model", "claude_sonnet")

        # Get the settings for registered models
        from core.config import get_settings

        self.settings = get_settings()

        # Make sure the model exists in registered_models
        if not hasattr(self.settings, "REGISTERED_MODELS") or self.model_key not in self.settings.REGISTERED_MODELS:
            raise ValueError(f"Model '{self.model_key}' not found in registered_models configuration")

        self.model_config = self.settings.REGISTERED_MODELS[self.model_key]
        logger.info(f"Initialized ContextualChunker with model_key={self.model_key}")

    def _situate_context(self, doc: str, chunk: str) -> str:
        import litellm

        # Extract model name from config
        model_name = self.model_config.get("model_name")

        # Create system and user messages
        system_message = {
            "role": "system",
            "content": "You are an AI assistant that situates a chunk within a document for the purposes of improving search retrieval of the chunk.",
        }

        # Add document context and chunk to user message
        user_message = {
            "role": "user",
            "content": f"{self.DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc)}\n\n{self.CHUNK_CONTEXT_PROMPT.format(chunk_content=chunk)}",
        }

        # Prepare parameters for litellm
        model_params = {
            "model": model_name,
            "messages": [system_message, user_message],
            "max_tokens": 1024,
            "temperature": 0.0,
        }

        # Add all model-specific parameters from the config
        for key, value in self.model_config.items():
            if key != "model_name":
                model_params[key] = value

        # Use litellm for completion
        response = litellm.completion(**model_params)
        return response.choices[0].message.content

    def split_text(self, text: str) -> List[Chunk]:
        base_chunks = self.standard_chunker.split_text(text)
        contextualized_chunks = []

        for chunk in base_chunks:
            context = self._situate_context(text, chunk.content)
            content = f"{context}; {chunk.content}"
            contextualized_chunks.append(Chunk(content=content, metadata=chunk.metadata))

        return contextualized_chunks


class MorphikParser(BaseParser):
    """Unified parser that handles different file types and chunking strategies"""

    # Docling converter is expensive to initialize, so we cache it at class level
    _docling_converter: Optional[DocumentConverter] = None

    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        assemblyai_api_key: Optional[str] = None,
        anthropic_api_key: Optional[str] = None,
        frame_sample_rate: int = 1,
        use_contextual_chunking: bool = False,
    ):
        # Initialize basic configuration
        self._assemblyai_api_key = assemblyai_api_key
        self._anthropic_api_key = anthropic_api_key
        self.frame_sample_rate = frame_sample_rate

        # Get settings from config
        self.settings = get_settings()

        # Initialize chunker based on configuration
        if use_contextual_chunking:
            self.chunker = ContextualChunker(chunk_size, chunk_overlap, anthropic_api_key)
        else:
            self.chunker = StandardChunker(chunk_size, chunk_overlap)

        # Initialize logger
        self.logger = logging.getLogger(__name__)

        # Setup for API mode parsing
        self._parse_api_endpoints: Optional[List[str]] = None
        self._parse_api_key: Optional[str] = None
        if getattr(self.settings, "PARSER_MODE", "local") == "api":
            if self.settings.MORPHIK_EMBEDDING_API_DOMAIN:
                self._parse_api_endpoints = [
                    f"{ep.rstrip('/')}/parse" for ep in self.settings.MORPHIK_EMBEDDING_API_DOMAIN
                ]
                self._parse_api_key = self.settings.MORPHIK_EMBEDDING_API_KEY
                self.logger.info(f"Parser API mode enabled with {len(self._parse_api_endpoints)} endpoint(s)")

    @classmethod
    def _get_docling_converter(cls) -> DocumentConverter:
        """Get or create the cached Docling converter."""
        if cls._docling_converter is None:
            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_ocr = True
            try:
                import easyocr  # noqa: F401
                from docling.datamodel.pipeline_options import EasyOcrOptions
                pipeline_options.ocr_options = EasyOcrOptions(lang=["en"])
            except ImportError:
                pass  # Use Docling's default OCR if EasyOCR is unavailable
            pipeline_options.do_table_structure = True
            pipeline_options.table_structure_options = TableStructureOptions(mode="accurate")
            pipeline_options.images_scale = 2.0
            pipeline_options.generate_picture_images = True

            cls._docling_converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
                }
            )
        return cls._docling_converter

    def _is_video_file(self, file: bytes, filename: str) -> bool:
        """Check if the file is a video file."""
        try:
            mime_type = detect_content_type(content=file, filename=filename)
            return mime_type.startswith("video/")
        except Exception as e:
            logging.error(f"Error detecting file type: {str(e)}")
            return filename.lower().endswith(".mp4")

    def _is_xml_file(self, filename: str, content_type: Optional[str] = None) -> bool:
        """Check if the file is an XML file."""
        if filename and filename.lower().endswith(".xml"):
            return True
        if content_type and content_type in ["application/xml", "text/xml"]:
            return True
        return False

    def _is_plain_text_file(self, filename: str) -> bool:
        """Check if the file is a plain text file that should be read directly without partitioning."""
        plain_text_extensions = {".txt", ".md", ".markdown", ".json", ".csv", ".tsv", ".log", ".rst", ".yaml", ".yml"}
        lower_filename = filename.lower()
        return any(lower_filename.endswith(ext) for ext in plain_text_extensions)

    async def _parse_video(self, file: bytes) -> Tuple[Dict[str, Any], str]:
        """Parse video file to extract transcript and frame descriptions"""
        if not self._assemblyai_api_key:
            raise ValueError("AssemblyAI API key is required for video parsing")

        # Save video to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_file:
            temp_file.write(file)
            video_path = temp_file.name

        try:
            # Load the config to get the frame_sample_rate from morphik.toml
            config = load_config()
            parser_config = config.get("parser", {})
            vision_config = parser_config.get("vision", {})
            frame_sample_rate = vision_config.get("frame_sample_rate", self.frame_sample_rate)

            # Process video
            parser = VideoParser(
                video_path,
                assemblyai_api_key=self._assemblyai_api_key,
                frame_sample_rate=frame_sample_rate,
            )
            results = await parser.process_video()

            # Combine frame descriptions and transcript
            frame_text = "\n".join(results.frame_descriptions.time_to_content.values())
            transcript_text = "\n".join(results.transcript.time_to_content.values())
            combined_text = f"Frame Descriptions:\n{frame_text}\n\nTranscript:\n{transcript_text}"

            metadata = {
                "video_metadata": results.metadata,
                "frame_timestamps": list(results.frame_descriptions.time_to_content.keys()),
                "transcript_timestamps": list(results.transcript.time_to_content.keys()),
            }

            return metadata, combined_text
        finally:
            os.unlink(video_path)

    async def _parse_xml(self, file: bytes, filename: str) -> Tuple[List[Chunk], int]:
        """Parse XML file directly using XMLChunker."""
        self.logger.info(f"Processing '{filename}' with dedicated XML chunker.")

        # Get XML parser configuration
        xml_config = {}
        if self.settings and hasattr(self.settings, "PARSER_XML"):
            xml_config = self.settings.PARSER_XML.model_dump()

        # Use XMLChunker to process the XML
        xml_chunker = XMLChunker(content=file, config=xml_config)
        xml_chunks_data = xml_chunker.chunk()

        # Map to Chunk objects
        chunks = []
        for i, chunk_data in enumerate(xml_chunks_data):
            metadata = {
                "unit": chunk_data.get("unit"),
                "xml_id": chunk_data.get("xml_id"),
                "breadcrumbs": chunk_data.get("breadcrumbs"),
                "source_path": chunk_data.get("source_path"),
                "prev_chunk_xml_id": chunk_data.get("prev"),
                "next_chunk_xml_id": chunk_data.get("next"),
            }
            chunks.append(Chunk(content=chunk_data["text"], metadata=metadata))

        return chunks, len(file)

    async def _parse_document_via_api(self, file: bytes, filename: str) -> str:
        """Parse document via remote API (GPU server)."""
        if not self._parse_api_endpoints or not self._parse_api_key:
            raise RuntimeError("Parser API not configured")

        headers = {"Authorization": f"Bearer {self._parse_api_key}"}
        timeout = Timeout(read=300.0, connect=30.0, write=60.0, pool=30.0)

        last_error: Optional[Exception] = None
        for endpoint in self._parse_api_endpoints:
            try:
                async with AsyncClient(timeout=timeout) as client:
                    files = {"file": (filename, file)}
                    data = {"filename": filename}
                    resp = await client.post(endpoint, files=files, data=data, headers=headers)
                    resp.raise_for_status()
                    result = resp.json()
                    return result.get("text", "")
            except Exception as e:
                self.logger.warning(f"Parse API call to {endpoint} failed: {e}")
                last_error = e
                continue

        raise RuntimeError(f"All parse API endpoints failed. Last error: {last_error}")

    async def _parse_with_gemini_vision(self, file: bytes, filename: str) -> str:
        """Extract text from a PDF using Gemini vision, page by page via pymupdf rendering."""
        import asyncio
        import base64
        import litellm

        model_key = getattr(self.settings, "DOCUMENT_ANALYSIS_MODEL", None)
        if not model_key:
            self.logger.warning("DOCUMENT_ANALYSIS_MODEL not configured; skipping vision fallback")
            return ""
        if not hasattr(self.settings, "REGISTERED_MODELS") or model_key not in self.settings.REGISTERED_MODELS:
            self.logger.warning(f"Document analysis model '{model_key}' not in registered_models; skipping vision")
            return ""

        model_config = self.settings.REGISTERED_MODELS[model_key]
        model_name = model_config.get("model_name", "")

        try:
            import fitz  # pymupdf
        except ImportError:
            self.logger.warning("pymupdf not installed; cannot use vision fallback")
            return ""

        suffix = os.path.splitext(filename)[1].lower() or ".pdf"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(file)
            tmp_path = tmp.name

        all_pages_text: list[str] = []
        try:
            def render_pages():
                doc = fitz.open(tmp_path)
                pages_data = []
                for page in doc:
                    pix = page.get_pixmap(dpi=150)
                    pages_data.append(pix.tobytes("png"))
                doc.close()
                return pages_data

            pages_png = await asyncio.to_thread(render_pages)
            self.logger.info(f"Vision fallback: processing {len(pages_png)} pages with {model_name}")

            for i, png_bytes in enumerate(pages_png):
                b64 = base64.standard_b64encode(png_bytes).decode("utf-8")
                messages = [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": (
                                    "Extract ALL text content from this document page, "
                                    "including every table. Render tables as markdown tables. "
                                    "Preserve headings, sections, and all data values. "
                                    "Output only the extracted content."
                                ),
                            },
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{b64}"},
                            },
                        ],
                    }
                ]
                model_params: dict = {"model": model_name, "messages": messages}
                for key, value in model_config.items():
                    if key != "model_name":
                        model_params[key] = value

                try:
                    resp = await litellm.acompletion(**model_params)
                    page_text = resp.choices[0].message.content or ""
                    if page_text.strip():
                        all_pages_text.append(f"<!-- page {i + 1} -->\n{page_text.strip()}")
                except Exception as page_err:
                    self.logger.warning(f"Vision extraction failed for page {i + 1}: {page_err}")
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        return "\n\n".join(all_pages_text)

    async def _parse_document_local(self, file: bytes, filename: str) -> str:
        """Parse document using local Docling, with Gemini vision fallback for PDFs with poor extraction."""
        import subprocess
        import shutil
        import asyncio

        suffix = os.path.splitext(filename)[1].lower() or ".pdf"
        
        # Convert legacy .doc files to .docx before passing to Docling
        if suffix == ".doc":
            if shutil.which("soffice"):
                with tempfile.NamedTemporaryFile(delete=False, suffix=".doc") as doc_temp:
                    doc_temp.write(file)
                    doc_temp_path = doc_temp.name
                
                try:
                    output_dir = os.path.dirname(doc_temp_path)
                    base_filename = os.path.splitext(os.path.basename(doc_temp_path))[0]
                    expected_docx_path = os.path.join(output_dir, f"{base_filename}.docx")
                    
                    self.logger.info(f"Converting legacy .doc file {filename} to .docx")
                    
                    def run_conversion():
                        return subprocess.run(
                            [
                                "soffice",
                                "--headless",
                                "--convert-to",
                                "docx",
                                "--outdir",
                                output_dir,
                                doc_temp_path,
                            ],
                            capture_output=True,
                            text=True,
                            timeout=60,
                        )
                    
                    result = await asyncio.to_thread(run_conversion)
                    
                    if result.returncode == 0 and os.path.exists(expected_docx_path):
                        with open(expected_docx_path, "rb") as docx_file:
                            file = docx_file.read()
                        suffix = ".docx"
                        os.unlink(expected_docx_path)
                        self.logger.info(f"Successfully converted {filename} to .docx for parsing")
                    else:
                        self.logger.warning(f"LibreOffice conversion failed for {filename}: {result.stderr}")
                except Exception as convert_exc:
                    self.logger.warning(f"Error during .doc to .docx conversion for {filename}: {convert_exc}")
                finally:
                    if os.path.exists(doc_temp_path):
                        os.unlink(doc_temp_path)
            else:
                self.logger.warning(f"soffice not found, cannot convert legacy .doc file {filename}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            temp_file.write(file)
            temp_path = temp_file.name

        try:
            # Run Docling conversion in a thread to avoid blocking the event loop
            import asyncio
            converter = self._get_docling_converter()

            def run_docling():
                return converter.convert(temp_path)

            result = await asyncio.to_thread(run_docling)
            text = result.document.export_to_markdown()

            if not text.strip():
                self.logger.warning(f"Docling returned no text for {filename}")

            # Vision fallback for PDFs: if Docling extracted very little text relative to file
            # size (< 50 chars/KB), the document likely contains image-based tables or complex
            # layouts that Docling can't parse — use Gemini vision to extract the content.
            is_pdf = suffix == ".pdf"
            file_kb = len(file) / 1024
            chars_per_kb = len(text.strip()) / max(file_kb, 1)
            if is_pdf and chars_per_kb < 50:
                self.logger.info(
                    f"Docling yielded only {chars_per_kb:.1f} chars/KB for {filename}; "
                    "trying Gemini vision fallback"
                )
                vision_text = await self._parse_with_gemini_vision(file, filename)
                if vision_text.strip():
                    self.logger.info(f"Vision fallback extracted {len(vision_text)} chars from {filename}")
                    return vision_text
                self.logger.warning(f"Vision fallback returned no text for {filename}; using Docling output")

            return text
        except Exception as e:
            self.logger.error(f"Docling parsing failed for {filename}: {e}")
            return ""
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass

    async def _parse_document(self, file: bytes, filename: str) -> Tuple[Dict[str, Any], str]:
        """Parse document using Docling (local or API), or read directly for plain text files."""
        # For plain text files, read directly without parsing
        if self._is_plain_text_file(filename):
            try:
                text = file.decode("utf-8")
            except UnicodeDecodeError:
                text = file.decode("latin-1")
            return {}, text

        # For complex formats, use API if configured, otherwise local Docling
        if self._parse_api_endpoints:
            try:
                text = await self._parse_document_via_api(file, filename)
                return {}, text
            except Exception as e:
                self.logger.warning(f"API parsing failed, falling back to local: {e}")
                text = await self._parse_document_local(file, filename)
                return {}, text
        else:
            text = await self._parse_document_local(file, filename)
            return {}, text

    async def parse_file_to_text(self, file: bytes, filename: str) -> Tuple[Dict[str, Any], str]:
        """Parse file content into text based on file type"""
        if self._is_video_file(file, filename):
            return await self._parse_video(file)
        elif self._is_xml_file(filename):
            # For XML files, we'll handle parsing and chunking together
            # This method should not be called for XML files in normal flow
            # Return empty to indicate XML files should use parse_and_chunk_xml
            return {}, ""
        return await self._parse_document(file, filename)

    async def parse_and_chunk_xml(self, file: bytes, filename: str) -> List[Chunk]:
        """Parse and chunk XML files in one step."""
        chunks, _ = await self._parse_xml(file, filename)
        return chunks

    def is_xml_file(self, filename: str, content_type: Optional[str] = None) -> bool:
        """Public method to check if file is XML."""
        return self._is_xml_file(filename, content_type)

    async def split_text(self, text: str) -> List[Chunk]:
        """Split text into chunks using configured chunking strategy"""
        return self.chunker.split_text(text)
