from __future__ import annotations

from typing import List, Optional
import re

from loguru import logger

from .parser import paginate_extract_paragraphs, iterate_page_paragraphs
from .embeddings import EmbeddingService
from .qdrant_store import QdrantStore
from qdrant_client.http.models import Distance


def ingest_document_to_qdrant(
    doc_id: str,
    start_url: str,
    collection_prefix: str = "docs_",
    recreate: bool = True,
    next_selector: Optional[str] = None,
    next_text: str = "Показать еще",
    headless: bool = True,
    max_pages: Optional[int] = None,
    content_selector: str = ".reader_article_body",
    # Article chunking
    article_regex: Optional[str] = r"^Статья\s+\d+[\.|\-]?",
    disable_article_grouping: bool = False,
    qdrant_url: Optional[str] = None,
    qdrant_host: Optional[str] = None,
    qdrant_port: Optional[int] = None,
    # qdrant_grpc_port: Optional[int] = None,
    qdrant_api_key: Optional[str] = None,
    embedding_model: str = "ai-forever/FRIDA",
) -> None:
    """Parse a document by pages, split to paragraphs and store chunks in a dedicated Qdrant collection.

    Each document goes to its own collection named f"{collection_prefix}{doc_id}".
    If recreate=True, the previous chunks for this doc are removed (by payload filter) before upsert.
    """

    # 1) Initialize embeddings and Qdrant
    embedder = EmbeddingService(model_name=embedding_model)
    store = QdrantStore(
        url=qdrant_url,
        host=qdrant_host,
        port=qdrant_port,
        api_key=qdrant_api_key,
        prefer_grpc=True,
        # grpc_port=qdrant_grpc_port,
    )
    collection_name = f"{collection_prefix}{doc_id}"
    if recreate:
        store.recreate_collection(collection_name, vector_size=embedder.dimension, distance=Distance.COSINE)
    else:
        store.ensure_collection(collection_name, vector_size=embedder.dimension, distance=Distance.COSINE)

    # 2) Stream per page: group and upload incrementally
    start_index = 0
    compiled = re.compile(article_regex) if (not disable_article_grouping and article_regex) else None
    any_uploaded = False
    for page_paras in iterate_page_paragraphs(
        start_url=start_url,
        max_pages=max_pages,
        next_selector=next_selector,
        next_text=next_text,
        headless=headless,
        content_selector=content_selector,
    ):
        if not page_paras:
            continue
        if compiled is not None:
            page_chunks, page_payloads = _group_paragraphs_into_articles_with_payload(page_paras, compiled)
            if not page_chunks:
                page_chunks = ["\n\n".join(page_paras)]
                page_payloads = [{}]
        else:
            page_chunks = ["\n\n".join(page_paras)]
            page_payloads = [{}]

        vectors = embedder.embed(page_chunks)
        store.upsert_chunks(
            collection_name,
            doc_id,
            page_chunks,
            vectors,
            extra_payload=page_payloads,
            start_index=start_index,
        )
        start_index += len(page_chunks)
        any_uploaded = True

    if not any_uploaded:
        logger.warning("Не удалось извлечь текст: пустой результат.")
    else:
        logger.info(f"Загружено чанков: {start_index} в коллекцию {collection_name}")


def _group_paragraphs_into_articles_with_payload(
    paragraphs: List[str], pattern: re.Pattern[str]
) -> tuple[List[str], List[dict]]:
    """Group paragraphs into article-sized chunks and derive payload metadata.

    Metadata per chunk:
    - article_number, article_title (split heading into number/title when possible)
    - chapter_number, chapter_title (if preceding heading paragraph matches chapter pattern)
    """
    chunks: List[str] = []
    payloads: List[dict] = []

    chapter_pat = re.compile(r"^Глава\s+(\d+)[\.:\-]?\s*(.*)$", re.IGNORECASE)
    article_num_pat = re.compile(r"^Статья\s+(\d+)[\.|\-]?\s*(.*)$", re.IGNORECASE)

    current: List[str] = []
    current_meta: dict = {}
    last_chapter_meta: dict = {}

    for para in paragraphs:
        ch_m = chapter_pat.match(para)
        if ch_m:
            # Update current chapter context
            last_chapter_meta = {
                "chapter_number": ch_m.group(1),
                "chapter_title": ch_m.group(2).strip() if ch_m.group(2) else "",
            }
            # Don't include chapter header into article text
            continue

        if pattern.match(para):
            # Flush previous article
            if current:
                chunks.append("\n\n".join(current))
                payloads.append(current_meta)
                current = []
                current_meta = {}

            # Start new article
            current.append(para)
            a_m = article_num_pat.match(para)
            article_number = a_m.group(1) if a_m else ""
            article_title = (a_m.group(2).strip() if a_m and a_m.group(2) else "")
            current_meta = {
                "article_number": article_number,
                "article_title": article_title,
                **last_chapter_meta,
                "type": "article",
            }
        else:
            if current:
                current.append(para)
            else:
                # Skip preface before the first article
                continue

    if current:
        chunks.append("\n\n".join(current))
        payloads.append(current_meta)

    return chunks, payloads


