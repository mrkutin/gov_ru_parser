from __future__ import annotations

from typing import List, Optional
from datetime import datetime, timezone
import re

from loguru import logger

from .parser import iterate_page_paragraphs, _trim_cross_page_overlap, _should_merge_cross_page
from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore, FastEmbedSparse, RetrievalMode
from langchain_huggingface import HuggingFaceEmbeddings
from qdrant_client.http.models import VectorParams, Distance, SparseVectorParams


def ingest_document_to_qdrant(
    doc_id: str,
    start_url: str,
    recreate: bool = True,
    next_selector: Optional[str] = ".show-more",
    next_text: Optional[str] = "Следующая",
    content_selector: str = ".reader_article_body",
    headless: bool = False,
    max_pages: Optional[int] = None,
    # Article chunking
    article_regex: Optional[str] = r"^Статья\s+\d+[\.|\-]?",
    disable_article_grouping: bool = False,
    qdrant_url: Optional[str] = None,
    qdrant_host: Optional[str] = None,
    qdrant_port: Optional[int] = None,
) -> None:
    """Parse a document by pages, split to paragraphs and store chunks in a dedicated Qdrant collection.

    Each document goes to its own collection named f"{collection_prefix}{doc_id}".
    If recreate=True, the previous chunks for this doc are removed (by payload filter) before upsert.
    """

    # 1) Initialize embeddings and Qdrant (LangChain vector store)
    dense_embeddings = HuggingFaceEmbeddings(model_name="ai-forever/FRIDA")
    sparse_embeddings = FastEmbedSparse(model_name="Qdrant/bm25")
    collection_name = f"{doc_id}"

    # Create Qdrant client
    if qdrant_url:
        client = QdrantClient(url=qdrant_url, prefer_grpc=True)
    elif qdrant_host:
        client = QdrantClient(host=qdrant_host, port=qdrant_port or 6333, prefer_grpc=True)
    else:
        client = QdrantClient(path=":memory:")

    def _create_collection_if_needed() -> None:
        try:
            dim = len(dense_embeddings.embed_query("test"))
        except Exception:
            dim = 768
        client.create_collection(
            collection_name=collection_name,
            vectors_config={
                "dense": VectorParams(size=dim, distance=Distance.COSINE),
            },
            sparse_vectors_config={
                "sparse": SparseVectorParams(),
            },
        )

    # Recreate collection if requested, otherwise ensure it exists
    if recreate:
        try:
            client.delete_collection(collection_name=collection_name)
        except Exception:
            pass
        _create_collection_if_needed()
    else:
        try:
            _ = client.get_collection(collection_name=collection_name)
        except Exception:
            _create_collection_if_needed()

    # Vector store instance (HYBRID mode with named vectors)
    vector_store = QdrantVectorStore(
        client=client,
        collection_name=collection_name,
        embedding=dense_embeddings,
        sparse_embedding=sparse_embeddings,
        retrieval_mode=RetrievalMode.HYBRID,
        vector_name="dense",
        sparse_vector_name="sparse",
        content_payload_key="page_content",
        metadata_payload_key="metadata",
    )

    # 2) Stream per page with cross-page seam merge
    start_index = 0
    compiled = re.compile(article_regex) if (not disable_article_grouping and article_regex) else None
    any_uploaded = False
    prev_paras: List[str] | None = None
    # Cross-page article aggregator state (when grouping enabled)
    chapter_pat = re.compile(r"^Глава\s+(\d+)[\.:\-]?\s*(.*)$", re.IGNORECASE)
    article_num_pat = re.compile(r"^Статья\s+(\d+)[\.|\-]?\s*(.*)$", re.IGNORECASE)
    last_chapter_meta: dict = {}
    current_article_paras: List[str] = []
    current_article_meta: dict | None = None

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

        if prev_paras is None:
            prev_paras = list(page_paras)
            continue

        # Merge seam between prev_paras tail and current head
        if prev_paras and page_paras:
            trimmed_head = _trim_cross_page_overlap(prev_paras[-1], page_paras[0])
            if _should_merge_cross_page(prev_paras[-1], trimmed_head):
                if prev_paras[-1].endswith("-"):
                    prev_paras[-1] = prev_paras[-1][:-1] + trimmed_head.lstrip()
                else:
                    tail = prev_paras[-1].rstrip()
                    head = trimmed_head.lstrip()
                    if tail and head and tail[-1].isalpha() and head[0].isalpha():
                        prev_paras[-1] = tail + head
                    else:
                        prev_paras[-1] = tail + " " + head
                page_paras = page_paras[1:]
            else:
                page_paras[0] = trimmed_head
                if not page_paras[0].strip():
                    page_paras = page_paras[1:]

        # Upsert finalized articles/chunks from previous page
        if prev_paras:
            if compiled is not None:
                # Stream paragraphs through the cross-page aggregator
                finished_chunks: List[str] = []
                finished_payloads: List[dict] = []
                for para in prev_paras:
                    ch_m = chapter_pat.match(para)
                    if ch_m:
                        # Update chapter context for subsequent articles
                        last_chapter_meta = {
                            "chapter_number": ch_m.group(1),
                            "chapter_title": (ch_m.group(2).strip() if ch_m.group(2) else ""),
                        }
                        continue
                    if compiled.match(para):
                        # Flush previous article if exists
                        if current_article_paras:
                            finished_chunks.append("\n\n".join(current_article_paras))
                            finished_payloads.append(current_article_meta or {})
                            current_article_paras = []
                            current_article_meta = None
                        # Start new article
                        current_article_paras = [para]
                        a_m = article_num_pat.match(para)
                        article_number = a_m.group(1) if a_m else ""
                        article_title = (a_m.group(2).strip() if a_m and a_m.group(2) else "")
                        current_article_meta = {
                            "article_number": article_number,
                            "article_title": article_title,
                            **last_chapter_meta,
                        }
                    else:
                        if current_article_paras:
                            current_article_paras.append(para)
                        else:
                            # Skip preface before the first article
                            continue

                # Upsert any finished chunks from this page
                if finished_chunks:
                    now_str = datetime.now().astimezone().isoformat(timespec='seconds')
                    # Deduplicate by article identity; keep the longest text per key
                    dedup_map: dict[tuple, tuple[str, dict]] = {}
                    order: List[tuple] = []
                    for idx in range(len(finished_chunks)):
                        text = finished_chunks[idx]
                        meta = finished_payloads[idx] if idx < len(finished_payloads) else {}
                        key = (
                            meta.get("chapter_number"),
                            meta.get("chapter_title"),
                            meta.get("article_number"),
                            meta.get("article_title"),
                        )
                        if key not in dedup_map:
                            dedup_map[key] = (text, meta)
                            order.append(key)
                        else:
                            if len(text) > len(dedup_map[key][0]):
                                dedup_map[key] = (text, meta)
                    texts_out: List[str] = []
                    metas_out: List[dict] = []
                    for key in order:
                        t, m = dedup_map[key]
                        texts_out.append(t)
                        metas_out.append(m)
                    metadatas: List[dict] = []
                    ids: List[int] = []
                    for idx in range(len(texts_out)):
                        metadatas.append({**metas_out[idx], "upload_time": now_str})
                        ids.append(start_index + idx)
                    vector_store.add_texts(texts=texts_out, metadatas=metadatas, ids=ids)
                    start_index += len(texts_out)
                    any_uploaded = True
            else:
                # Grouping disabled: upsert whole page as a single chunk
                page_chunks = ["\n\n".join(prev_paras)]
                page_payloads = [{}]
                now_str = datetime.now().astimezone().isoformat(timespec='seconds')
                metadatas: List[dict] = []
                ids: List[int] = []
                for idx in range(len(page_chunks)):
                    extra = page_payloads[idx] if page_payloads and idx < len(page_payloads) else {}
                    metadatas.append({**extra, "upload_time": now_str})
                    ids.append(start_index + idx)
                vector_store.add_texts(texts=page_chunks, metadatas=metadatas, ids=ids)
                start_index += len(page_chunks)
                any_uploaded = True

        # Move buffer to current page (post-merge)
        prev_paras = list(page_paras)

    # Flush last buffered page
    if prev_paras:
        if compiled is not None:
            # Process remaining paragraphs and flush the last open article
            finished_chunks: List[str] = []
            finished_payloads: List[dict] = []
            for para in prev_paras:
                ch_m = chapter_pat.match(para)
                if ch_m:
                    last_chapter_meta = {
                        "chapter_number": ch_m.group(1),
                        "chapter_title": (ch_m.group(2).strip() if ch_m.group(2) else ""),
                    }
                    continue
                if compiled.match(para):
                    if current_article_paras:
                        finished_chunks.append("\n\n".join(current_article_paras))
                        finished_payloads.append(current_article_meta or {})
                        current_article_paras = []
                        current_article_meta = None
                    current_article_paras = [para]
                    a_m = article_num_pat.match(para)
                    article_number = a_m.group(1) if a_m else ""
                    article_title = (a_m.group(2).strip() if a_m and a_m.group(2) else "")
                    current_article_meta = {
                        "article_number": article_number,
                        "article_title": article_title,
                        **last_chapter_meta,
                    }
                else:
                    if current_article_paras:
                        current_article_paras.append(para)
                    else:
                        continue
            # Flush the last open article
            if current_article_paras:
                finished_chunks.append("\n\n".join(current_article_paras))
                finished_payloads.append(current_article_meta or {})
                current_article_paras = []
                current_article_meta = None

            if finished_chunks:
                now_str = datetime.now().astimezone().isoformat(timespec='seconds')
                # Deduplicate by article identity; keep the longest text per key
                dedup_map: dict[tuple, tuple[str, dict]] = {}
                order: List[tuple] = []
                for idx in range(len(finished_chunks)):
                    text = finished_chunks[idx]
                    meta = finished_payloads[idx] if idx < len(finished_payloads) else {}
                    key = (
                        meta.get("chapter_number"),
                        meta.get("chapter_title"),
                        meta.get("article_number"),
                        meta.get("article_title"),
                    )
                    if key not in dedup_map:
                        dedup_map[key] = (text, meta)
                        order.append(key)
                    else:
                        if len(text) > len(dedup_map[key][0]):
                            dedup_map[key] = (text, meta)
                texts_out: List[str] = []
                metas_out: List[dict] = []
                for key in order:
                    t, m = dedup_map[key]
                    texts_out.append(t)
                    metas_out.append(m)
                metadatas: List[dict] = []
                ids: List[int] = []
                for idx in range(len(texts_out)):
                    metadatas.append({**metas_out[idx], "upload_time": now_str})
                    ids.append(start_index + idx)
                vector_store.add_texts(texts=texts_out, metadatas=metadatas, ids=ids)
                start_index += len(texts_out)
                any_uploaded = True
        else:
            page_chunks = ["\n\n".join(prev_paras)]
            page_payloads = [{}]
            now_str = datetime.now().astimezone().isoformat(timespec='seconds')
            metadatas: List[dict] = []
            ids: List[int] = []
            for idx in range(len(page_chunks)):
                extra = page_payloads[idx] if page_payloads and idx < len(page_payloads) else {}
                metadatas.append({**extra, "upload_time": now_str})
                ids.append(start_index + idx)
            vector_store.add_texts(texts=page_chunks, metadatas=metadatas, ids=ids)
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
                **last_chapter_meta
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


