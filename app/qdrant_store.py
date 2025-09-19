from __future__ import annotations

from typing import Iterable, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    VectorParams,
    Distance,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
)


class QdrantStore:
    def __init__(
        self,
        url: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        api_key: Optional[str] = None,
        prefer_grpc: bool = False,
        # grpc_port: Optional[int] = None,
    ) -> None:
        if url:
            self.client = QdrantClient(url=url, api_key=api_key, prefer_grpc=prefer_grpc)
        elif host:
            self.client = QdrantClient(
                host=host,
                port=port or 6333,
                # grpc_port=grpc_port or 6334,
                api_key=api_key,
                prefer_grpc=prefer_grpc,
            )
        else:
            # In-memory for local/dev if nothing provided
            self.client = QdrantClient(path=":memory:")

    def ensure_collection(self, collection_name: str, vector_size: int, distance: Distance = Distance.COSINE) -> None:
        exists = False
        try:
            _ = self.client.get_collection(collection_name=collection_name)
            exists = True
        except Exception:
            exists = False
        if not exists:
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=vector_size, distance=distance),
            )

    def recreate_collection(self, collection_name: str, vector_size: int, distance: Distance = Distance.COSINE) -> None:
        try:
            self.client.delete_collection(collection_name=collection_name)
        except Exception:
            # Ignore if not exists
            pass
        self.client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_size, distance=distance),
        )

    def delete_doc(self, collection_name: str, doc_id: str) -> None:
        self.client.delete(
            collection_name=collection_name,
            points_selector=Filter(must=[FieldCondition(key="doc_id", match=MatchValue(value=doc_id))]),
        )

    def upsert_chunks(
        self,
        collection_name: str,
        doc_id: str,
        chunks: Iterable[str],
        vectors: List[List[float]],
        extra_payload: Optional[List[dict]] = None,
        start_index: int = 0,
    ) -> None:
        points: List[PointStruct] = []
        for idx, (text, vector) in enumerate(zip(chunks, vectors)):
            points.append(
                PointStruct(
                    id=start_index + idx,
                    vector=vector,
                    payload={
                        "doc_id": doc_id,
                        "chunk_index": start_index + idx,
                        "text": text,
                        **(extra_payload[idx] if extra_payload and idx < len(extra_payload) else {}),
                    },
                )
            )
        self.client.upsert(collection_name=collection_name, points=points)


