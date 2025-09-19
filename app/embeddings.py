from __future__ import annotations

from typing import Iterable, List

from sentence_transformers import SentenceTransformer


class EmbeddingService:
    """Thin wrapper around SentenceTransformer for batch embeddings.

    Default model targets multilingual/Russian texts. Replace with FRIDA model
    when its exact Hugging Face ID is confirmed.
    """

    def __init__(self, model_name: str = "ai-forever/FRIDA"):
        # FRIDA recommends CLS pooling and supports prompts via SentenceTransformers
        self.model = SentenceTransformer(model_name)
        self._dim = self.model.get_sentence_embedding_dimension()

    @property
    def dimension(self) -> int:
        return self._dim

    def embed(self, texts: Iterable[str]) -> List[List[float]]:
        # Use search_document prompt for document/paragraph embeddings per model card
        arr = list(texts)
        return self.model.encode(arr, normalize_embeddings=True, prompt_name="search_document").tolist()


