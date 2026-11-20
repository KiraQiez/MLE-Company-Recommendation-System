"""Shared CPU embedding model for ingestion and backfills."""

from collections.abc import Iterator, Sequence

from fastembed import TextEmbedding

MODEL_NAME = "BAAI/bge-small-en-v1.5"


def embedding_text(description: str | None, industry: str | None) -> str | None:
    """Prefer the description and fall back to industry if it has usable text."""
    for text in (description, industry):
        if text and any(character.isalnum() for character in text):
            return text
    return None


class CompanyEmbedder:
    """Load the cached FastEmbed model once and format its output for pgvector."""

    def __init__(self) -> None:
        # FastEmbed downloads the weights into its local cache when they are missing.
        self.model = TextEmbedding(MODEL_NAME, threads=2, providers=["CPUExecutionProvider"])

    def embed_many(self, texts: Sequence[str]) -> Iterator[str]:
        for vector in self.model.embed(texts, batch_size=64):
            yield "[" + ",".join(map(str, vector.tolist())) + "]"

    def __call__(self, text: str) -> str:
        return next(self.embed_many([text]))
