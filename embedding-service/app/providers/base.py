"""Base provider interface for embedding generation."""

from abc import ABC, abstractmethod
from typing import List


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers."""

    @abstractmethod
    async def embed(self, texts: List[str], model_name: str) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.

        Args:
            texts: List of text strings to embed
            model_name: Name of the model to use

        Returns:
            List of embedding vectors (one per input text)

        Raises:
            ValueError: If texts is empty or model_name is invalid
            RuntimeError: If embedding generation fails
        """
        pass

    @abstractmethod
    async def get_embedding_dimension(self, model_name: str) -> int:
        """
        Get the dimension of embeddings for a given model.

        Args:
            model_name: Name of the model

        Returns:
            Embedding dimension (e.g., 384, 768, 1536)
        """
        pass

    @abstractmethod
    async def is_available(self) -> bool:
        """
        Check if the provider is available and ready.

        Returns:
            True if provider is ready, False otherwise
        """
        pass

    @abstractmethod
    def get_supported_models(self) -> List[str]:
        """
        Get list of supported model names.

        Returns:
            List of model names supported by this provider
        """
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """Shutdown provider resources."""
        pass
