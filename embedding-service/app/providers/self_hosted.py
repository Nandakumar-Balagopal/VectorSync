"""Self-hosted embedding provider using sentence-transformers."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
import structlog
from sentence_transformers import SentenceTransformer

from app.providers.base import EmbeddingProvider

logger = structlog.get_logger()


class SelfHostedProvider(EmbeddingProvider):
    """
    Self-hosted embedding provider using sentence-transformers.
    
    Uses a thread pool executor to avoid blocking the event loop
    since sentence-transformers is CPU/GPU bound.
    """

    def __init__(
        self,
        default_model: str = "all-MiniLM-L6-v2",
        max_workers: int = 4,
        device: str = "cpu",
    ):
        """
        Initialize the self-hosted provider.

        Args:
            default_model: Default model to load
            max_workers: Maximum number of worker threads
            device: Device to use ('cpu', 'cuda', 'mps')
        """
        self.default_model = default_model
        self.device = device
        self.max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._models: Dict[str, SentenceTransformer] = {}
        self._model_dimensions: Dict[str, int] = {}
        
        logger.info(
            "initializing_self_hosted_provider",
            default_model=default_model,
            device=device,
            max_workers=max_workers,
        )

    def _load_model(self, model_name: str) -> SentenceTransformer:
        """
        Load a model (runs in thread pool).

        Args:
            model_name: Name of the model to load

        Returns:
            Loaded SentenceTransformer model
        """
        if model_name not in self._models:
            logger.info("loading_model", model_name=model_name, device=self.device)
            model = SentenceTransformer(model_name, device=self.device)
            dimension = model.get_sentence_embedding_dimension()
            if dimension is None:
                raise RuntimeError(f"Failed to determine embedding dimension for model '{model_name}'")
            self._models[model_name] = model
            self._model_dimensions[model_name] = dimension
            logger.info(
                "model_loaded",
                model_name=model_name,
                dimension=self._model_dimensions[model_name],
            )
        return self._models[model_name]

    def _encode_batch(self, texts: List[str], model_name: str) -> List[List[float]]:
        """
        Encode texts using the model (runs in thread pool).

        Args:
            texts: List of texts to encode
            model_name: Model to use

        Returns:
            List of embeddings
        """
        model = self._load_model(model_name)
        
        # Convert to list of floats for JSON serialization
        embeddings = model.encode(
            texts,
            convert_to_numpy=True,
            show_progress_bar=False,
            batch_size=32,  # Internal batching for efficiency
        )
        
        return [embedding.tolist() for embedding in embeddings]

    async def embed(self, texts: List[str], model_name: str) -> List[List[float]]:
        """
        Generate embeddings for texts asynchronously.

        Args:
            texts: List of texts to embed
            model_name: Model name to use

        Returns:
            List of embedding vectors

        Raises:
            ValueError: If texts is empty
            RuntimeError: If encoding fails
        """
        if not texts:
            raise ValueError("texts cannot be empty")

        logger.debug(
            "embedding_batch",
            num_texts=len(texts),
            model_name=model_name,
        )

        try:
            # Run encoding in thread pool to avoid blocking event loop
            loop = asyncio.get_event_loop()
            embeddings = await loop.run_in_executor(
                self._executor,
                self._encode_batch,
                texts,
                model_name,
            )
            
            logger.debug(
                "embedding_complete",
                num_embeddings=len(embeddings),
                dimension=len(embeddings[0]) if embeddings else 0,
            )
            
            return embeddings

        except Exception as e:
            logger.error(
                "embedding_failed",
                error=str(e),
                model_name=model_name,
                num_texts=len(texts),
            )
            raise RuntimeError(f"Failed to generate embeddings: {str(e)}") from e

    async def get_embedding_dimension(self, model_name: str) -> int:
        """
        Get embedding dimension for a model.

        Args:
            model_name: Model name

        Returns:
            Embedding dimension
        """
        if model_name not in self._model_dimensions:
            # Load model to get dimension
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self._executor,
                self._load_model,
                model_name,
            )
        
        return self._model_dimensions[model_name]

    async def is_available(self) -> bool:
        """
        Check if provider is available.

        Returns:
            True if available
        """
        try:
            # Try to load default model
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self._executor,
                self._load_model,
                self.default_model,
            )
            return True
        except Exception as e:
            logger.error("provider_unavailable", error=str(e))
            return False

    def get_supported_models(self) -> List[str]:
        """
        Get list of commonly supported models.

        Returns:
            List of model names
        """
        return [
            "all-MiniLM-L6-v2",  # 384 dim, fast
            "all-mpnet-base-v2",  # 768 dim, better quality
            "paraphrase-multilingual-MiniLM-L12-v2",  # Multilingual
        ]

    async def shutdown(self) -> None:
        """Shutdown the provider and cleanup resources."""
        logger.info("shutting_down_self_hosted_provider")
        self._executor.shutdown(wait=True)
        self._models.clear()
        self._model_dimensions.clear()
