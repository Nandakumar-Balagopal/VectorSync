"""Managed embedding provider for external APIs (OpenAI, Gemini, etc.)."""

import asyncio
from typing import List, Dict, Any, Optional
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import structlog

from app.providers.base import EmbeddingProvider

logger = structlog.get_logger()


class ManagedProvider(EmbeddingProvider):
    """
    Managed embedding provider for external APIs.
    
    Supports OpenAI, Google Gemini, and other API-based embedding services.
    Uses async HTTP calls with rate limiting via semaphore.
    """

    def __init__(
        self,
        api_url: str,
        api_key: Optional[str] = None,
        max_concurrent_requests: int = 10,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
    ):
        """
        Initialize the managed provider.

        Args:
            api_url: Base URL for the API
            api_key: API key for authentication
            max_concurrent_requests: Maximum concurrent API requests
            timeout_seconds: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.api_url = api_url
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        
        # Semaphore for rate limiting
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # HTTP client with connection pooling
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(
                max_connections=max_concurrent_requests,
                max_keepalive_connections=max_concurrent_requests // 2,
            ),
        )
        
        # Model dimension cache
        self._model_dimensions: Dict[str, int] = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
            "embedding-001": 768,  # Google Gemini
        }
        
        logger.info(
            "initializing_managed_provider",
            api_url=api_url,
            max_concurrent=max_concurrent_requests,
            timeout=timeout_seconds,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
    )
    async def _call_api(
        self,
        texts: List[str],
        model_name: str,
    ) -> List[List[float]]:
        """
        Call the external API with retry logic.

        Args:
            texts: List of texts to embed
            model_name: Model name

        Returns:
            List of embeddings

        Raises:
            httpx.HTTPError: If API call fails after retries
        """
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        # OpenAI-style request format
        payload = {
            "input": texts,
            "model": model_name,
        }

        async with self._semaphore:  # Rate limiting
            logger.debug(
                "calling_external_api",
                num_texts=len(texts),
                model_name=model_name,
            )
            
            response = await self._client.post(
                f"{self.api_url}/embeddings",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract embeddings from OpenAI-style response
            embeddings = [item["embedding"] for item in data["data"]]
            
            return embeddings

    async def embed(self, texts: List[str], model_name: str) -> List[List[float]]:
        """
        Generate embeddings using external API.

        Args:
            texts: List of texts to embed
            model_name: Model name

        Returns:
            List of embedding vectors

        Raises:
            ValueError: If texts is empty
            RuntimeError: If API call fails
        """
        if not texts:
            raise ValueError("texts cannot be empty")

        logger.debug(
            "embedding_batch_managed",
            num_texts=len(texts),
            model_name=model_name,
        )

        try:
            embeddings = await self._call_api(texts, model_name)
            
            logger.debug(
                "embedding_complete_managed",
                num_embeddings=len(embeddings),
                dimension=len(embeddings[0]) if embeddings else 0,
            )
            
            return embeddings

        except httpx.HTTPError as e:
            logger.error(
                "api_call_failed",
                error=str(e),
                model_name=model_name,
                num_texts=len(texts),
            )
            raise RuntimeError(f"Failed to call embedding API: {str(e)}") from e
        except Exception as e:
            logger.error(
                "embedding_failed_managed",
                error=str(e),
                model_name=model_name,
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
        # Return cached dimension or default
        return self._model_dimensions.get(model_name, 1536)

    async def is_available(self) -> bool:
        """
        Check if the API is available.

        Returns:
            True if API is reachable
        """
        try:
            # Simple health check
            response = await self._client.get(
                self.api_url,
                timeout=5.0,
            )
            return response.status_code < 500
        except Exception as e:
            logger.error("provider_unavailable_managed", error=str(e))
            return False

    def get_supported_models(self) -> List[str]:
        """
        Get list of supported models.

        Returns:
            List of model names
        """
        return list(self._model_dimensions.keys())

    async def shutdown(self) -> None:
        """Shutdown the provider and cleanup resources."""
        logger.info("shutting_down_managed_provider")
        await self._client.aclose()
