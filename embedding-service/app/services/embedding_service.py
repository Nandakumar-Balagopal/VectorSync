"""Core embedding service with batching and provider management."""

import time
from typing import List, Dict, Tuple, TypedDict
import structlog

from app.models.requests import EmbeddingRecord
from app.models.responses import EmbeddingResult
from app.providers.base import EmbeddingProvider
from app.providers.self_hosted import SelfHostedProvider
from app.providers.managed import ManagedProvider
from app.config import settings

logger = structlog.get_logger()


class SupportedModelMetadata(TypedDict):
    """Typed model metadata entry."""

    model_name: str
    provider: str
    dimension: int
    available: bool


class HealthStatus(TypedDict):
    """Typed health status payload."""

    providers: Dict[str, bool]
    models_loaded: List[str]


class EmbeddingService:
    """
    Core service for managing embedding generation.
    
    Handles provider selection, batching, and error handling.
    """

    def __init__(self):
        """Initialize the embedding service with providers."""
        self._providers: Dict[str, EmbeddingProvider] = {}
        self._initialize_providers()

    def _initialize_providers(self) -> None:
        """Initialize embedding providers based on configuration."""
        logger.info("initializing_embedding_providers")

        # Initialize self-hosted provider
        self._providers["self_hosted"] = SelfHostedProvider(
            default_model=settings.default_model,
            max_workers=settings.self_hosted_max_workers,
            device=settings.self_hosted_device,
        )

        # Initialize managed provider if configured
        if settings.managed_api_url:
            self._providers["managed"] = ManagedProvider(
                api_url=settings.managed_api_url,
                api_key=settings.managed_api_key,
                max_concurrent_requests=settings.managed_max_concurrent,
                timeout_seconds=settings.managed_timeout,
                max_retries=settings.managed_max_retries,
            )

        logger.info(
            "providers_initialized",
            providers=list(self._providers.keys()),
        )

    def _get_provider(self, provider_type: str) -> EmbeddingProvider:
        """
        Get provider by type.

        Args:
            provider_type: Provider type ('self_hosted' or 'managed')

        Returns:
            EmbeddingProvider instance

        Raises:
            ValueError: If provider not available
        """
        if provider_type not in self._providers:
            raise ValueError(
                f"Provider '{provider_type}' not available. "
                f"Available providers: {list(self._providers.keys())}"
            )
        return self._providers[provider_type]

    def _batch_records(
        self,
        records: List[EmbeddingRecord],
    ) -> List[List[EmbeddingRecord]]:
        """
        Split records into batches based on max_batch_size.

        Args:
            records: List of records to batch

        Returns:
            List of batches
        """
        batch_size = settings.max_batch_size
        batches = []
        
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            batches.append(batch)
        
        logger.debug(
            "records_batched",
            total_records=len(records),
            num_batches=len(batches),
            batch_size=batch_size,
        )
        
        return batches

    async def embed_batch(
        self,
        records: List[EmbeddingRecord],
        request_id: str,
    ) -> Tuple[List[EmbeddingResult], float]:
        """
        Generate embeddings for a batch of records.

        Args:
            records: List of records to embed
            request_id: Request ID for logging

        Returns:
            Tuple of (results, processing_time_ms)
        """


        start_time = time.time()
        results: List[EmbeddingResult] = []

        logger.info(
            "processing_batch",
            request_id=request_id,
            num_records=len(records),
        )

        # Group records by provider and model
        grouped: Dict[Tuple[str, str], List[EmbeddingRecord]] = {}
        for record in records:
            key = (record.provider, record.model_name)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(record)

        # Process each group
        for (provider_type, model_name), group_records in grouped.items():
            try:
                provider = self._get_provider(provider_type)
                
                # Split into sub-batches if needed
                batches = self._batch_records(group_records)
                
                for batch in batches:
                    # Extract texts
                    texts = [r.text for r in batch]
                    
                    # Generate embeddings
                    embeddings = await provider.embed(texts, model_name)
                    
                    # Create results (preserve vector_id mapping)
                    for record, embedding in zip(batch, embeddings):
                        results.append(
                            EmbeddingResult(
                                vector_id=record.vector_id,
                                embedding=embedding,
                            )
                        )

            except Exception as e:
                logger.error(
                    "batch_processing_failed",
                    request_id=request_id,
                    provider=provider_type,
                    model=model_name,
                    error=str(e),
                )
                
                # Add error results for failed records
                for record in group_records:
                    results.append(
                        EmbeddingResult(
                            vector_id=record.vector_id,
                            embedding=[],
                            error=str(e),
                        )
                    )

        processing_time_ms = (time.time() - start_time) * 1000

        logger.info(
            "batch_processing_complete",
            request_id=request_id,
            total_records=len(records),
            successful=sum(1 for r in results if not r.error),
            failed=sum(1 for r in results if r.error),
            processing_time_ms=processing_time_ms,
        )

        return results, processing_time_ms

    async def embed_query(
        self,
        text: str,
        model_name: str,
        provider_type: str,
    ) -> Tuple[List[float], int, float]:
        """
        Generate embedding for a single query (low latency).

        Args:
            text: Query text
            model_name: Model to use
            provider_type: Provider type

        Returns:
            Tuple of (embedding, dimension, processing_time_ms)
        """
        if not text or not text.strip():
            raise ValueError("Query text cannot be empty or whitespace only")
        start_time = time.time()

        logger.debug(
            "processing_query",
            model_name=model_name,
            provider=provider_type,
        )

        try:
            provider = self._get_provider(provider_type)
            
            # Generate embedding
            embeddings = await provider.embed([text], model_name)
            embedding = embeddings[0]
            
            # Get dimension
            dimension = len(embedding)
            
            processing_time_ms = (time.time() - start_time) * 1000

            logger.debug(
                "query_processing_complete",
                dimension=dimension,
                processing_time_ms=processing_time_ms,
            )

            return embedding, dimension, processing_time_ms

        except Exception as e:
            logger.error(
                "query_processing_failed",
                model_name=model_name,
                provider=provider_type,
                error=str(e),
            )
            raise

    async def get_supported_models_with_metadata(self) -> List[SupportedModelMetadata]:
        """
        Get supported models enriched with provider and dimension metadata.

        Returns:
            List of model metadata dictionaries
        """
        models: List[SupportedModelMetadata] = []

        for provider_name, provider in self._providers.items():
            is_available = await provider.is_available()

            for model_name in provider.get_supported_models():
                try:
                    dimension = await provider.get_embedding_dimension(model_name)
                except Exception as e:
                    logger.warning(
                        "model_dimension_lookup_failed",
                        provider=provider_name,
                        model_name=model_name,
                        error=str(e),
                    )
                    dimension = 0

                models.append(
                    {
                        "model_name": model_name,
                        "provider": provider_name,
                        "dimension": dimension,
                        "available": is_available,
                    }
                )

        return models

    async def get_health_status(self) -> HealthStatus:
        """
        Get health status of all providers.

        Returns:
            Health status dictionary
        """
        provider_status = {}
        models_loaded = []

        for provider_name, provider in self._providers.items():
            is_available = await provider.is_available()
            provider_status[provider_name] = is_available
            
            if is_available:
                models_loaded.extend(provider.get_supported_models())

        return {
            "providers": provider_status,
            "models_loaded": list(set(models_loaded)),
        }

    async def shutdown(self) -> None:
        """Shutdown all providers."""
        logger.info("shutting_down_embedding_service")
        
        for provider_name, provider in self._providers.items():
            try:
                await provider.shutdown()
            except Exception as e:
                logger.error(
                    "provider_shutdown_failed",
                    provider=provider_name,
                    error=str(e),
                )
