"""Tests for the embedding service."""

import pytest
from app.services.embedding_service import EmbeddingService
from app.models.requests import EmbeddingRecord
from app.config import settings


@pytest.fixture
def embedding_service():
    """Create an embedding service instance for testing."""
    service = EmbeddingService()
    yield service
    # Cleanup is handled by the service itself


@pytest.mark.asyncio
async def test_embed_batch_self_hosted(embedding_service):
    """Test batch embedding with self-hosted provider."""
    records = [
        EmbeddingRecord(
            vector_id="vec-001",
            source_table="products",
            source_row_id="prod-123",
            text="Lightweight running shoes",
            model_name="all-MiniLM-L6-v2",
            provider="self_hosted",
        ),
        EmbeddingRecord(
            vector_id="vec-002",
            source_table="products",
            source_row_id="prod-456",
            text="Comfortable walking shoes",
            model_name="all-MiniLM-L6-v2",
            provider="self_hosted",
        ),
    ]

    results, processing_time = await embedding_service.embed_batch(
        records, "test-request-001"
    )

    assert len(results) == 2
    assert all(r.vector_id in ["vec-001", "vec-002"] for r in results)
    assert all(len(r.embedding) == 384 for r in results)  # MiniLM dimension
    assert all(r.error is None for r in results)
    assert processing_time > 0


@pytest.mark.asyncio
async def test_embed_query_self_hosted(embedding_service):
    """Test query embedding with self-hosted provider."""
    embedding, dimension, processing_time = await embedding_service.embed_query(
        text="affordable running shoes",
        model_name="all-MiniLM-L6-v2",
        provider_type="self_hosted",
    )

    assert len(embedding) == 384
    assert dimension == 384
    assert processing_time > 0
    assert all(isinstance(x, float) for x in embedding)


@pytest.mark.asyncio
async def test_embed_batch_preserves_vector_ids(embedding_service):
    """Test that vector IDs are preserved in results."""
    records = [
        EmbeddingRecord(
            vector_id=f"vec-{i:03d}",
            source_table="test",
            source_row_id=f"row-{i}",
            text=f"Test text {i}",
            model_name="all-MiniLM-L6-v2",
            provider="self_hosted",
        )
        for i in range(10)
    ]

    results, _ = await embedding_service.embed_batch(records, "test-request-002")

    # Check that all vector IDs are present
    result_ids = {r.vector_id for r in results}
    expected_ids = {f"vec-{i:03d}" for i in range(10)}
    assert result_ids == expected_ids


@pytest.mark.asyncio
async def test_embed_batch_handles_errors_gracefully(embedding_service):
    """Test that batch processing handles errors gracefully."""
    records = [
        EmbeddingRecord(
            vector_id="vec-001",
            source_table="test",
            source_row_id="row-1",
            text="Valid text",
            model_name="all-MiniLM-L6-v2",
            provider="self_hosted",
        ),
        EmbeddingRecord(
            vector_id="vec-002",
            source_table="test",
            source_row_id="row-2",
            text="Another valid text",
            model_name="invalid-model",  # This will fail
            provider="self_hosted",
        ),
    ]

    results, _ = await embedding_service.embed_batch(records, "test-request-003")

    # Should have results for both records
    assert len(results) == 2
    
    # At least one should succeed
    successful = [r for r in results if not r.error]
    assert len(successful) >= 1


@pytest.mark.asyncio
async def test_batching_respects_max_batch_size(embedding_service):
    """Test that batching respects max_batch_size configuration."""
    # Create more records than max_batch_size
    num_records = settings.max_batch_size + 10
    records = [
        EmbeddingRecord(
            vector_id=f"vec-{i:04d}",
            source_table="test",
            source_row_id=f"row-{i}",
            text=f"Test text {i}",
            model_name="all-MiniLM-L6-v2",
            provider="self_hosted",
        )
        for i in range(num_records)
    ]

    results, _ = await embedding_service.embed_batch(records, "test-request-004")

    # All records should be processed
    assert len(results) == num_records
    assert all(r.vector_id == f"vec-{i:04d}" for i, r in enumerate(results))


@pytest.mark.asyncio
async def test_health_status(embedding_service):
    """Test health status check."""
    health = await embedding_service.get_health_status()

    assert "providers" in health
    assert "models_loaded" in health
    assert "self_hosted" in health["providers"]
    assert health["providers"]["self_hosted"] is True
    assert len(health["models_loaded"]) > 0


@pytest.mark.asyncio
async def test_empty_text_raises_error(embedding_service):
    """Test that empty text raises appropriate error."""
    with pytest.raises(Exception):
        await embedding_service.embed_query(
            text="",
            model_name="all-MiniLM-L6-v2",
            provider_type="self_hosted",
        )


@pytest.mark.asyncio
async def test_concurrent_requests(embedding_service):
    """Test that service handles concurrent requests correctly."""
    import asyncio

    async def embed_single(i: int):
        return await embedding_service.embed_query(
            text=f"Test query {i}",
            model_name="all-MiniLM-L6-v2",
            provider_type="self_hosted",
        )

    # Run 5 concurrent requests
    results = await asyncio.gather(*[embed_single(i) for i in range(5)])

    assert len(results) == 5
    assert all(len(r[0]) == 384 for r in results)  # All have correct dimension