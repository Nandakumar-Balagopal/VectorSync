"""API routes for the embedding service."""

import time
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
import structlog

from app.models.requests import BatchEmbedRequest, QueryEmbedRequest
from app.models.responses import (
    BatchEmbedResponse,
    QueryEmbedResponse,
    HealthResponse,
    ErrorResponse,
    ModelsResponse,
    ModelMetadata,
)
from app.services.embedding_service import EmbeddingService
from app.config import settings

logger = structlog.get_logger()

router = APIRouter()

# Global service instance (initialized in main.py)
embedding_service: EmbeddingService | None = None


def set_embedding_service(service: EmbeddingService) -> None:
    """Set the global embedding service instance."""
    global embedding_service
    embedding_service = service


@router.post("/embed", response_model=BatchEmbedResponse)
async def embed_batch(request: BatchEmbedRequest, req: Request) -> BatchEmbedResponse:
    """
    Generate embeddings for a batch of records.
    
    This endpoint is optimized for high-throughput ingestion.
    Supports partial failures - successful embeddings are returned even if some fail.
    """
    if embedding_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    request_id = request.request_id
    
    logger.info(
        "batch_embed_request",
        request_id=request_id,
        num_records=len(request.records),
        client_host=req.client.host if req.client else None,
    )

    try:
        results, processing_time_ms = await embedding_service.embed_batch(
            request.records,
            request_id,
        )

        successful = sum(1 for r in results if not r.error)
        failed = sum(1 for r in results if r.error)

        response = BatchEmbedResponse(
            request_id=request_id,
            results=results,
            total_records=len(request.records),
            successful=successful,
            failed=failed,
            processing_time_ms=processing_time_ms,
        )

        logger.info(
            "batch_embed_response",
            request_id=request_id,
            successful=successful,
            failed=failed,
            processing_time_ms=processing_time_ms,
        )

        return response

    except Exception as e:
        logger.error(
            "batch_embed_error",
            request_id=request_id,
            error=str(e),
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process batch: {str(e)}",
        )


@router.post("/embed-query", response_model=QueryEmbedResponse)
async def embed_query(request: QueryEmbedRequest, req: Request) -> QueryEmbedResponse:
    """
    Generate embedding for a single query text.
    
    This endpoint is optimized for low-latency search queries.
    """
    if embedding_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    logger.debug(
        "query_embed_request",
        model_name=request.model_name,
        provider=request.provider,
        client_host=req.client.host if req.client else None,
    )

    try:
        embedding, dimension, processing_time_ms = await embedding_service.embed_query(
            request.text,
            request.model_name,
            request.provider,
        )

        response = QueryEmbedResponse(
            embedding=embedding,
            model_name=request.model_name,
            dimension=dimension,
            processing_time_ms=processing_time_ms,
        )

        logger.debug(
            "query_embed_response",
            dimension=dimension,
            processing_time_ms=processing_time_ms,
        )

        return response

    except ValueError as e:
        logger.warning("query_embed_validation_error", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("query_embed_error", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate query embedding: {str(e)}",
        )


@router.get("/models", response_model=ModelsResponse)
async def list_models() -> ModelsResponse:
    """
    List available embedding models with metadata.
    """
    if embedding_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        model_entries = await embedding_service.get_supported_models_with_metadata()
        return ModelsResponse(
            models=[ModelMetadata(**entry) for entry in model_entries]
        )
    except Exception as e:
        logger.error("list_models_error", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list models: {str(e)}",
        )


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.
    
    Returns service status and provider availability.
    """
    if embedding_service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        health_status = await embedding_service.get_health_status()

        response = HealthResponse(
            status="healthy",
            version=settings.service_version,
            providers=health_status["providers"],
            models_loaded=health_status["models_loaded"],
        )

        return response

    except Exception as e:
        logger.error("health_check_error", error=str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}",
        )


@router.get("/")
async def root() -> dict[str, str]:
    """Root endpoint with service information."""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "running",
    }