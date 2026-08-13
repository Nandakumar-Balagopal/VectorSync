"""Main FastAPI application entry point."""

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router, set_embedding_service
from app.services.embedding_service import EmbeddingService
from app.config import settings


# Configure structured logging
def configure_logging() -> None:
    """Configure structured logging with structlog."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer() if settings.log_level == "DEBUG"
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


configure_logging()
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Lifespan context manager for startup and shutdown events.
    
    Initializes the embedding service on startup and cleans up on shutdown.
    """
    logger.info(
        "starting_embedding_service",
        service_name=settings.service_name,
        version=settings.service_version,
        default_provider=settings.default_provider,
        default_model=settings.default_model,
    )

    # Initialize embedding service
    service = EmbeddingService()
    set_embedding_service(service)

    logger.info("embedding_service_started")

    yield

    # Shutdown
    logger.info("shutting_down_embedding_service")
    await service.shutdown()
    logger.info("embedding_service_stopped")


# Create FastAPI application
app = FastAPI(
    title=settings.service_name,
    version=settings.service_version,
    description="Production-ready embedding service for VectorSync",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all HTTP requests."""
    if settings.enable_request_logging:
        logger.info(
            "http_request",
            method=request.method,
            path=request.url.path,
            client_host=request.client.host if request.client else None,
        )

    response = await call_next(request)

    if settings.enable_request_logging:
        logger.info(
            "http_response",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
        )

    return response


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle uncaught exceptions."""
    logger.error(
        "unhandled_exception",
        error=str(exc),
        path=request.url.path,
        method=request.method,
        exc_info=True,
    )

    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if settings.log_level == "DEBUG" else "An error occurred",
        },
    )


# Include API routes
app.include_router(router, prefix="/api/v1")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        reload=False,
    )