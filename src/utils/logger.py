"""
Structured logging configuration for WMATA ETL pipeline.

Uses structlog for JSON-formatted, context-rich logging.
"""

import logging
import sys
from typing import Any

import structlog


def configure_logging(
    level: str = "INFO", json_format: bool = True, service_name: str = "wmata-etl"
) -> None:
    """
    Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: If True, output JSON logs. If False, output human-readable.
        service_name: Service name to include in all log entries.
    """
    # Set up standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )

    # Choose processors based on format
    renderer: Any
    if json_format:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        renderer,
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Bind service name to all loggers
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=service_name)


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """
    Get a structured logger instance.

    Args:
        name: Optional logger name for context.

    Returns:
        Configured structlog logger.
    """
    logger = structlog.get_logger()
    if name:
        logger = logger.bind(component=name)
    return logger  # type: ignore[no-any-return]


# Configure on import with defaults
configure_logging()
