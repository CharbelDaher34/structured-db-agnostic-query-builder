"""
Centralised logging for the ``query_builder`` package.

Every module in the package obtains its logger through :class:`QueryBuilderLogger`
rather than calling :func:`logging.getLogger` directly. That gives us a single
configuration point — host applications can adjust the package's verbosity with
one call without touching the host logger or any of our internal modules.

Two pieces of public API:

- :meth:`QueryBuilderLogger.get` — drop-in replacement for ``logging.getLogger``
  used inside every module of the package.
- :meth:`QueryBuilderLogger.configure` — call once at app startup to set the
  package-wide log level, format, or propagation behaviour. Reads ``LOG_LEVEL``
  from the environment by default.
"""

import logging
import os
from typing import Optional, Union

# Root of the package's logger hierarchy. Every ``QueryBuilderLogger.get(__name__)``
# inside the package returns a child of this logger, so configuring it once
# cascades to all of them.
_PACKAGE_ROOT = "query_builder"

_DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s :: %(message)s"
_DEFAULT_DATEFMT = "%H:%M:%S"

# Marker attribute attached to our handler so re-running configure() doesn't
# stack duplicate handlers (which would double every log line).
_HANDLER_MARKER = "_query_builder_handler"


class QueryBuilderLogger:
    """
    Single entry point for getting and configuring package loggers.

    Usage inside the package::

        from query_builder._logging import QueryBuilderLogger
        logger = QueryBuilderLogger.get(__name__)

    Usage from the host application (anywhere before the first query)::

        from query_builder import QueryBuilderLogger
        QueryBuilderLogger.configure(level="DEBUG")
    """

    _configured = False

    @classmethod
    def get(cls, name: str) -> logging.Logger:
        """
        Return a logger under the package namespace.

        On first call, configures the package logger from the environment
        (``LOG_LEVEL``, default ``INFO``) so modules that import a logger
        at module load time aren't silenced when the host app forgets to
        call :meth:`configure` explicitly.
        """
        cls._ensure_configured()
        return logging.getLogger(name)

    @classmethod
    def configure(
        cls,
        level: Optional[Union[str, int]] = None,
        format: Optional[str] = None,
        propagate: bool = False,
    ) -> None:
        """
        Configure the package's logging.

        Args:
            level: Logging level — ``"DEBUG"`` / ``"INFO"`` / ``"WARNING"`` /
                ``"ERROR"`` / ``"CRITICAL"`` or the equivalent ``logging`` int.
                Falls back to the ``LOG_LEVEL`` env var, then to ``"INFO"``.
            format: Log message format string. Defaults to
                ``"%(asctime)s [%(levelname)s] %(name)s :: %(message)s"``.
            propagate: Whether to bubble records up to the root logger.
                Defaults to ``False`` so the package owns its own output and
                doesn't duplicate when the host app also has a handler on root.
        """
        resolved_level: Union[str, int] = level or os.getenv("LOG_LEVEL") or "INFO"
        if isinstance(resolved_level, str):
            resolved_level = resolved_level.upper()

        pkg_logger = logging.getLogger(_PACKAGE_ROOT)
        pkg_logger.setLevel(resolved_level)
        pkg_logger.propagate = propagate

        # Only attach our handler once — re-configuring (e.g. switching levels
        # at runtime) shouldn't double-log every line.
        if not any(getattr(h, _HANDLER_MARKER, False) for h in pkg_logger.handlers):
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter(format or _DEFAULT_FORMAT, datefmt=_DEFAULT_DATEFMT)
            )
            setattr(handler, _HANDLER_MARKER, True)
            pkg_logger.addHandler(handler)

        cls._configured = True

    @classmethod
    def _ensure_configured(cls) -> None:
        if not cls._configured:
            cls.configure()
