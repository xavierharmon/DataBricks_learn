"""
api/main.py
-----------------------------------------------------------------------
FASTAPI APPLICATION — entry point for the GraphQL API

This file:
  1. Creates the FastAPI app
  2. Connects to the data backend (DuckDB or Databricks)
  3. Mounts the Strawberry GraphQL endpoint
  4. Injects the connector into every GraphQL request via context
  5. Adds a REST health endpoint for monitoring

HOW TO RUN:
    cd ecommerce_analytics
    python -m uvicorn api.main:app --reload --port 8000

THEN OPEN:
    http://localhost:8000/graphql   ← GraphQL Playground (browser UI)
    http://localhost:8000/health    ← REST health check
    http://localhost:8000/schema    ← Raw GraphQL SDL

THE GRAPHQL PLAYGROUND:
    Strawberry ships with GraphiQL — an in-browser IDE for writing
    and testing GraphQL queries. Open it and you'll see:
      - Auto-complete for all fields and types
      - Schema explorer (Docs panel on the right)
      - Query history
    No Postman needed — everything is in the browser.
-----------------------------------------------------------------------
"""

from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from strawberry.fastapi import GraphQLRouter

from api.config import settings
from api.schema.schema import schema
from api.connectors.databricks_connector import get_connector


# -----------------------------------------------------------------------
# CONNECTOR LIFECYCLE
# Connect once at startup, disconnect cleanly at shutdown.
# This avoids reconnecting on every request (expensive for cloud DBs).
# -----------------------------------------------------------------------

connector = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan handler — runs setup before the server starts
    and teardown after it stops.

    This is the recommended FastAPI pattern for managing shared
    resources like database connections.
    """
    global connector
    print(f"\n🚀 Starting {settings.api_title}")
    print(f"   Backend: {settings.data_backend.value}")

    connector = get_connector(settings)
    print(f"   Connected: {connector.get_backend_name()}\n")

    yield  # Server runs here

    # Cleanup on shutdown
    if connector:
        connector.disconnect()
        print("\n👋 Connector closed. Goodbye.")


# -----------------------------------------------------------------------
# GRAPHQL CONTEXT
# The context dict is passed to every resolver via info.context.
# Add anything resolvers need here — connector, settings, auth info.
# -----------------------------------------------------------------------

async def get_context(request: Request) -> Dict[str, Any]:
    """
    Build the context dict injected into every GraphQL resolver.

    resolver receives:  info.context["connector"]
                        info.context["settings"]
                        info.context["request"]

    This is how resolvers access the database without importing
    global state — dependency injection keeps them testable.
    """
    return {
        "connector": connector,
        "settings": settings,
        "request": request,
    }


# -----------------------------------------------------------------------
# GRAPHQL ROUTER
# Strawberry's FastAPI integration — mounts GraphQL at /graphql
# -----------------------------------------------------------------------

graphql_router = GraphQLRouter(
    schema,
    context_getter=get_context,
    graphiql=True,          # Enable the browser-based GraphQL IDE
)


# -----------------------------------------------------------------------
# FASTAPI APP
# -----------------------------------------------------------------------

app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=(
        "GraphQL API over the e-commerce analytics semantic model. "
        "Exposes orders, customers, and products from the mart layer."
    ),
    lifespan=lifespan,
)

# Mount GraphQL at /graphql
app.include_router(graphql_router, prefix="/graphql")


# -----------------------------------------------------------------------
# ADDITIONAL REST ENDPOINTS
# -----------------------------------------------------------------------

@app.get("/health", tags=["monitoring"])
async def health_check():
    """Simple health check for load balancers and monitoring tools."""
    return {
        "status": "ok",
        "api": settings.api_title,
        "version": settings.api_version,
        "backend": connector.get_backend_name() if connector else "not connected",
    }


@app.get("/schema", tags=["developer"])
async def get_schema():
    """Return the raw GraphQL SDL — useful for client codegen."""
    return {"schema": str(schema)}


@app.get("/", tags=["developer"])
async def root():
    """Redirect hint for the root URL."""
    return {
        "message": f"Welcome to {settings.api_title}",
        "graphql_playground": "http://localhost:8000/graphql",
        "health": "http://localhost:8000/health",
        "docs": "http://localhost:8000/docs",
    }


# -----------------------------------------------------------------------
# ERROR HANDLING
# -----------------------------------------------------------------------

@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    """
    Return clean 400 errors for validation failures
    (e.g. invalid dimension name in revenueBy query).
    """
    return JSONResponse(
        status_code=400,
        content={"error": str(exc)}
    )
