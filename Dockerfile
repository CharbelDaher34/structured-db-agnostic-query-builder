FROM python:3.13-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Sync dependencies using uv
RUN uv sync --frozen

# Copy application code
COPY . .

# Expose port
EXPOSE 8837

# Run the API using uv
CMD ["uv", "run", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8837`"]
