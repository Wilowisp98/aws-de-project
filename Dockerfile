FROM python:3.12-slim-bookworm

# Install system dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install UV package manager
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod +x /install.sh && /install.sh && rm /install.sh

# Set up UV environment path
ENV PATH="/root/.local/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy dependency files first for better layer caching
COPY requirements.txt ./

# Install dependencies using UV
RUN uv pip install --system --requirement requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Health check (dynamic based on environment)
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/${ENVIRONMENT}/v1/health || curl -f http://localhost:8000/api/v1/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]