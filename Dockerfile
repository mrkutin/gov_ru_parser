# Use Playwright base image with browsers and system deps preinstalled
FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install langchain-qdrant langchain-huggingface

# Ensure matching browser binaries are available for the installed Playwright
RUN python -m playwright install --with-deps chromium

# Copy application code
COPY app ./app
COPY main.py ./
COPY entrypoint.sh ./

# Default command is provided by docker-compose; keep a sensible fallback
ENTRYPOINT ["/app/entrypoint.sh"]


