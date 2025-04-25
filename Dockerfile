FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install uv
RUN apt update
RUN apt install -y git curl
ENV PATH="/root/.local/bin:$PATH"

# Increase file descriptor limits
RUN echo "* soft nofile 65536" >> /etc/security/limits.conf && \
    echo "* hard nofile 65536" >> /etc/security/limits.conf && \
    echo "session required pam_limits.so" >> /etc/pam.d/common-session

# Copy project files
COPY pyproject.toml .
COPY uv.lock .
COPY README.md .
COPY fastapi.log-config.yaml .
COPY news_extractor_engine ./news_extractor_engine

# Install dependencies with uv and create virtual environment
RUN uv sync --frozen
RUN uv add . --dev

# Create logs directory
RUN mkdir -p logs/api

# Set the command to run the app using the virtual environment’s Python
CMD [ "uv", "run", "python", "news_extractor_engine/main.py" ]