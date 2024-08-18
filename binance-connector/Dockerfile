FROM python:3.9-slim as base

# Set environment variables
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.4.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=false \
    POETRY_NO_INTERACTION=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN curl -sSL https://install.python-poetry.org | python -

# Add poetry to PATH
ENV PATH="${POETRY_HOME}/bin:${PATH}"

WORKDIR /app

# Copy project files
COPY . .

# Install project dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Create start script
RUN echo '#!/bin/sh\n\
set -e\n\
\n\
echo "Waiting for Pinot controller to be ready..."\n\
until curl -s http://pinot-controller:9000/health > /dev/null; do\n\
  echo "Pinot controller is unavailable - sleeping"\n\
  sleep 1\n\
done\n\
\n\
echo "Pinot controller is up - executing command"\n\
exec "$@"\n\
' > /app/entrypoint.sh

# Make the script executable
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]

CMD ["python", "quix_consumer.py"]
