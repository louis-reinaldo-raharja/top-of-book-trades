FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry

COPY pyproject.toml poetry.lock* ./

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

COPY . .

# Copy JAR files
COPY jars /opt/flink/lib/

# Set environment variable for PyFlink
ENV PYTHONPATH="/opt/flink/lib:$PYTHONPATH"

CMD ["python", "pyflink_test.py"]