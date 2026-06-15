# Python orchestrator: generates seeded data, drives sessions/rounds, collects metrics,
# builds the report. Mounted live at /app so code edits don't require a rebuild.
# Python 3.11 (not 3.12): pyspark 3.5 still imports the stdlib `distutils` removed in 3.12.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# mysql client is handy for manual StarRocks debugging from inside the container.
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-mysql-client curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sleep", "infinity"]
