FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "dbt-core>=1.11,<1.12" \
    "dbt-spark[PyHive]==1.11.0"

WORKDIR /workspace