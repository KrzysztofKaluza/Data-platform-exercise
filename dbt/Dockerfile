FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y git && \
    pip install --no-cache-dir \
    dbt-core \
    dbt-trino

WORKDIR /app

CMD ["/bin/bash"]