FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY tests/ ./tests/
COPY generate_dirty_data.py pipeline.py ./
RUN mkdir -p /app/shared-data/raw /app/shared-data/processed /tmp/dask

CMD ["python", "-m", "src.pipeline_flow"]
