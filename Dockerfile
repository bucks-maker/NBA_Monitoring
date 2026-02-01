FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY scripts/ scripts/

RUN mkdir -p data

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
