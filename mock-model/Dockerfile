FROM python:3.12-slim

WORKDIR /app

# Requirements first: this layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .

# 0.0.0.0, not 127.0.0.1: the container's localhost is unreachable from outside it.
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8001"]
