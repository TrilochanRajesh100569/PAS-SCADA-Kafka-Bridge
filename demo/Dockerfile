FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates ./templates
COPY static ./static

RUN useradd -r -u 1000 -m demo && chown -R demo:demo /app
USER demo

ENV PYTHONUNBUFFERED=1 \
    PORT=8090 \
    MODE=synthetic

EXPOSE 8090

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:8090/healthz || exit 1

CMD ["python", "-u", "app.py"]
