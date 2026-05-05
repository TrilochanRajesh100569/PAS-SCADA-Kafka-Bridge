FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY monitor.py config.yaml ./

RUN useradd -r -u 1000 -m monitor \
 && mkdir -p /var/state /etc/monitor \
 && chown -R monitor:monitor /var/state /etc/monitor /app

USER monitor

ENV PYTHONUNBUFFERED=1 \
    MONITOR_CONFIG=/etc/monitor/config.yaml \
    MONITOR_STATE=/var/state/state.json \
    HEALTH_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:8080/healthz || exit 1

CMD ["python", "-u", "monitor.py"]
