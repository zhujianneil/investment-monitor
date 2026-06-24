FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc supervisor && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/data /app/reports

# 2026-06-23: 加 web 视图, supervisord 同时管 main + web_view
COPY supervisord.conf /etc/supervisor/conf.d/investment-monitor.conf

EXPOSE 8090

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/investment-monitor.conf"]
