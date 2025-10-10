FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Dependencias del sistema si las llegas a necesitar para requests/ssl
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Reqs mínimos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código
COPY scheduler.py .

CMD ["python", "scheduler.py"]
