FROM python:3.11-slim

# Build tools
RUN apt-get update && apt-get install -y \
    gcc g++ make curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Requirements
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Playwright
RUN playwright install chromium
RUN playwright install-deps chromium

# Kod
COPY . .

CMD ["python", "main.py"]
