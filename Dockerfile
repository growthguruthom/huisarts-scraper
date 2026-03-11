FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (without playwright/streamlit - not needed for scraping)
COPY requirements-scraper.txt .
RUN pip install --no-cache-dir -r requirements-scraper.txt

# Copy application code
COPY main.py .
COPY run_enrich.py .
COPY daily_run_docker.sh .
COPY scraper/ scraper/
COPY .env .env

RUN chmod +x daily_run_docker.sh
RUN mkdir -p data

VOLUME /app/data

CMD ["./daily_run_docker.sh"]
