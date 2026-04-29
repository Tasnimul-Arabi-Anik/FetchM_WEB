FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    BROWSER_PATH=/usr/bin/chromium

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    chromium \
    curl \
    gcc \
    gfortran \
    libopenblas-dev \
    libfreetype6-dev \
    libjpeg62-turbo-dev \
    libpng-dev \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/LATEST/linux-amd64/datasets" \
    -o /usr/local/bin/datasets \
 && chmod +x /usr/local/bin/datasets

RUN curl -fsSL "https://github.com/shenwei356/taxonkit/releases/download/v0.20.0/taxonkit_linux_amd64.tar.gz" \
    -o /tmp/taxonkit.tar.gz \
 && tar -xzf /tmp/taxonkit.tar.gz -C /usr/local/bin taxonkit \
 && chmod +x /usr/local/bin/taxonkit \
 && rm -f /tmp/taxonkit.tar.gz

COPY fetchm_webapp/requirements.txt /tmp/webapp-requirements.txt

RUN pip install --no-cache-dir -r /tmp/webapp-requirements.txt \
 && pip install --no-cache-dir gunicorn

COPY fetchm_webapp /app/fetchm_webapp

WORKDIR /app/fetchm_webapp

RUN mkdir -p /app/fetchm_webapp/data/jobs

EXPOSE 8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", "wsgi:app"]
