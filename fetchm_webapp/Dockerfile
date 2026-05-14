FROM python:3.12-slim

ARG FETCHM_WEBAPP_GIT_COMMIT=unknown
ARG NEXTFLOW_VERSION=25.04.6
ARG MINIFORGE_VERSION=25.3.1-0
ARG NCBI_DATASETS_URL=https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets
ARG TAXONKIT_VERSION=0.20.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FETCHM_WEBAPP_GIT_COMMIT=${FETCHM_WEBAPP_GIT_COMMIT} \
    PORT=8000 \
    BROWSER_PATH=/usr/bin/chromium

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    bash \
    ca-certificates \
    chromium \
    curl \
    gcc \
    gfortran \
    git \
    libopenblas-dev \
    libfreetype6-dev \
    libjpeg62-turbo-dev \
    libpng-dev \
    openjdk-21-jre-headless \
    pkg-config \
    procps \
    unzip \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://github.com/nextflow-io/nextflow/releases/download/v${NEXTFLOW_VERSION}/nextflow" \
    -o /usr/local/bin/nextflow \
 && chmod +x /usr/local/bin/nextflow

RUN curl -fsSL "https://github.com/conda-forge/miniforge/releases/download/${MINIFORGE_VERSION}/Miniforge3-${MINIFORGE_VERSION}-Linux-x86_64.sh" \
    -o /tmp/miniforge.sh \
 && bash /tmp/miniforge.sh -b -p /opt/conda \
 && rm -f /tmp/miniforge.sh \
 && /opt/conda/bin/conda clean -afy

ENV PATH=/opt/conda/bin:$PATH

RUN curl -fsSL "${NCBI_DATASETS_URL}" \
    -o /usr/local/bin/datasets \
 && chmod +x /usr/local/bin/datasets

RUN curl -fsSL "https://github.com/shenwei356/taxonkit/releases/download/v${TAXONKIT_VERSION}/taxonkit_linux_amd64.tar.gz" \
    -o /tmp/taxonkit.tar.gz \
 && tar -xzf /tmp/taxonkit.tar.gz -C /usr/local/bin taxonkit \
 && chmod +x /usr/local/bin/taxonkit \
 && rm -f /tmp/taxonkit.tar.gz

COPY fetchm_webapp/requirements.txt /tmp/webapp-requirements.txt

RUN pip install --no-cache-dir -r /tmp/webapp-requirements.txt \
 && pip install --no-cache-dir gunicorn

COPY fetchm_webapp /app/fetchm_webapp

WORKDIR /app/fetchm_webapp

RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin fetchm \
 && mkdir -p /app/fetchm_webapp/data/jobs /app/fetchm_webapp/data/external_tools /home/fetchm/.taxonkit \
 && chown -R fetchm:fetchm /app/fetchm_webapp /home/fetchm

ENV HOME=/home/fetchm \
    NXF_HOME=/app/fetchm_webapp/data/external_tools/nextflow/home \
    NXF_CONDA_CACHEDIR=/app/fetchm_webapp/data/external_tools/conda/envs \
    CONDA_PKGS_DIRS=/app/fetchm_webapp/data/external_tools/conda/pkgs \
    NXF_SYNTAX_PARSER=v1

USER fetchm

EXPOSE 8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", "wsgi:app"]
