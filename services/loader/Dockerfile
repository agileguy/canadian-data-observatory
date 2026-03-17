FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    cron \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

ENV GDAL_CONFIG=/usr/bin/gdal-config

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir numpy && \
    pip install --no-cache-dir -r requirements.txt

COPY crontab /etc/cron.d/cdo-loader
RUN chmod 0644 /etc/cron.d/cdo-loader && crontab /etc/cron.d/cdo-loader

COPY app/ ./app/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
