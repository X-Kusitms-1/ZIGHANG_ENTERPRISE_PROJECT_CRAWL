# Dockerfile (Py 3.12 + Playwright on Debian trixie) - patched
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY requirements.txt /app/requirements.txt

# Chromium/Playwright 런타임 deps + 폰트 (Debian 패키지명 사용)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg \
      libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
      libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
      libxrandr2 libgbm1 libasound2 libatspi2.0-0 libxshmfence1 \
      libx11-xcb1 libx11-6 libxext6 libxrender1 libxss1 \
      fonts-liberation fonts-unifont fonts-noto fonts-noto-cjk fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install chromium

COPY . /app
