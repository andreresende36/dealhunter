# =============================================================================
# DealHunter — Dockerfile
# Python 3.11 + Playwright (Chromium headless)
# Build: docker build -t dealhunter .
# =============================================================================

FROM python:3.11-slim

# Metadados
LABEL maintainer="DealHunter <contato@sempreblack.com.br>"
LABEL description="Sistema automatizado de caça de ofertas — Sempre Black"

# Variáveis de build
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers \
    TZ=America/Sao_Paulo

# Dependências de sistema necessárias para Playwright/Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg \
    ca-certificates \
    tzdata \
    # Deps do Chromium
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libexpat1 \
    libxcb1 \
    libxkbcommon0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Diretório de trabalho
WORKDIR /app

# Instala dependências Python primeiro (aproveita cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Instala browsers do Playwright (apenas Chromium para economizar espaço)
RUN playwright install chromium && \
    playwright install-deps chromium

# Copia código-fonte
COPY src/ ./src/
COPY n8n/ ./n8n/

# Cria diretórios de dados e logs
RUN mkdir -p /app/data /app/logs

# Usuário não-root para segurança
RUN useradd --create-home --shell /bin/bash dealhunter && \
    chown -R dealhunter:dealhunter /app
USER dealhunter

# Healthcheck
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "from src.config import settings; print('ok')" || exit 1

# Comando padrão — pode ser sobrescrito no docker-compose
CMD ["python", "-m", "src.runner"]
