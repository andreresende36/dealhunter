# =============================================================================
# Crivo — Dockerfile
# Python 3.11 + Playwright (Chromium headless)
# Build: docker build -t crivo .
# =============================================================================

FROM python:3.11-slim-bookworm

# Metadados
LABEL maintainer="Crivo <[EMAIL_ADDRESS]>"
LABEL description="Sistema automatizado de busca de ofertas — Crivo"

# Variáveis de build
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers \
    TZ=America/Sao_Paulo

# Dependências de sistema para Playwright/Chromium + fontes
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg \
    ca-certificates \
    tzdata \
    fonts-liberation \
    fonts-noto-color-emoji \
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
    pip install --no-cache-dir -r requirements.txt && \
    playwright install chromium

# Copia código-fonte, prompts e dados de referência estáticos
# NOTA: ml_categories.json vai para /app/ (ROOT_DIR) — não /app/data/ (volume)
#       ml_main_categories_all_items.json é montado via bind mount no compose
COPY src/ ./src/
COPY prompts/ ./prompts/
COPY data/ml_categories.json ./

# Cria diretórios de dados e logs, configura usuário não-root
RUN mkdir -p /app/data /app/logs && \
    useradd --create-home --shell /bin/bash crivo && \
    chown -R crivo:crivo /app
USER crivo

# Healthcheck
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "from src.config import settings; print('ok')" || exit 1

# Comando padrão — pode ser sobrescrito no docker-compose
CMD ["python", "-m", "src.runner"]
