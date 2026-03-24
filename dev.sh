#!/usr/bin/env bash
set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV="$ROOT/.venv"
LOG_DIR="$ROOT/logs"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

mkdir -p "$LOG_DIR"

# Verificações
if [ ! -d "$VENV" ]; then
  echo -e "${RED}[erro]${NC} .venv não encontrado. Rode:"
  echo -e "  python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

if [ ! -d "$ROOT/admin/node_modules" ]; then
  echo -e "${YELLOW}[info]${NC} Instalando dependências do admin..."
  (cd "$ROOT/admin" && npm install)
fi

# Cleanup ao pressionar Ctrl+C
cleanup() {
  echo -e "\n${YELLOW}[info]${NC} Encerrando serviços..."
  kill "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null
  wait "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null
  kill "$TAIL_PID" 2>/dev/null
  echo -e "${GREEN}[ok]${NC} Serviços encerrados."
  exit 0
}
trap cleanup INT TERM

# Backend Python
echo -e "${GREEN}[backend]${NC} Iniciando Python na porta 8000..."
source "$VENV/bin/activate"
python -m src.runner >> "$LOG_DIR/backend.log" 2>&1 &
BACKEND_PID=$!

# Frontend Next.js
echo -e "${GREEN}[frontend]${NC} Iniciando Next.js na porta 3000..."
(cd "$ROOT/admin" && npm run dev >> "$LOG_DIR/frontend.log" 2>&1) &
FRONTEND_PID=$!

echo ""
echo -e "  Backend:  ${GREEN}http://localhost:8000${NC}  (docs: http://localhost:8000/docs)"
echo -e "  Frontend: ${GREEN}http://localhost:3000${NC}"
echo -e "  Logs:     ${LOG_DIR}/"
echo -e "\n  Pressione ${YELLOW}Ctrl+C${NC} para encerrar tudo.\n"

# Exibe logs dos dois serviços em tempo real
tail -f "$LOG_DIR/backend.log" -f "$LOG_DIR/frontend.log" &
TAIL_PID=$!

wait "$BACKEND_PID" "$FRONTEND_PID"
kill "$TAIL_PID" 2>/dev/null
