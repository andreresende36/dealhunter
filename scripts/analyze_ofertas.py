"""
Crivo — Análise de Cards de Ofertas via IA
Lê o cards.json gerado pelo screenshot_ofertas.py, seleciona uma amostra de 100 cards
e envia ao Claude para identificar campos extraíveis, padrões de preço/parcelamento
e quaisquer outros padrões relevantes.

Uso:
    python scripts/analyze_ofertas.py
    python scripts/analyze_ofertas.py --run 2026-03-26_13-23-13
    python scripts/analyze_ofertas.py --sample 50

Saída:
    debug/ofertas/{run_id}/analysis.md
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import httpx  # noqa: E402
import structlog  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402

from src.config import settings  # noqa: E402

logger = structlog.get_logger(__name__)

OFERTAS_DIR = ROOT_DIR / "debug" / "ofertas"
SAMPLE_SIZE = 100
MODEL = "anthropic/claude-sonnet-4-6"
API_URL = "https://openrouter.ai/api/v1/chat/completions"

# ---------------------------------------------------------------------------
# Prompt de análise
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Você é um especialista em web scraping e análise de HTML de e-commerce.
Receberá uma amostra de elementos HTML brutos de cards de produtos da página de ofertas
do Mercado Livre Brasil. Sua tarefa é fazer uma análise técnica completa e detalhada.

Responda em português, em formato Markdown bem estruturado."""

USER_PROMPT_TEMPLATE = """Abaixo estão {count} cards de produtos coletados de {pages} páginas
diferentes de https://www.mercadolivre.com.br/ofertas.

Analise o HTML de todos os cards e produza um relatório técnico cobrindo obrigatoriamente:

## 1. Inventário de Campos Extraíveis
Para cada campo, informe:
- Nome do campo
- Seletor CSS mais confiável
- Tipo de dado (texto, número, booleano, URL...)
- Se é obrigatório ou opcional (com % de presença estimada)
- Exemplo de valor extraído

## 2. Padrões de Preço
Documente todos os padrões encontrados:
- Preço regular (só um preço)
- Preço com desconto (preço atual + preço riscado)
- Preço Pix/boleto (desconto adicional por meio de pagamento)
- Como distinguir cada caso pelo HTML
- Estrutura exata das classes andes-money-amount (fraction, cents, discount)
- Edge cases e variações observadas

## 3. Padrões de Parcelamento
- Como o parcelamento aparece no HTML
- Formato: "X x R$ Y sem juros" vs "X x R$ Y"
- Como extrair: número de parcelas, valor por parcela, se tem juros
- Relação entre preço parcelado e preço à vista

## 4. Badges e Destaques
- Quais badges aparecem (ex: "Oferta do dia", "Mais vendido", etc.)
- Seletores e variações de cada badge
- Frequência aproximada

## 5. Outros Dados Relevantes
- Frete grátis: como detectar, variações no texto
- Avaliações: estrutura do HTML, quando está ausente
- Imagens: atributos src vs data-src, lazy loading
- Layout do card: variações de estrutura observadas (poly-card vs outros)
- Atributos de dados úteis (data-*, aria-*, etc.)

## 6. Qualidade e Confiabilidade
- Campos com maior variação/instabilidade
- Casos onde seletores podem falhar
- Recomendações de seletores alternativos (fallbacks)

## 7. Dados Não Capturados Atualmente
Identifique campos visíveis no HTML que o projeto ainda não extrai
mas que poderiam ser úteis.

---

{cards_block}"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def find_latest_run() -> Path | None:
    """Retorna o diretório da execução mais recente em debug/ofertas/."""
    if not OFERTAS_DIR.exists():
        return None
    runs = sorted(
        [
            d
            for d in OFERTAS_DIR.iterdir()
            if d.is_dir() and ((d / "cards.json").exists() or (d / "index.html").exists())
        ],
        reverse=True,
    )
    return runs[0] if runs else None


def _extract_cards_from_html(html_path: Path) -> list[dict]:
    """
    Fallback: extrai dados dos cards a partir do index.html gerado pelo scraper.
    Usado quando cards.json ainda não existe (execuções antigas).
    """
    print("cards.json não encontrado — extraindo dados do index.html...")
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "lxml")
    cards: list[dict] = []

    for card_div in soup.select("div.card[id^='card-']"):
        ml_id = card_div.get("id", "").removeprefix("card-")
        if not ml_id:
            continue

        # Número da página
        page_span = card_div.select_one(".card-header span:not(.ml-id)")
        page = 0
        if page_span:
            text = page_span.get_text()
            try:
                page = int(text.replace("pág.", "").strip())
            except ValueError:
                pass

        # URL
        link = card_div.select_one(".card-header a[href]")
        url = str(link.get("href", "")) if link else ""

        # Título
        title_div = card_div.select_one(".card-title")
        title = title_div.get_text(strip=True) if title_div else ""

        # HTML do card (estava escaped dentro do <pre>)
        pre = card_div.select_one(f"#html-{ml_id} pre")
        import html as html_module
        card_html = html_module.unescape(pre.get_text()) if pre else ""

        if card_html:
            cards.append({"ml_id": ml_id, "page": page, "title": title, "url": url, "card_html": card_html})

    print(f"Extraídos {len(cards)} cards do index.html")
    return cards


def load_cards(run_dir: Path) -> list[dict]:
    """Carrega cards.json do diretório de execução, com fallback para index.html."""
    json_path = run_dir / "cards.json"
    if json_path.exists():
        with json_path.open(encoding="utf-8") as f:
            return json.load(f)

    html_path = run_dir / "index.html"
    if html_path.exists():
        cards = _extract_cards_from_html(html_path)
        # Persiste para uso futuro
        json_path.write_text(json.dumps(cards, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"cards.json gerado em: {json_path}")
        return cards

    raise FileNotFoundError(
        f"Nem cards.json nem index.html encontrados em {run_dir}.\n"
        "Execute primeiro: python scripts/screenshot_ofertas.py"
    )


def sample_cards(cards: list[dict], n: int) -> list[dict]:
    """
    Seleciona n cards distribuídos uniformemente entre as páginas.
    Garante diversidade: não pega todos de uma só página.
    """
    if len(cards) <= n:
        return cards

    # Agrupa por página
    by_page: dict[int, list[dict]] = {}
    for card in cards:
        by_page.setdefault(card["page"], []).append(card)

    pages = sorted(by_page.keys())
    per_page = max(1, n // len(pages))
    sampled: list[dict] = []

    for page in pages:
        page_cards = by_page[page]
        take = min(per_page, len(page_cards))
        sampled.extend(random.sample(page_cards, take))
        if len(sampled) >= n:
            break

    # Completa até n se sobrou espaço
    if len(sampled) < n:
        remaining = [c for c in cards if c not in sampled]
        extra = random.sample(remaining, min(n - len(sampled), len(remaining)))
        sampled.extend(extra)

    return sampled[:n]


def build_cards_block(cards: list[dict]) -> str:
    """Formata os cards HTML em blocos delimitados para o prompt."""
    blocks: list[str] = []
    for i, card in enumerate(cards, 1):
        blocks.append(
            f"### Card {i} — {card['ml_id']} (página {card['page']})\n"
            f"Título: {card['title']}\n"
            f"URL: {card['url']}\n"
            f"```html\n{card['card_html']}\n```"
        )
    return "\n\n---\n\n".join(blocks)


# ---------------------------------------------------------------------------
# Chamada à API
# ---------------------------------------------------------------------------


async def call_openrouter(prompt_user: str) -> str:
    """Envia prompt ao Claude via OpenRouter e retorna o texto da resposta."""
    api_key = settings.openrouter.api_key
    if not api_key:
        raise RuntimeError(
            "OPENROUTER_API_KEY não configurada. "
            "Adicione ao .env: OPENROUTER_API_KEY=sk-or-..."
        )

    logger.info("calling_api", model=MODEL)

    async with httpx.AsyncClient(timeout=300.0) as client:
        resp = await client.post(
            API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://crivo.ai",
                "X-Title": "Crivo",
            },
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt_user},
                ],
                "temperature": 0.2,
                "max_tokens": 8000,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    usage = data.get("usage", {})
    logger.info(
        "api_response_received",
        input_tokens=usage.get("prompt_tokens"),
        output_tokens=usage.get("completion_tokens"),
    )

    return data["choices"][0]["message"]["content"]


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def main(run_id: str | None = None, sample_size: int = SAMPLE_SIZE) -> None:
    # Localiza execução
    if run_id:
        run_dir = OFERTAS_DIR / run_id
        if not run_dir.exists():
            print(f"Execução '{run_id}' não encontrada em {OFERTAS_DIR}")
            sys.exit(1)
    else:
        run_dir = find_latest_run()
        if not run_dir:
            print(
                "Nenhuma execução com cards.json encontrada.\n"
                "Execute primeiro: python scripts/screenshot_ofertas.py"
            )
            sys.exit(1)

    print(f"Usando execução: {run_dir.name}")

    # Carrega e amostra cards
    all_cards = load_cards(run_dir)
    pages_available = len({c["page"] for c in all_cards})
    print(f"Cards disponíveis: {len(all_cards)} em {pages_available} páginas")

    sampled = sample_cards(all_cards, sample_size)
    pages_sampled = len({c["page"] for c in sampled})
    print(f"Amostra: {len(sampled)} cards de {pages_sampled} páginas")

    # Monta prompt
    cards_block = build_cards_block(sampled)
    prompt_user = USER_PROMPT_TEMPLATE.format(
        count=len(sampled),
        pages=pages_sampled,
        cards_block=cards_block,
    )

    # Estimativa de tokens (regra prática: ~1 token / 4 chars)
    estimated_tokens = len(prompt_user) // 4
    print(f"Tokens estimados no prompt: ~{estimated_tokens:,}")
    print("Enviando para análise... (pode levar até 2 min)")

    # Chama API
    analysis = await call_openrouter(prompt_user)

    # Salva resultado
    output_path = run_dir / "analysis.md"
    header = (
        f"# Análise de Cards — Ofertas Mercado Livre\n\n"
        f"**Execução:** {run_dir.name}  \n"
        f"**Cards analisados:** {len(sampled)} de {len(all_cards)} "
        f"({pages_sampled} páginas)  \n"
        f"**Modelo:** {MODEL}\n\n---\n\n"
    )
    output_path.write_text(header + analysis, encoding="utf-8")

    print(f"\nAnálise salva: {output_path}")
    print(f"Abra com: open '{output_path}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analisa cards de ofertas do ML via IA"
    )
    parser.add_argument("--run", help="ID da execução (ex: 2026-03-26_13-23-13)")
    parser.add_argument(
        "--sample", type=int, default=SAMPLE_SIZE, help="Nº de cards (padrão: 100)"
    )
    args = parser.parse_args()

    asyncio.run(main(run_id=args.run, sample_size=args.sample))
