"""
Crivo — Screenshot de Ofertas do Mercado Livre
Navega pelas primeiras 20 páginas de /ofertas, tira screenshot de cada card
e salva o HTML bruto do elemento. Tudo consolidado em um único relatório HTML.

Uso:
    python scripts/screenshot_ofertas.py

Saída:
    debug/ofertas/{run_id}/index.html
"""

from __future__ import annotations

import asyncio
import html as html_module
import json
import random
import sys
from datetime import datetime
from pathlib import Path
from typing import TypedDict
from urllib.parse import parse_qs, urlparse

# Garante que o pacote src seja encontrado quando executado da raiz do projeto
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import structlog  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402
from playwright.async_api import Page  # noqa: E402

from src.scraper.base_scraper import BASE_HEADERS  # noqa: E402
from src.scraper.ml_scraper import OFERTAS_URL, SELECTORS, MLScraper, ScrapeSource  # noqa: E402

logger = structlog.get_logger(__name__)

MAX_PAGES = 20
OUTPUT_DIR = ROOT_DIR / "debug" / "ofertas"


# ---------------------------------------------------------------------------
# Estrutura de dado por card
# ---------------------------------------------------------------------------


class CardData(TypedDict):
    ml_id: str
    page: int
    title: str
    url: str
    screenshot: bytes   # PNG raw
    card_html: str      # outer HTML do elemento BeautifulSoup


# ---------------------------------------------------------------------------
# Scraper especializado (sem bloqueio de imagens)
# ---------------------------------------------------------------------------


def _resolve_tracking_url(url: str) -> str:
    """
    Cards patrocinados usam URLs de tracking via click1.mercadolivre.com.br/mclics/...
    A URL real do produto fica no parâmetro ?url= dentro dessa URL.
    Retorna a URL real se encontrada, caso contrário retorna a URL original.
    """
    if "click1.mercadolivre" not in url and "mclics" not in url:
        return url
    try:
        qs = parse_qs(urlparse(url).query)
        real = qs.get("url", [None])[0]
        return real if real else url
    except Exception:
        return url


class OfertasScreenshotter(MLScraper):
    """
    Herda MLScraper mas remove o bloqueio de imagens do contexto Playwright.
    Necessário para que as fotos dos produtos apareçam nos screenshots dos cards.
    """

    async def _new_context(self) -> None:
        """Cria contexto Playwright com anti-bot MAS sem bloquear imagens."""
        user_agent = self._pick_user_agent()
        vp: dict[str, int] = random.choice(
            [
                {"width": 1920, "height": 1080},
                {"width": 1440, "height": 900},
                {"width": 1366, "height": 768},
                {"width": 1280, "height": 800},
            ]
        )

        if self._browser is None:
            raise RuntimeError("Browser not started")

        self._context = await self._browser.new_context(
            user_agent=user_agent,
            viewport={"width": vp["width"], "height": vp["height"]},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers=BASE_HEADERS,
            # SEM route de bloqueio de imagens — necessário para screenshots visuais
        )

        # Injeta script para esconder sinais de automação
        await self._context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] });
            window.chrome = { runtime: {} };
        """
        )

    async def collect_all_cards(self) -> list[CardData]:
        """
        Navega até MAX_PAGES páginas de /ofertas e coleta screenshot + HTML de cada card.
        Retorna lista de CardData ordenada por página.
        """
        source = ScrapeSource("ofertas", OFERTAS_URL, max_pages=MAX_PAGES)
        all_cards: dict[str, CardData] = {}

        async with self:
            page = await self._new_page()
            try:
                url: str | None = source.url
                page_num = 0

                while url and page_num < MAX_PAGES:
                    page_num += 1
                    logger.info("scraping_page", page=page_num, url=url)

                    success = await self._goto(page, url)
                    if not success:
                        logger.warning("page_load_failed", page=page_num)
                        break

                    # Aguarda cards aparecerem
                    try:
                        await page.wait_for_selector(
                            "div.poly-card, li.promotion-item, li.ui-search-layout__item",
                            timeout=15_000,
                        )
                    except Exception:
                        logger.warning("no_cards_found", page=page_num)
                        break

                    # Espera imagens carregarem antes de tirar screenshots
                    await page.wait_for_load_state("networkidle", timeout=10_000)

                    await self._human_scroll(page)

                    # Coleta cards desta página
                    page_cards = await self._collect_page_cards(page, page_num)
                    for card in page_cards:
                        if card["ml_id"] not in all_cards:
                            all_cards[card["ml_id"]] = card

                    logger.info(
                        "page_done",
                        page=page_num,
                        new_cards=len(page_cards),
                        total=len(all_cards),
                    )

                    # Próxima página
                    next_url = await self._get_next_page_url(page)
                    if not next_url:
                        logger.info("no_more_pages", stopped_at=page_num)
                        break
                    url = next_url
                    await self._random_delay()

            finally:
                await page.close()

        return list(all_cards.values())

    async def _collect_page_cards(self, page: Page, page_num: int) -> list[CardData]:
        """Coleta screenshot + HTML de todos os cards de uma página."""
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        items = soup.select(SELECTORS["card"])

        # Mapeia ml_id → índice DOM (igual a _screenshot_cards do MLScraper)
        index_map: dict[str, int] = {}
        item_map: dict[str, BeautifulSoup] = {}
        for idx, item in enumerate(items):
            link_tag = item.select_one(SELECTORS["link"])
            if not link_tag:
                continue
            raw_url = str(link_tag.get("href", ""))
            mid = self._extract_ml_id(raw_url)
            if mid:
                index_map[mid] = idx
                item_map[mid] = item

        card_locator = page.locator(SELECTORS["card"])
        card_count = await card_locator.count()

        results: list[CardData] = []
        for ml_id, idx in index_map.items():
            if idx >= card_count:
                continue

            item = item_map[ml_id]

            # Extrai título e URL para o relatório
            title_tag = item.select_one(SELECTORS["title"])
            title = title_tag.get_text(strip=True) if title_tag else ""
            link_tag = item.select_one(SELECTORS["link"])
            raw_url = str(link_tag.get("href", "")) if link_tag else ""
            if raw_url.startswith("/"):
                raw_url = self.full_url(raw_url)
            raw_url = _resolve_tracking_url(raw_url)

            # Screenshot do card via Playwright
            try:
                screenshot_bytes = await card_locator.nth(idx).screenshot()
            except Exception as exc:
                logger.debug("screenshot_failed", ml_id=ml_id, error=str(exc))
                screenshot_bytes = b""

            results.append(
                CardData(
                    ml_id=ml_id,
                    page=page_num,
                    title=title,
                    url=raw_url,
                    screenshot=screenshot_bytes,
                    card_html=str(item),  # outer HTML completo do elemento
                )
            )

        logger.debug("collected_cards", page=page_num, count=len(results))
        return results


# ---------------------------------------------------------------------------
# Geração do relatório HTML
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #f0f2f5;
    padding: 24px;
    color: #333;
}
h1 { font-size: 22px; margin-bottom: 6px; }
.meta { color: #666; font-size: 13px; margin-bottom: 20px; }
.summary {
    background: #fff;
    padding: 14px 18px;
    border-radius: 8px;
    margin-bottom: 24px;
    display: flex;
    gap: 32px;
    flex-wrap: wrap;
    box-shadow: 0 1px 3px rgba(0,0,0,.08);
}
.summary-item { text-align: center; }
.summary-item .val { font-size: 28px; font-weight: 700; color: #3b82f6; }
.summary-item .lbl { font-size: 11px; color: #888; text-transform: uppercase; }
.page-section { margin-bottom: 32px; }
.page-title {
    font-size: 16px;
    font-weight: 700;
    color: #555;
    margin-bottom: 14px;
    padding-bottom: 6px;
    border-bottom: 2px solid #e2e8f0;
}
.grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
    gap: 18px;
}
.card {
    background: #fff;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,.10);
}
.card-header {
    background: #f8fafc;
    border-bottom: 1px solid #e2e8f0;
    padding: 8px 12px;
    font-size: 11px;
    color: #64748b;
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
}
.card-header .ml-id {
    font-family: monospace;
    font-weight: 700;
    color: #3b82f6;
}
.card-header a {
    color: #3b82f6;
    text-decoration: none;
    margin-left: auto;
}
.card-header a:hover { text-decoration: underline; }
.card-img { background: #fafafa; }
.card-img img { width: 100%; display: block; }
.card-img .no-img {
    color: #aaa;
    font-size: 12px;
    padding: 40px 0;
    text-align: center;
}
.card-title {
    padding: 10px 12px 4px;
    font-size: 12px;
    color: #374151;
    line-height: 1.4;
}
.card-html details {
    border-top: 1px solid #f0f0f0;
}
.card-html summary {
    padding: 8px 12px;
    font-size: 11px;
    color: #94a3b8;
    cursor: pointer;
    user-select: none;
}
.card-html summary:hover { background: #f8fafc; }
.card-html pre {
    margin: 0;
    padding: 10px 12px;
    font-size: 10px;
    color: #475569;
    background: #f8fafc;
    overflow-x: auto;
    border-top: 1px solid #e2e8f0;
    white-space: pre-wrap;
    word-break: break-all;
    max-height: 300px;
    overflow-y: auto;
}
"""


def _build_card_html(card: CardData, img_src: str) -> str:
    if img_src:
        img_html = f'<img src="{img_src}" alt="card {card["ml_id"]}" loading="lazy">'
    else:
        img_html = '<div class="no-img">screenshot não disponível</div>'

    title_short = (card["title"][:80] + "…") if len(card["title"]) > 80 else card["title"]
    card_html_escaped = html_module.escape(card["card_html"])
    ml_id = card["ml_id"]
    url = card["url"]

    return f"""
  <div class="card" id="card-{ml_id}">
    <div class="card-header">
      <span class="ml-id">{ml_id}</span>
      <span>pág. {card["page"]}</span>
      {f'<a href="{url}" target="_blank" rel="noopener">ver produto</a>' if url else ""}
    </div>
    <div class="card-img">{img_html}</div>
    <div class="card-title">{html_module.escape(title_short)}</div>
    <div class="card-html">
      <details id="html-{ml_id}">
        <summary>HTML do card ({len(card["card_html"])} chars)</summary>
        <pre>{card_html_escaped}</pre>
      </details>
    </div>
  </div>"""


def generate_report(cards: list[CardData], run_id: str) -> Path:
    """
    Gera relatório HTML único com screenshots e HTML de todos os cards.

    Screenshots são salvos como arquivos PNG individuais em images/{ml_id}.png
    para manter o HTML pequeno e abrível no browser.
    """
    output_dir = OUTPUT_DIR / run_id
    images_dir = output_dir / "images"
    output_dir.mkdir(parents=True, exist_ok=True)
    images_dir.mkdir(exist_ok=True)

    # Salva cada screenshot como PNG e mapeia ml_id → caminho relativo
    img_paths: dict[str, str] = {}
    for card in cards:
        if card["screenshot"]:
            img_file = images_dir / f"{card['ml_id']}.png"
            img_file.write_bytes(card["screenshot"])
            img_paths[card["ml_id"]] = f"images/{card['ml_id']}.png"

    # Agrupa por página
    pages: dict[int, list[CardData]] = {}
    for card in cards:
        pages.setdefault(card["page"], []).append(card)

    sections_html = ""
    for page_num in sorted(pages.keys()):
        page_cards = pages[page_num]
        cards_html = "\n".join(
            _build_card_html(c, img_paths.get(c["ml_id"], "")) for c in page_cards
        )
        sections_html += f"""
<div class="page-section">
  <div class="page-title">Página {page_num} — {len(page_cards)} cards</div>
  <div class="grid">
{cards_html}
  </div>
</div>"""

    with_screenshot = sum(1 for c in cards if c["screenshot"])
    run_label = run_id.replace("_", " ")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Crivo — Ofertas ML {run_label}</title>
<style>{_CSS}</style>
</head>
<body>

<h1>Ofertas do Mercado Livre</h1>
<p class="meta">Execução: <strong>{run_label}</strong></p>

<div class="summary">
  <div class="summary-item">
    <div class="val">{len(cards)}</div>
    <div class="lbl">Cards coletados</div>
  </div>
  <div class="summary-item">
    <div class="val">{with_screenshot}</div>
    <div class="lbl">Com screenshot</div>
  </div>
  <div class="summary-item">
    <div class="val">{len(pages)}</div>
    <div class="lbl">Páginas</div>
  </div>
  <div class="summary-item">
    <div class="val">{len(cards) - with_screenshot}</div>
    <div class="lbl">Sem screenshot</div>
  </div>
</div>

{sections_html}

</body>
</html>"""

    report_path = output_dir / "index.html"
    report_path.write_text(html, encoding="utf-8")

    # Salva JSON com todos os dados (sem bytes de screenshot) para análise posterior
    json_data = [
        {
            "ml_id": c["ml_id"],
            "page": c["page"],
            "title": c["title"],
            "url": c["url"],
            "card_html": c["card_html"],
        }
        for c in cards
    ]
    json_path = output_dir / "cards.json"
    json_path.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("cards_json_saved", path=str(json_path), count=len(json_data))

    return report_path


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def main() -> None:
    run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logger.info("start", run_id=run_id, max_pages=MAX_PAGES)

    scraper = OfertasScreenshotter()
    cards = await scraper.collect_all_cards()

    if not cards:
        logger.error("no_cards_collected")
        sys.exit(1)

    report_path = generate_report(cards, run_id)
    logger.info(
        "done",
        cards=len(cards),
        report=str(report_path),
    )
    print(f"\nRelatório gerado: {report_path}")
    print(f"Total de cards: {len(cards)}")


if __name__ == "__main__":
    asyncio.run(main())
