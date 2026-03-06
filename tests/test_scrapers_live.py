"""
DealHunter — Teste REAL contra o Mercado Livre.
Abre o browser (headless), carrega a página de ofertas e valida os seletores.

Rodar com:
    .venv/bin/python -m pytest tests/test_scrapers_live.py -v -s

Flags úteis:
    -s          mostra prints/logs no console
    -k smoke    roda só o smoke test rápido (1 página)
"""

import pytest
from src.scraper.ml_scraper import MLScraper, ScrapeSource


@pytest.mark.asyncio
async def test_smoke_scrape_one_page():
    """
    Smoke test: abre 1 página de ofertas do ML e verifica que extraiu algo.
    Valida que os seletores CSS ainda funcionam no site real.
    """
    source = ScrapeSource(
        name="ofertas_do_dia",
        url="https://www.mercadolivre.com.br/ofertas",
        max_pages=1,
    )
    scraper = MLScraper(sources=[source])
    products = await scraper.scrape()

    print(f"\n{'='*60}")
    print(f"  RESULTADO: {len(products)} produtos extraídos")
    print(f"{'='*60}")

    for i, p in enumerate(products[:5], 1):
        print(f"\n  [{i}] {p.title[:60]}")
        print(f"      ML ID:     {p.ml_id}")
        print(f"      Preço:     R$ {p.price:.2f}")
        print(
            f"      Original:  R$ {p.original_price:.2f}"
            if p.original_price
            else "      Original:  —"
        )
        print(f"      Desconto:  {p.discount_pct}%")
        print(f"      Frete:     {'Grátis ✓' if p.free_shipping else 'Pago'}")
        print(f"      Imagem:    {'✓' if p.image_url else '✗'}")
        print(f"      URL:       {p.url[:80]}...")

    if len(products) > 5:
        print(f"\n  ... e mais {len(products) - 5} produtos")
    print()

    # Validações
    assert (
        len(products) > 0
    ), "Nenhum produto encontrado — seletores CSS podem estar desatualizados!"

    # Verifica campos obrigatórios em cada produto
    for p in products:
        assert p.ml_id.startswith("MLB"), f"ML ID inválido: {p.ml_id}"
        assert len(p.title) > 5, f"Título muito curto: {p.title}"
        assert p.price > 0, f"Preço inválido: {p.price}"
        assert p.url.startswith("http"), f"URL inválida: {p.url}"
        assert p.source == "ofertas_do_dia"
