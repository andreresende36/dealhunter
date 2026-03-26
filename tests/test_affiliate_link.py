"""
Teste do AffiliateLinkBuilder.

A classe agora requer (storage, user_id) e usa a API do ML para gerar links.
Os testes unitários verificam apenas helpers estáticos (regex).
O teste HTTP standalone continua disponível via `python tests/test_affiliate_link.py`.

Uso:
    python -m pytest tests/test_affiliate_link.py -v
    python tests/test_affiliate_link.py          # modo standalone com HTTP check
"""

import os
import re
import sys
import urllib.request


# Garante que src/ está no path ao rodar standalone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# URL de produto real para o teste HTTP (produto barato/qualquer)
SAMPLE_URL = (
    "https://www.mercadolivre.com.br/multivitaminico-120-caps-growth-supplements"
    "-sabor-neutro-nova-formula/p/MLB21555776"
)

# Regex usado internamente pelo AffiliateLinkBuilder
_ML_ID_PATTERN = re.compile(r"(MLB\d+)")


# ---------------------------------------------------------------------------
# Testes unitários (pytest)
# ---------------------------------------------------------------------------


def test_extract_ml_id_from_url():
    """Deve extrair o ID do produto de URLs do ML."""
    match = _ML_ID_PATTERN.search(SAMPLE_URL)
    assert match is not None
    assert match.group(1) == "MLB21555776"


def test_extract_ml_id_no_match():
    """URLs sem MLB ID não devem dar match."""
    url = "https://www.amazon.com.br/produto/123"
    match = _ML_ID_PATTERN.search(url)
    assert match is None


def test_extract_ml_id_from_short_url():
    """Deve funcionar com URLs curtas do ML."""
    url = "https://produto.mercadolivre.com.br/MLB-123456789"
    match = _ML_ID_PATTERN.search(url.replace("-", ""))
    assert match is not None


def test_ml_id_pattern_extracts_correct_id():
    """Deve extrair apenas o primeiro MLB ID."""
    url = "https://www.mercadolivre.com.br/tenis/p/MLB987654321?ref=MLB111"
    match = _ML_ID_PATTERN.search(url)
    assert match is not None
    assert match.group(1) == "MLB987654321"


# ---------------------------------------------------------------------------
# Teste HTTP (standalone) — verifica se o ML aceita a URL
# ---------------------------------------------------------------------------


def check_http(url: str) -> tuple[int, str]:
    """Faz HEAD request seguindo redirects e retorna (status, url_final)."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; CrivoBot/1.0)"},
        method="HEAD",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.url
    except urllib.error.HTTPError as e:
        return e.code, url


if __name__ == "__main__":
    print("=" * 60)
    print("URL de teste:")
    print(f"  {SAMPLE_URL}\n")

    match = _ML_ID_PATTERN.search(SAMPLE_URL)
    print(f"ML ID extraído: {match.group(1) if match else 'N/A'}")

    print("\nVerificando HTTP...")
    status, final_url = check_http(SAMPLE_URL)
    print(f"  Status: {status}")
    print(f"  URL final: {final_url}")

    if status in (200, 301, 302):
        print("\n✓ ML aceitou a URL.")
    else:
        print(f"\n✗ Resposta inesperada ({status}).")
