"""
DealHunter — Card Debugger
Gera relatório HTML com screenshots dos cards rejeitados pelo Score Engine.

Uso (chamado automaticamente em main.py quando SCRAPER_DEBUG_SCREENSHOTS=true):
    from src.analyzer.card_debugger import generate_report
    path = generate_report(rejected_scored, screenshots, run_id)
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from src.analyzer.score_engine import ScoredProduct

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# CSS do relatório
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
    box-shadow: 0 1px 3px rgba(0,0,0,.08);
}
.summary-item { text-align: center; }
.summary-item .val { font-size: 28px; font-weight: 700; color: #e53e3e; }
.summary-item .lbl { font-size: 11px; color: #888; text-transform: uppercase; }
.grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 18px;
}
.card {
    background: #fff;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,.1);
}
.card-img {
    background: #fafafa;
    border-bottom: 1px solid #eee;
    text-align: center;
    padding: 8px;
}
.card-img img { max-width: 100%; max-height: 280px; display: inline-block; }
.card-img .no-img { color: #aaa; font-size: 12px; padding: 40px 0; }
.card-body { padding: 14px; }
.card-title {
    font-size: 12px;
    color: #555;
    margin-bottom: 10px;
    line-height: 1.4;
}
.card-title a { color: #0078d4; text-decoration: none; }
.card-title a:hover { text-decoration: underline; }
.score-line {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 8px;
}
.score-val {
    font-size: 26px;
    font-weight: 800;
    color: #e53e3e;
}
.score-min { font-size: 11px; color: #aaa; }
.reason {
    background: #fff5f5;
    border-left: 3px solid #e53e3e;
    padding: 6px 10px;
    font-size: 11px;
    color: #c53030;
    border-radius: 0 4px 4px 0;
    margin-bottom: 12px;
}
.breakdown { width: 100%; border-collapse: collapse; font-size: 12px; }
.breakdown tr:last-child td { border-bottom: none; }
.breakdown td {
    padding: 4px 6px;
    border-bottom: 1px solid #f0f0f0;
}
.breakdown td:last-child {
    text-align: right;
    font-weight: 600;
    color: #333;
}
.breakdown tr.zero td { color: #bbb; }
.badge-pill {
    display: inline-block;
    background: #edf2ff;
    color: #3b5bdb;
    border-radius: 99px;
    padding: 2px 8px;
    font-size: 10px;
    margin-bottom: 8px;
}
.badge-pill.no-badge { background: #f1f3f5; color: #adb5bd; }
"""

# ---------------------------------------------------------------------------
# Gerador
# ---------------------------------------------------------------------------


def generate_report(
    rejected: list["ScoredProduct"],
    screenshots: dict[str, bytes],
    run_id: str,
    min_score: int,
    output_dir: str = "debug/rejected",
) -> Path | None:
    """
    Gera relatório HTML com todos os cards rejeitados.

    Args:
        rejected:     Lista de ScoredProduct com passed=False.
        screenshots:  Dict {ml_id: bytes_png} dos screenshots dos cards.
        run_id:       Identificador da execução (usado como subdiretório).
        min_score:    Score mínimo configurado (para contexto no relatório).
        output_dir:   Diretório base para o relatório.

    Returns:
        Path do arquivo HTML gerado, ou None se não houver rejeitados.
    """
    if not rejected:
        return None

    report_dir = Path(output_dir) / run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    # Ordena do maior score (mais perto de passar) para o menor
    ordered = sorted(rejected, key=lambda x: x.score, reverse=True)

    cards_html = _build_cards(ordered, screenshots)
    avg_score = round(sum(s.score for s in rejected) / len(rejected), 1) if rejected else 0.0
    run_label = run_id.replace("_", " ")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DealHunter — Rejeitados {run_label}</title>
<style>{_CSS}</style>
</head>
<body>

<h1>🔍 Cards Rejeitados pelo Score Engine</h1>
<p class="meta">Execução: <strong>{run_label}</strong> &nbsp;|&nbsp;
Score mínimo configurado: <strong>{min_score}</strong></p>

<div class="summary">
  <div class="summary-item">
    <div class="val">{len(rejected)}</div>
    <div class="lbl">Rejeitados</div>
  </div>
  <div class="summary-item">
    <div class="val">{len(screenshots)}</div>
    <div class="lbl">Com screenshot</div>
  </div>
  <div class="summary-item">
    <div class="val">{avg_score}</div>
    <div class="lbl">Score médio</div>
  </div>
  <div class="summary-item">
    <div class="val">{ordered[0].score:.1f}</div>
    <div class="lbl">Score mais alto</div>
  </div>
</div>

<div class="grid">
{cards_html}
</div>

</body>
</html>
"""

    report_path = report_dir / "index.html"
    report_path.write_text(html, encoding="utf-8")

    logger.info(
        "debug_report_generated",
        path=str(report_path),
        rejected=len(rejected),
        with_screenshots=len(screenshots),
    )
    return report_path


# ---------------------------------------------------------------------------
# Auxiliares privados
# ---------------------------------------------------------------------------


def _build_cards(
    ordered: list["ScoredProduct"],
    screenshots: dict[str, bytes],
) -> str:
    html_parts: list[str] = []

    for s in ordered:
        p = s.product
        b = s.breakdown

        # --- Screenshot ---
        if p.ml_id in screenshots:
            b64 = base64.b64encode(screenshots[p.ml_id]).decode()
            img_html = f'<img src="data:image/png;base64,{b64}" alt="card {p.ml_id}">'
        else:
            img_html = '<div class="no-img">screenshot não disponível</div>'

        # --- Badge pill ---
        if p.badge:
            badge_html = f'<span class="badge-pill">{p.badge}</span>'
        else:
            badge_html = '<span class="badge-pill no-badge">sem badge</span>'

        # --- Breakdown rows ---
        criteria = [
            ("Desconto",      b.discount.final_score),
            ("Badge",         b.badge.final_score),
            ("Avaliação",     b.rating.final_score),
            ("Reviews",       b.reviews.final_score),
            ("Frete grátis",  b.free_shipping.final_score),
            ("Parcelamento",  b.installments.final_score),
            ("Título",        b.title_quality.final_score),
        ]
        rows = ""
        for label, pts in criteria:
            zero_class = ' class="zero"' if pts == 0 else ""
            rows += f"<tr{zero_class}><td>{label}</td><td>{pts:.1f} pt</td></tr>"

        # --- Metadados inline ---
        price_str = f"R$ {p.price:.2f}".replace(".", ",")
        original_str = (
            f" <s>R$ {p.original_price:.2f}</s>".replace(".", ",")
            if p.original_price
            else ""
        )
        discount_str = (
            f" &nbsp;-{p.discount_pct:.0f}%"
            if p.discount_pct
            else ""
        )

        title_short = (p.title[:70] + "…") if len(p.title) > 70 else p.title
        reason = s.reject_reason or "Rejeitado"

        # --- JSON detalhado (collapsible) ---
        debug_json = json.dumps(
            {
                "ml_id": p.ml_id,
                "score": s.score,
                "reason": reason,
                "badge": p.badge,
                "price": p.price,
                "original_price": p.original_price,
                "discount_pct": p.discount_pct,
                "rating": p.rating,
                "review_count": p.review_count,
                "free_shipping": p.free_shipping,
                "installments_without_interest": p.installments_without_interest,
                "breakdown": {
                    "discount": b.discount.final_score,
                    "badge": b.badge.final_score,
                    "rating": b.rating.final_score,
                    "reviews": b.reviews.final_score,
                    "free_shipping": b.free_shipping.final_score,
                    "installments": b.installments.final_score,
                    "title": b.title_quality.final_score,
                },
            },
            indent=2,
            ensure_ascii=False,
        )

        html_parts.append(f"""
  <div class="card">
    <div class="card-img">{img_html}</div>
    <div class="card-body">
      <p class="card-title">
        <a href="{p.url}" target="_blank">{title_short}</a><br>
        <small>{p.ml_id} &nbsp;|&nbsp; {price_str}{original_str}{discount_str}</small>
      </p>
      {badge_html}
      <div class="score-line">
        <span class="score-val">{s.score:.1f}</span>
        <span class="score-min">/ {s.breakdown.discount.max_points + s.breakdown.badge.max_points + s.breakdown.rating.max_points + s.breakdown.reviews.max_points + s.breakdown.free_shipping.max_points + s.breakdown.installments.max_points + s.breakdown.title_quality.max_points:.0f} pts</span>
      </div>
      <div class="reason">{reason}</div>
      <table class="breakdown">{rows}</table>
      <details style="margin-top:10px">
        <summary style="font-size:11px;color:#888;cursor:pointer">JSON completo</summary>
        <pre style="font-size:10px;color:#555;overflow:auto;margin-top:6px">{debug_json}</pre>
      </details>
    </div>
  </div>""")

    return "\n".join(html_parts)
