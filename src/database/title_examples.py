"""
Crivo — Title Examples
Dataclasses para o sistema de feedback de títulos.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TitleExampleData:
    """Dados para salvar um exemplo de título no banco.

    product_title, category e price foram removidos da tabela title_examples
    (deriváveis via scored_offer_id → scored_offers → products).
    category_id é resolvido a partir do nome da categoria.
    """

    generated_title: str
    final_title: str
    action: str  # "approved" | "edited" | "timeout"
    scored_offer_id: str | None = None
    category_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "generated_title": self.generated_title,
            "final_title": self.final_title,
            "action": self.action,
            "scored_offer_id": self.scored_offer_id,
            "category_id": self.category_id,
        }


@dataclass
class TitleExample:
    """Exemplo carregado do banco para injeção few-shot no prompt."""

    product_title: str
    final_title: str
    action: str

    @classmethod
    def from_dict(cls, data: dict) -> TitleExample:
        return cls(
            product_title=data["product_title"],
            final_title=data["final_title"],
            action=data["action"],
        )
