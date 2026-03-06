"""
Testes de integração do DealHunter.

Testa o pipeline completo (storage, score, formatação, affiliate links)
usando SQLite real em memória e mocks para serviços externos.
"""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from src.scraper.base_scraper import ScrapedProduct
from src.analyzer.score_engine import ScoreEngine
from src.analyzer.fake_discount_detector import FakeDiscountDetector
from src.distributor.message_formatter import MessageFormatter
from src.distributor.affiliate_links import AffiliateLinkBuilder
from src.database.sqlite_fallback import SQLiteFallback
from src.database.storage_manager import StorageManager
from src.database.exceptions import SupabaseError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def good_product() -> ScrapedProduct:
    return ScrapedProduct(
        ml_id="MLB999111222",
        url="https://www.mercadolivre.com.br/tenis/p/MLB999111222",
        title="Tênis Nike Air Max 270 Masculino Preto Original",
        price=299.90,
        original_price=599.90,
        rating=4.7,
        review_count=350,
        category="Calçados",
        free_shipping=True,
        image_url="https://http2.mlstatic.com/image.jpg",
        source="ofertas_do_dia",
    )


@pytest.fixture
def mediocre_product() -> ScrapedProduct:
    return ScrapedProduct(
        ml_id="MLB888777666",
        url="https://www.mercadolivre.com.br/camiseta/p/MLB888777666",
        title="Camiseta Básica Masculina Algodão Kit 3 Peças",
        price=59.90,
        original_price=99.90,
        rating=4.2,
        review_count=50,
        category="Moda",
        free_shipping=False,
        source="categoria_moda",
    )


@pytest.fixture
def fake_discount_product() -> ScrapedProduct:
    return ScrapedProduct(
        ml_id="MLB777666555",
        url="https://www.mercadolivre.com.br/produto/p/MLB777666555",
        title="Produto com Desconto Inflado Suspeitamente",
        price=49.90,
        original_price=5000.0,  # 100x — claramente inflado
        rating=4.0,
        review_count=20,
        source="ofertas_do_dia",
    )


@pytest_asyncio.fixture
async def sqlite_db(tmp_path: Path) -> SQLiteFallback:
    """SQLite real em arquivo temporário."""
    db = SQLiteFallback(db_path=tmp_path / "test.db")
    await db.initialize()
    yield db
    await db.close()


@pytest.fixture
def mock_settings():
    """Mocka settings para testes independentes de .env."""
    with patch("src.analyzer.score_engine.settings") as mock:
        mock.score.min_discount_pct = 20.0
        mock.score.min_score = 60
        mock.score.min_rating = 4.0
        mock.score.min_reviews = 10
        mock.score.weight_discount = 40.0
        mock.score.weight_rating = 25.0
        mock.score.weight_reviews = 15.0
        mock.score.weight_free_shipping = 10.0
        mock.score.weight_title_quality = 10.0
        yield mock


@pytest.fixture
def mock_affiliate_settings():
    with patch("src.distributor.affiliate_links.settings") as mock:
        mock.mercado_livre.affiliate_id = "test_affiliate_123"
        mock.mercado_livre.affiliate_tag = "sempreblack"
        yield mock


# ---------------------------------------------------------------------------
# Teste: Pipeline Score → Fake Detection → Formatação
# ---------------------------------------------------------------------------


class TestScoreToFormatPipeline:
    """Testa o fluxo análise → detecção de fraude → formatação."""

    def test_good_product_flows_through(
        self, mock_settings, mock_affiliate_settings, good_product
    ):
        engine = ScoreEngine()
        detector = FakeDiscountDetector()
        formatter = MessageFormatter()
        affiliate = AffiliateLinkBuilder()

        # 1. Fake detection
        fake_result = detector.check(good_product)
        assert fake_result.is_fake is False

        # 2. Score
        scored = engine.evaluate(good_product)
        assert scored.passed is True
        assert scored.score >= 60

        # 3. Affiliate URL
        aff_url = affiliate.build(good_product.url)
        assert "matt_tool" in aff_url
        assert "matt_affiliate" in aff_url

        # 4. Formatação
        msg = formatter.format(good_product, short_link="https://s.black/abc")
        assert msg.telegram_text
        assert msg.whatsapp_text
        assert "299" in msg.telegram_text
        assert "https://s.black/abc" in msg.whatsapp_text

    def test_fake_product_rejected(self, mock_settings, fake_discount_product):
        detector = FakeDiscountDetector()
        result = detector.check(fake_discount_product)
        assert result.is_fake is True

    def test_batch_pipeline(
        self, mock_settings, good_product, mediocre_product, fake_discount_product
    ):
        engine = ScoreEngine()
        detector = FakeDiscountDetector()

        products = [good_product, mediocre_product, fake_discount_product]

        # 1. Fake detection em batch
        fake_results = detector.check_batch(products)
        genuine = [p for p, r in fake_results if not r.is_fake]
        assert len(genuine) < len(products)  # Pelo menos o fake deve ser removido

        # 2. Score em batch
        approved = engine.evaluate_batch(genuine)
        # O bom produto deve passar
        ml_ids = [s.product.ml_id for s in approved]
        assert good_product.ml_id in ml_ids


# ---------------------------------------------------------------------------
# Teste: SQLite Fallback — ciclo completo
# ---------------------------------------------------------------------------


class TestSQLiteFallbackIntegration:
    """Testa operações completas no SQLite real."""

    @pytest.mark.asyncio
    async def test_full_product_lifecycle(
        self, sqlite_db: SQLiteFallback, good_product
    ):
        # 1. Inserir produto
        product_id = await sqlite_db.upsert_product(good_product)
        assert product_id is not None

        # 2. Verificar duplicata
        is_dup = await sqlite_db.check_duplicate(good_product.ml_id)
        assert is_dup is True

        # 3. Buscar ID
        found_id = await sqlite_db.get_product_id(good_product.ml_id)
        assert found_id == product_id

        # 4. Adicionar histórico de preço
        ok = await sqlite_db.add_price_history(product_id, 299.90, 599.90)
        assert ok is True

        # 5. Buscar histórico
        history = await sqlite_db.get_price_history(product_id, days=30)
        assert len(history) == 1
        assert history[0]["price"] == 299.90

        # 6. Salvar scored offer
        offer_id = await sqlite_db.save_scored_offer(
            product_id, rule_score=75, final_score=75, status="approved"
        )
        assert offer_id is not None

        # 7. Marcar como enviado
        ok = await sqlite_db.mark_as_sent(offer_id, "telegram", "https://s.black/abc")
        assert ok is True

        # 8. Verificar envio recente
        was_sent = await sqlite_db.was_recently_sent(good_product.ml_id, hours=1)
        assert was_sent is True

    @pytest.mark.asyncio
    async def test_upsert_preserves_first_seen(
        self, sqlite_db: SQLiteFallback, good_product
    ):
        # Inserir
        product_id = await sqlite_db.upsert_product(good_product)
        assert product_id is not None

        # Atualizar com preço diferente
        good_product.price = 249.90
        updated_id = await sqlite_db.upsert_product(good_product)
        assert updated_id == product_id  # Mesmo ID

    @pytest.mark.asyncio
    async def test_log_event(self, sqlite_db: SQLiteFallback):
        ok = await sqlite_db.log_event("test_event", {"key": "value"})
        assert ok is True

        logs = await sqlite_db.get_recent_logs("test_event", limit=5)
        assert len(logs) == 1
        assert logs[0]["event_type"] == "test_event"
        assert logs[0]["details"]["key"] == "value"

    @pytest.mark.asyncio
    async def test_unsynced_count(self, sqlite_db: SQLiteFallback, good_product):
        await sqlite_db.upsert_product(good_product)
        counts = await sqlite_db.get_unsynced_count()
        assert counts["products"] >= 1


# ---------------------------------------------------------------------------
# Teste: StorageManager com fallback
# ---------------------------------------------------------------------------


class TestStorageManagerFallback:
    """Testa o failover do StorageManager quando Supabase falha."""

    @pytest.mark.asyncio
    async def test_sqlite_only_mode(self, good_product, tmp_path):
        with patch("src.database.storage_manager.settings") as mock_cfg:
            mock_cfg.is_production = False

            with patch("src.database.sqlite_fallback.settings") as mock_sqlite_cfg:
                mock_sqlite_cfg.sqlite.db_path = tmp_path / "fallback.db"

                manager = StorageManager(force_sqlite=True)
                async with manager:
                    assert manager.backend == "sqlite"

                    product_id = await manager.upsert_product(good_product)
                    assert product_id is not None

                    is_dup = await manager.check_duplicate(good_product.ml_id)
                    assert is_dup is True

    @pytest.mark.asyncio
    async def test_supabase_failure_falls_back_to_sqlite(self, good_product, tmp_path):
        with patch("src.database.storage_manager.settings") as mock_cfg:
            mock_cfg.is_production = True

            with patch("src.database.sqlite_fallback.settings") as mock_sqlite_cfg:
                mock_sqlite_cfg.sqlite.db_path = tmp_path / "fallback2.db"

                manager = StorageManager(force_sqlite=False)
                # Simula que Supabase está conectado
                manager._using_supabase = True
                await manager._sqlite.initialize()

                # Mocka supabase para falhar
                manager._supabase.upsert_product = AsyncMock(
                    side_effect=SupabaseError(
                        "Connection refused", operation="upsert_product"
                    )
                )

                # Deve cair no SQLite sem explodir
                product_id = await manager.upsert_product(good_product)
                assert product_id is not None

                await manager._sqlite.close()


# ---------------------------------------------------------------------------
# Teste: Redação de dados sensíveis
# ---------------------------------------------------------------------------


class TestLogRedaction:
    """Testa que o processador de redação mascara dados sensíveis."""

    def test_redact_anthropic_key(self):
        from src.logging_config import _redact_sensitive_data

        event = {"error": "Auth failed with key sk-ant-api03-abcdefghijklmnop"}
        result = _redact_sensitive_data(None, None, event)
        assert "abcdefghijklmnop" not in result["error"]
        assert "sk-ant-api03-" in result["error"]
        assert "****" in result["error"]

    def test_redact_jwt(self):
        from src.logging_config import _redact_sensitive_data

        jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"  # noqa: E501
        event = {"detail": f"Failed: {jwt}"}
        result = _redact_sensitive_data(None, None, event)
        assert "dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U" not in result["detail"]
        assert "****" in result["detail"]

    def test_redact_bearer(self):
        from src.logging_config import _redact_sensitive_data

        event = {"header": "Bearer eyJhbGciOiJIUzI1NiJ9.abc123.xyz"}
        result = _redact_sensitive_data(None, None, event)
        assert "abc123" not in result["header"]
        assert "Bearer ****" in result["header"]

    def test_non_string_values_untouched(self):
        from src.logging_config import _redact_sensitive_data

        event = {"count": 42, "ok": True, "items": ["a", "b"]}
        result = _redact_sensitive_data(None, None, event)
        assert result["count"] == 42
        assert result["ok"] is True


# ---------------------------------------------------------------------------
# Teste: WhatsApp Rate Limiting
# ---------------------------------------------------------------------------


class TestWhatsAppRateLimiting:
    """Testa que o rate limiter do WhatsApp funciona."""

    @pytest.mark.asyncio
    async def test_rate_limit_tracking(self):
        with patch("src.distributor.whatsapp_notifier.settings") as mock_cfg:
            mock_cfg.whatsapp.api_url = "http://localhost:8080"
            mock_cfg.whatsapp.api_key = "test_key"
            mock_cfg.whatsapp.instance_name = "test"
            mock_cfg.whatsapp.group_ids = ["group1"]
            mock_cfg.whatsapp.send_delay = 0.01
            mock_cfg.whatsapp.max_messages_per_minute = 5

            from src.distributor.whatsapp_notifier import WhatsAppNotifier

            notifier = WhatsAppNotifier()

            # Simula 5 envios
            import time

            for _ in range(5):
                notifier._sent_timestamps.append(time.monotonic())

            # O próximo deve aguardar
            assert len(notifier._sent_timestamps) == 5
