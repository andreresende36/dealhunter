import json
import re
from pathlib import Path

import httpx
import structlog

from src.config import settings
from src.utils.prompts_loader import load_prompt

_CLASSIFIER_USER_TEMPLATE = load_prompt("classifier_user")
_GENDER_USER_TEMPLATE = load_prompt("gender_user")

logger = structlog.get_logger(__name__)

# Base path to the categories JSON
DATA_DIR = Path(__file__).parent.parent.parent / "data"
CATEGORIES_FILE = DATA_DIR / "ml_main_categories_all_items.json"

# Keyword mapping to assign categories based on product title
KEYWORDS_MAP = {
    "Celulares e Telefones": [
        "smartphone",
        "iphone",
        "celular",
        "motorola",
        "samsung galaxy",
        "poco",
        "redmi",
        "xiaomi",
        "realme",
        "asus rog",
        "zenfone",
    ],
    "Informática": [
        "notebook",
        "monitor",
        "mouse",
        "teclado",
        "impressora",
        "placa de vídeo",
        "roteador",
        "ssd",
        "hd externo",
        "memória ram",
        "processador",
        "placa mãe",
        "gabinete",
        "pendrive",
        "macbook",
        "ipad",
        "tablet",
        "kindle",
        "cartão de memória",
    ],
    "Eletrônicos, Áudio e Vídeo": [
        "tv",
        "smart tv",
        "fita led",
        "caixa de som",
        "jbl",
        "fone de ouvido",
        "microfone",
        "projetor",
        "receptor",
        "drone",
        "amplificador",
        "soundbar",
        "headset",
        "chromecast",
        "fire tv",
        "alexa",
        "echo dot",
        "smart display",
        "toca-discos",
    ],
    "Eletrodomésticos": [
        "geladeira",
        "fogão",
        "micro-ondas",
        "lavadora",
        "ar condicionado",
        "liquidificador",
        "fritadeira",
        "air fryer",
        "aspirador",
        "batedeira",
        "cafeteira",
        "ventilador",
        "purificador",
        "freezer",
        "coifa",
        "adega",
        "forno elétrico",
        "máquina de lavar",
        "secadora",
    ],
    "Calçados, Roupas e Bolsas": [
        "tênis",
        "camiseta",
        "calça",
        "jaqueta",
        "bolsa",
        "mochila",
        "sapato",
        "sandália",
        "vestido",
        "meia",
        "cueca",
        "calcinha",
        "sutiã",
        "chinelo",
        "bota",
        "camisa",
        "bermuda",
        "saia",
        "mala",
        "moletom",
        "casaco",
        "blazer",
        "cinto",
        "boné",
    ],
    "Casa, Móveis e Decoração": [
        "sofá",
        "cama",
        "mesa",
        "cadeira",
        "guarda-roupa",
        "tapete",
        "panela",
        "faqueiro",
        "travesseiro",
        "toalha",
        "lâmpada",
        "colchão",
        "armário",
        "escrivaninha",
        "rack",
        "painel",
        "luminária",
        "quadro",
        "espelho",
        "lençol",
        "edredom",
        "cobertor",
        "cortina",
    ],
    "Beleza e Cuidado Pessoal": [
        "perfume",
        "maquiagem",
        "shampoo",
        "protetor solar",
        "creme",
        "secador",
        "sabonete",
        "prancha",
        "chapinha",
        "barbeador",
        "depilador",
        "desodorante",
        "condicionador",
        "máscara",
        "hidratante",
        "esmalte",
        "escova secadora",
        "aparador de pelos",
    ],
    "Acessórios para Veículos": [
        "pneu",
        "capacete",
        "som automotivo",
        "óleo",
        "bateria",
        "amortecedor",
        "pastilha",
        "farol",
        "retrovisor",
        "calota",
        "macaco",
        "alarme",
        "jaqueta moto",
        "baú",
        "multimídia",
        "kit relação",
        "lubrificante",
        "bomba de ar",
    ],
    "Esportes e Fitness": [
        "bicicleta",
        "whey",
        "creatina",
        "barraca",
        "bola",
        "chuteira",
        "esteira",
        "halter",
        "caneleira",
        "kimono",
        "raquete",
        "patins",
        "skate",
        "bcaa",
        "pré-treino",
        "colágeno",
        "barraca de camping",
        "saco de pancada",
        "bicicleta ergométrica",
        "mochila hidratação",
    ],
    "Ferramentas": [
        "furadeira",
        "parafusadeira",
        "serra",
        "kit de chaves",
        "torno",
        "lixadeira",
        "esmerilhadeira",
        "martelete",
        "tupia",
        "compressor",
        "solda",
        "alicate",
        "chave de fenda",
        "trena",
        "macaco hidráulico",
        "jogo de soquetes",
        "chave de impacto",
    ],
    "Construção": [
        "tinta",
        "piso",
        "torneira",
        "chuveiro",
        "fechadura",
        "porta",
        "janela",
        "cimento",
        "argamassa",
        "porcelanato",
        "verniz",
        "massa corrida",
        "tubo",
        "cabo flexível",
        "disjuntor",
        "fita isolante",
        "tomada",
        "interruptor",
        "ducha",
        "quadro de distribuição",
    ],
    "Games": [
        "playstation",
        "xbox",
        "nintendo",
        "controle",
        "jogo ",
        "console",
        "ps5",
        "ps4",
        "switch",
        "gamepad",
        "volante",
        "cadeira gamer",
        "mesa gamer",
        "joystick",
    ],
    "Pet Shop": [
        "ração",
        "coleira",
        "casinha",
        "areia",
        "gato",
        "cachorro",
        "aquário",
        "comedouro",
        "arranhador",
        "antipulgas",
        "tapete higiênico",
        "shampoo pet",
        "brinquedo pet",
        "petisco",
    ],
    "Alimentos e Bebidas": [
        "cerveja",
        "vinho",
        "café",
        "gin",
        "chocolate",
        "doce",
        "azeite",
        "whisky",
        "vodka",
        "espumante",
        "leite",
        "bombom",
        "bala",
        "salgadinho",
        "refrigerante",
        "água mineral",
        "licor",
    ],
    "Bebês": [
        "fralda",
        "berço",
        "carrinho de bebê",
        "chupeta",
        "mamadeira",
        "cadeirinha",
        "andador",
        "lenço umedecido",
        "bebê conforto",
        "babá eletrônica",
        "banheira bebê",
        "esterilizador",
    ],
    "Instrumentos Musicais": [
        "violão",
        "teclado musical",
        "guitarra",
        "bateria acústica",
        "bateria eletrônica",
        "microfone",
        "baixo",
        "ukulele",
        "pedal",
        "amplificador guitarra",
        "corda",
        "cabo p10",
        "palheta",
        "saxofone",
    ],
    "Livros, Revistas e Comics": [
        "livro",
        "mangá",
        "quadrinho",
        "bíblia",
        "hqs",
        "box livros",
        "vade mecum",
        "dicionário",
        "romance",
        "ficção",
        "biografia",
    ],
    "Joias e Relógios": [
        "relógio",
        "smartwatch",
        "corrente",
        "anel",
        "brinco",
        "óculos",
        "pulseira",
        "colar",
        "pingente",
        "aliança",
        "apple watch",
        "galaxy watch",
    ],
    "Saúde": [
        "termômetro",
        "medidor de pressão",
        "inalador",
        "oxímetro",
        "emagrece",
        "massageador",
        "muleta",
        "cadeira de rodas",
        "curativo",
        "máscara descartável",
        "aparelho de pressão",
        "nebulizador",
    ],
    "Arte, Papelaria e Armarinho": [
        "caderno",
        "caneta",
        "lápis",
        "mochila escolar",
        "estojo",
        "pincel",
        "tinta acrílica",
        "tesoura",
        "fio",
        "barbante",
        "agulha",
        "eva",
        "marca texto",
        "post-it",
        "papel sulfite",
        "fita crepe",
    ],
    "Brinquedos e Hobbies": [
        "boneca",
        "carrinho",
        "lego",
        "quebra-cabeça",
        "jogo de tabuleiro",
        "pelúcia",
        "massinha",
        "nerf",
        "hot wheels",
        "barbie",
        "pistola de água",
        "fidget",
        "action figure",
        "tabuleiro",
        "pista",
    ],
    "Agro": [
        "trator",
        "semente",
        "adubo",
        "fertilizante",
        "pulverizador",
        "mangueira",
        "motosserra",
        "roçadeira",
        "arame",
        "mourão",
        "chocadeira",
        "ordenhadeira",
        "ração animal",
        "feno",
    ],
    "Câmeras e Acessórios": [
        "câmera",
        "gopro",
        "lente",
        "tripé",
        "flash",
        "cartão sd",
        "bolsa para câmera",
        "ring light",
        "câmera de segurança",
        "filmagem",
        "estúdio",
        "polaroid",
        "sony alpha",
        "canon",
        "nikon",
    ],
}


# ---------------------------------------------------------------------------
# Classificação de gênero
# ---------------------------------------------------------------------------

# Categorias onde faz sentido classificar gênero
GENDER_RELEVANT_CATEGORIES = {
    "Calçados, Roupas e Bolsas",
    "Esportes e Fitness",
    "Joias e Relógios",
    "Beleza e Cuidado Pessoal",
    "Brinquedos e Hobbies",
}

# Keywords que determinam gênero de forma explícita no título
_GENDER_KEYWORDS: dict[str, list[str]] = {
    "Feminino": [
        "feminino", "feminina", "femininos", "femininas",
        "mulher", "mulheres", "feminil",
        "menina", "meninas",
        "plus size",
        "calcinha", "sutiã", "lingerie",
        "saia", "vestido", "blusa",
        "feminino adulto",
    ],
    "Masculino": [
        "masculino", "masculina", "masculinos", "masculinas",
        "homem", "homens", "masculil",
        "menino", "meninos",
        "cueca", "bermuda masculina",
        "masculino adulto",
    ],
    "Unissex": [
        "unissex", "unisex",
    ],
}

# Pré-compila os padrões de gênero
_GENDER_PATTERNS: list[tuple[str, re.Pattern]] = [
    (
        gender,
        re.compile(
            r"\b(" + "|".join(re.escape(w) for w in words) + r")\b",
            re.IGNORECASE,
        ),
    )
    for gender, words in _GENDER_KEYWORDS.items()
]

_VALID_GENDERS = {"Masculino", "Feminino", "Unissex", "Sem gênero"}


def get_product_gender(title: str, category: str) -> str | None:
    """
    Classifica o gênero de um produto por keywords.

    Retorna:
    - "Sem gênero"  se a categoria não é gender-relevant
    - "Feminino" | "Masculino" | "Unissex"  se keyword encontrada
    - None  se categoria é relevant mas não há keyword (precisa de IA)
    """
    if category not in GENDER_RELEVANT_CATEGORIES:
        return "Sem gênero"

    for gender, pattern in _GENDER_PATTERNS:
        if pattern.search(title):
            return gender

    return None  # incerto — cabe à IA decidir


async def classify_gender_with_ai(title: str) -> str:
    """
    Classifica o gênero de um produto via LLM (google/gemini-2.5-flash).
    Chamado apenas quando keywords não determinaram o gênero.
    Retorna um dos 4 valores válidos; fallback = "Unissex".
    """
    api_key = settings.openrouter.api_key
    if not api_key:
        logger.warning("openrouter_key_missing_gender", title=title)
        return "Unissex"

    prompt = _GENDER_USER_TEMPLATE.format(title=title)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://crivo.ai",
                    "X-Title": "Crivo",
                },
                json={
                    "model": "google/gemini-2.5-flash",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,
                    "max_tokens": 15,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            raw = data["choices"][0]["message"]["content"]
            if not raw:
                return "Unissex"
            answer = raw.strip()

            for valid in _VALID_GENDERS:
                if valid.lower() == answer.lower() or answer.lower().startswith(valid.lower()):
                    return valid

            logger.warning("ai_gender_mismatch", title=title, ai_answer=answer)
            return "Unissex"

    except Exception as e:
        logger.error("ai_gender_error", error=str(e), title=title)
        return "Unissex"


class ProductClassifier:
    """
    Classifies Mercado Livre products based on title keywords.
    Ensures zero extra network latency while matching valid categories.
    """

    def __init__(self):
        self.rules = []
        self.valid_categories = set()
        self.default_category = "Outros"
        self._load_categories()
        self._compile_rules()

    def _load_categories(self):
        """Loads valid target categories from json file."""
        try:
            if CATEGORIES_FILE.exists():
                with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.valid_categories = {c["name"] for c in data}
            else:
                logger.warning("categories_file_not_found", path=str(CATEGORIES_FILE))
                self.valid_categories = set(KEYWORDS_MAP.keys())
        except Exception as e:
            logger.error("error_loading_categories", error=str(e))
            self.valid_categories = set(KEYWORDS_MAP.keys())

    def _compile_rules(self):
        """Pre-compiles regex rules for fast execution."""
        for category, words in KEYWORDS_MAP.items():
            if category not in self.valid_categories and self.valid_categories:
                # Still compile it, but maybe warn if necessary
                pass

            # boundaries to avoid partial matches
            pattern = r"\b(" + "|".join(re.escape(w) for w in words) + r")\b"
            self.rules.append((category, re.compile(pattern, re.IGNORECASE)))

    def classify(self, title: str) -> str:
        """Classifies a product title into a standard category."""
        if not title:
            return self.default_category

        for category, regex in self.rules:
            if regex.search(title):
                # Optionally enforce that category is in valid ones,
                # but valid ones are practically static.
                if self.valid_categories and category not in self.valid_categories:
                    return self.default_category
                return category

        return self.default_category


# Single shared instance for quick access
classifier_instance = ProductClassifier()


def get_product_category(title: str) -> str:
    """Convenience function mapped to shared classifier."""
    return classifier_instance.classify(title)

async def classify_with_ai(title: str) -> str:
    """
    Classifies a product using OpenRouter LLM (google/gemini-2.5-flash).
    Returns the category name if matched, or 'Outros' on failure.
    """
    api_key = settings.openrouter.api_key
    if not api_key:
        logger.warning("openrouter_key_missing", title=title)
        return "Outros"

    valid_cats = list(classifier_instance.valid_categories)
    if not valid_cats:
        return "Outros"

    prompt = _CLASSIFIER_USER_TEMPLATE.format(
        title=title,
        categories="\n".join(f"- {c}" for c in valid_cats),
    )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://crivo.ai",
                    "X-Title": "Crivo",
                },
                json={
                    "model": "google/gemini-2.5-flash",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,
                    "max_tokens": 50,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            raw = data["choices"][0]["message"]["content"]
            if not raw:
                logger.warning("ai_classification_empty", title=title)
                return "Outros"
            answer = raw.strip()

            for cat in valid_cats:
                if cat.lower() == answer.lower() or answer.lower().startswith(
                    cat.lower()
                ):
                    return cat

            logger.warning("ai_classification_mismatch", title=title, ai_answer=answer)
            return "Outros"

    except Exception as e:
        logger.error("ai_classification_error", error=str(e), title=title)
        return "Outros"
