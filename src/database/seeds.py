"""
DealHunter — Dados de Seed (Fonte Única de Verdade)

Define os badges e categorias canônicos que devem existir em TODOS os
ambientes (Supabase e SQLite). Usados durante a inicialização dos bancos.

Para adicionar um novo badge ou categoria, basta editar as listas abaixo.
O seeding é idempotente (ON CONFLICT DO NOTHING / INSERT OR IGNORE).
"""

# Badges conhecidos do Mercado Livre
BADGES: list[str] = [
    "Oferta do dia",
    "Oferta relâmpago",
    "Mais vendido",
    "Oferta imperdível",
]

# Categorias do Mercado Livre + "Outros" (fallback)
CATEGORIES: list[str] = [
    "Acessórios para Veículos",
    "Agro",
    "Alimentos e Bebidas",
    "Antiguidades e Coleções",
    "Arte, Papelaria e Armarinho",
    "Bebês",
    "Beleza e Cuidado Pessoal",
    "Brinquedos e Hobbies",
    "Calçados, Roupas e Bolsas",
    "Câmeras e Acessórios",
    "Carros, Motos e Outros",
    "Casa, Móveis e Decoração",
    "Celulares e Telefones",
    "Construção",
    "Eletrodomésticos",
    "Eletrônicos, Áudio e Vídeo",
    "Esportes e Fitness",
    "Ferramentas",
    "Festas e Lembrancinhas",
    "Games",
    "Imóveis",
    "Indústria e Comércio",
    "Informática",
    "Ingressos",
    "Instrumentos Musicais",
    "Joias e Relógios",
    "Livros, Revistas e Comics",
    "Música, Filmes e Seriados",
    "Pet Shop",
    "Saúde",
    "Serviços",
    "Outros",
]
