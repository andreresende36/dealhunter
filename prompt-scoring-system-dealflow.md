# Prompt: Implementar Sistema de Scoring de Ofertas do Mercado Livre (DealFlow Bot)

## Contexto do Projeto

Este módulo faz parte do **DealFlow Bot**, um sistema de automação que faz scraping de ofertas do Mercado Livre e posta as melhores no WhatsApp com links de afiliado. O módulo de scoring é responsável por atribuir uma **nota de 0 a 100** a cada oferta, permitindo ranquear e filtrar as melhores deals antes de publicar.

O sistema precisa ser implementado em **duas versões**:
1. **Módulo Python** (para uso standalone, testes e integração com scripts)
2. **Função JavaScript** (para uso direto em nós Code do n8n)

Ambas as versões devem ter lógica idêntica e produzir os mesmos resultados para os mesmos inputs.

---

## Estrutura de Dados de Entrada

Cada oferta chega como um objeto/dicionário com os seguintes campos (todos opcionais, pois o scraping pode falhar em capturar qualquer um deles):

```json
{
  "title": "Notebook Lenovo IdeaPad 3 15.6\" i5 8GB 256GB SSD",
  "original_price": 3499.00,
  "current_price": 2199.00,
  "discount_percentage": 37,
  "badge": "DEAL_OF_THE_DAY",
  "rating": 4.6,
  "review_count": 1842,
  "free_shipping": true,
  "installments": {
    "quantity": 12,
    "interest_free": true
  },
  "url": "https://www.mercadolivre.com.br/..."
}
```

**Observações sobre os dados:**
- `discount_percentage` pode vir diretamente do scraping OU ser calculado a partir de `original_price` e `current_price`
- `badge` pode vir como string variável do ML (ver mapeamento abaixo)
- `rating` é um float de 0.0 a 5.0
- `review_count` é um inteiro >= 0
- `free_shipping` é booleano
- `installments` pode ser null/undefined se não houver parcelamento, ou um objeto com `quantity` e `interest_free`
- Qualquer campo pode ser `null`, `undefined`, `""`, `0` ou simplesmente ausente

---

## Tabela de Pesos

| # | Critério               | Peso (pontos) | Tipo de Curva        |
|---|------------------------|---------------|----------------------|
| 1 | Percentual de Desconto | 30            | Sigmóide             |
| 2 | Badge                  | 15            | Escala discreta      |
| 3 | Nota de Avaliação      | 15            | Linear normalizada   |
| 4 | Número de Reviews      | 10            | Logarítmica          |
| 5 | Frete Grátis           | 10            | Binário              |
| 6 | Parcelamento sem Juros | 10            | Discreto progressivo |
| 7 | Qualidade do Título    | 10            | Heurísticas          |

**Total: 100 pontos**

---

## Detalhamento de Cada Critério

### 1. Percentual de Desconto (0–30 pontos)

**Objetivo:** Valorizar descontos reais e penalizar descontos inflados/baixos.

**Curva:** Sigmóide (logística) centrada em 35% de desconto.

**Fórmula:**
```
score = 30 / (1 + e^(-0.12 * (desconto - 35)))
```

**Onde:**
- `desconto` = percentual de desconto (0 a 100)
- O fator `0.12` controla a inclinação da curva (quão abrupta é a transição)
- O centro `35` significa que 35% de desconto rende ~15 pontos (metade do máximo)

**Comportamento esperado (valores aproximados):**
| Desconto | Pontos (~) | Interpretação                    |
|----------|-----------|----------------------------------|
| 0%       | 0.4       | Sem desconto, quase zero         |
| 10%      | 1.5       | Desconto cosmético               |
| 20%      | 4.5       | Desconto modesto                 |
| 30%      | 10.5      | Desconto razoável                |
| 35%      | 15.0      | Ponto médio da curva             |
| 40%      | 19.5      | Bom desconto                     |
| 50%      | 25.5      | Ótimo desconto                   |
| 60%      | 28.5      | Desconto excepcional             |
| 70%+     | ~29.5+    | Saturação (rendimento decrescente)|

**Cálculo do desconto quando `discount_percentage` não está disponível:**
```
Se original_price > 0 e current_price >= 0:
    desconto = ((original_price - current_price) / original_price) * 100
    desconto = max(0, min(desconto, 100))  # clamp entre 0 e 100
Senão:
    desconto = 0 (dado ausente)
```

**Cap máximo:** Descontos acima de 80% devem ser tratados como 80% para efeitos de cálculo (descontos absurdos são frequentemente erro de dados ou produto suspeito).

---

### 2. Badge (0–15 pontos)

**Objetivo:** Aproveitar a curadoria algorítmica do Mercado Livre.

**Curva:** Escala discreta fixa com mapeamento de strings.

**Tabela de mapeamento:**

| Badge (string do ML)        | Aliases possíveis no scraping                           | Fator | Pontos |
|-----------------------------|--------------------------------------------------------|-------|--------|
| Oferta Relâmpago            | `LIGHTNING_DEAL`, `oferta-relampago`, `Oferta relâmpago` | 1.00  | 15     |
| Imperdível                  | `MUST_HAVE`, `imperdivel`, `Imperdível`                 | 0.50  | 7.5    |
| Oferta do Dia               | `DEAL_OF_THE_DAY`, `oferta-do-dia`, `Oferta do dia`    | 0.30  | 4.5    |
| Mais Vendido                | `BEST_SELLER`, `mais-vendido`, `Mais vendido`           | 0.10  | 1.5    |
| Nenhum / Desconhecido       | `null`, `undefined`, `""`, qualquer outro valor          | 0.00  | 0      |

**Implementação:**
- Normalizar o badge para lowercase e remover acentos antes de comparar
- Usar um dicionário/map com todos os aliases
- Qualquer valor não reconhecido = 0 pontos
- Se o campo badge for um array (múltiplos badges), usar o de **maior pontuação**

---

### 3. Nota de Avaliação (0–15 pontos)

**Objetivo:** Valorizar produtos bem avaliados, com uma faixa útil entre 3.5 e 5.0.

**Curva:** Linear normalizada com piso em 3.5.

**Fórmula:**
```
Se rating >= 3.5:
    score = ((rating - 3.5) / 1.5) * 15
Senão:
    score = 0
```

**Comportamento:**
| Rating | Pontos |
|--------|--------|
| 0–3.49 | 0      |
| 3.5    | 0      |
| 4.0    | 5.0    |
| 4.25   | 7.5    |
| 4.5    | 10.0   |
| 4.75   | 12.5   |
| 5.0    | 15.0   |

**Justificativa:** Abaixo de 3.5 estrelas no ML já é considerado produto problemático. A maioria dos produtos populares fica entre 4.0 e 4.8, então a normalização 3.5–5.0 cria diferenciação real nessa faixa.

---

### 4. Número de Reviews (0–10 pontos)

**Objetivo:** Funcionar como "índice de confiança" da nota de avaliação.

**Curva:** Logarítmica (base 10) com saturação.

**Fórmula:**
```
Se review_count > 0:
    score = min(10, (log10(review_count) / log10(5000)) * 10)
Senão:
    score = 0
```

**Onde:**
- `5000` é o ponto de saturação (a partir do qual o ganho marginal é irrelevante)
- `log10` garante que os primeiros reviews valem muito mais que os últimos

**Comportamento:**
| Reviews | Pontos (~) |
|---------|-----------|
| 0       | 0         |
| 1       | 0         |
| 5       | 1.9       |
| 10      | 2.7       |
| 50      | 4.6       |
| 100     | 5.4       |
| 500     | 7.3       |
| 1000    | 8.1       |
| 5000    | 10.0      |
| 10000+  | 10.0 (cap)|

---

### 5. Frete Grátis (0–10 pontos)

**Objetivo:** Binário simples — frete grátis é um diferencial significativo.

**Fórmula:**
```
Se free_shipping == true:
    score = 10
Senão:
    score = 0
```

**Valores truthy para `free_shipping`:** `true`, `"true"`, `"sim"`, `"grátis"`, `"free"`, `1`
**Qualquer outro valor (incluindo ausente):** 0 pontos

---

### 6. Parcelamento sem Juros (0–10 pontos)

**Objetivo:** Valorizar progressivamente ofertas com mais parcelas sem juros.

**Curva:** Discreta progressiva.

**Tabela:**
| Condição                          | Pontos |
|-----------------------------------|--------|
| 12x sem juros                     | 10     |
| 10x sem juros                     | 8      |
| 6x sem juros                      | 6      |
| 3x sem juros                      | 4      |
| Parcelamento COM juros (qualquer) | 2      |
| Sem parcelamento / dados ausentes | 0      |

**Lógica:**
```
Se installments existe E interest_free == true:
    Se quantity >= 12: score = 10
    Senão se quantity >= 10: score = 8
    Senão se quantity >= 6: score = 6
    Senão se quantity >= 3: score = 4
    Senão: score = 2
Se installments existe E interest_free == false:
    score = 2
Senão:
    score = 0
```

---

### 7. Qualidade do Título (0–10 pontos)

**Objetivo:** Identificar títulos de vendedores profissionais e penalizar lixo/spam.

**Método:** Heurísticas baseadas em regras (sem uso de LLM).

**Subcritérios (somados, máximo 10):**

| Subcritério                        | Pontos | Regra                                                                |
|-------------------------------------|--------|----------------------------------------------------------------------|
| Comprimento adequado                | +3     | Entre 30 e 150 caracteres                                           |
| Comprimento muito curto (<30)       | +0     | Substitui o +3 acima                                                |
| Comprimento excessivo (>150)        | +1     | Substitui o +3 acima (keyword stuffing)                             |
| Presença de marca conhecida         | +2     | Match contra lista de marcas populares (case-insensitive)           |
| Ausência de CAPS LOCK excessivo     | +2     | Menos de 40% dos caracteres alfabéticos são maiúsculos              |
| Ausência de caracteres spam         | +2     | Sem emojis, sem `!!!`, sem `@@@`, sem `***`, sem `$$$`              |
| Presença de especificações técnicas | +1     | Contém padrões como `128GB`, `i7`, `4K`, `LED`, medidas (cm/mm/m)  |

**Lista de marcas populares (inicial — deve ser extensível):**
```
Samsung, Apple, Xiaomi, Motorola, LG, Sony, Philips, Nike, Adidas, 
Lenovo, Dell, HP, Asus, Acer, Logitech, JBL, Brastemp, Electrolux, 
Consul, Tramontina, Mondial, Intelbras, TP-Link, Multilaser, Positivo,
Havaianas, Nestlé, 3M, Bosch, Makita, DeWalt, Black+Decker, Oster,
Panasonic, Epson, Canon, Nikon, GoPro, Garmin, Whirlpool, Fischer
```

**Regex para detecção de especificações técnicas:**
```regex
\b\d+\s*(GB|TB|MB|GHz|MHz|MP|W|V|mAh|pol|"|mm|cm|m|kg|g|L|ml)\b|\b(i[3579]|Ryzen\s*[3579]|Core|DDR[45]|SSD|HDD|NVMe|LED|LCD|OLED|AMOLED|4K|8K|FHD|HD|UHD|WiFi|Bluetooth|USB-C|HDMI|RGB)\b
```
(case-insensitive)

---

## Redistribuição de Pontos para Dados Ausentes

**Princípio:** Quando um critério não pode ser calculado por falta de dados, seus pontos são **redistribuídos proporcionalmente** entre os critérios que possuem dados válidos. Isso garante que a escala de 0–100 continue significativa mesmo com dados incompletos.

### Algoritmo de Redistribuição

```
1. Para cada critério, verificar se os dados estão disponíveis:
   - Desconto: disponível se discount_percentage > 0 OU (original_price > 0 E current_price >= 0)
   - Badge: disponível se badge != null/undefined/"" (mesmo que o badge seja desconhecido e valha 0 pontos, ele é "disponível" — o critério foi avaliado)
   - Rating: disponível se rating != null/undefined E rating > 0
   - Reviews: disponível se review_count != null/undefined E review_count >= 0
   - Frete: disponível se free_shipping != null/undefined
   - Parcelamento: disponível se installments != null/undefined
   - Título: disponível se title != null/undefined/""

2. Calcular o score bruto de cada critério disponível.

3. Calcular o peso total dos critérios disponíveis:
   peso_disponivel = soma dos pesos dos critérios com dados

4. Calcular o fator de redistribuição:
   fator = 100 / peso_disponivel

5. Score final de cada critério:
   score_final_i = score_bruto_i * fator

6. Score total = soma de todos os score_final_i
```

### Exemplo de Redistribuição

**Cenário:** Uma oferta onde `rating` e `review_count` não estão disponíveis.

- Pesos disponíveis: 30 + 15 + 10 + 10 + 10 = 75 (desconto + badge + frete + parcelamento + título)
- Pesos indisponíveis: 15 + 10 = 25 (rating + reviews)
- Fator de redistribuição: 100 / 75 = 1.333...

Se os scores brutos fossem:
- Desconto: 20/30
- Badge: 7.5/15
- Frete: 10/10
- Parcelamento: 6/10
- Título: 7/10

Scores redistribuídos:
- Desconto: 20 × 1.333 = 26.67
- Badge: 7.5 × 1.333 = 10.00
- Frete: 10 × 1.333 = 13.33
- Parcelamento: 6 × 1.333 = 8.00
- Título: 7 × 1.333 = 9.33
- **Total: 67.33/100**

### Regra de Mínimo de Dados

Se **menos de 3 critérios** tiverem dados disponíveis, o score final deve ser marcado com um flag `low_confidence: true` para indicar que a pontuação não é confiável. O bot pode decidir não publicar ofertas com `low_confidence`.

---

## Output Esperado

Cada oferta processada deve retornar um objeto com o seguinte formato:

```json
{
  "total_score": 72.5,
  "low_confidence": false,
  "available_criteria": 7,
  "redistribution_factor": 1.0,
  "breakdown": {
    "discount": {
      "raw_score": 19.5,
      "final_score": 19.5,
      "max_points": 30,
      "input_value": 40,
      "available": true
    },
    "badge": {
      "raw_score": 7.5,
      "final_score": 7.5,
      "max_points": 15,
      "input_value": "DEAL_OF_THE_DAY",
      "available": true
    },
    "rating": {
      "raw_score": 11.0,
      "final_score": 11.0,
      "max_points": 15,
      "input_value": 4.6,
      "available": true
    },
    "reviews": {
      "raw_score": 8.8,
      "final_score": 8.8,
      "max_points": 10,
      "input_value": 1842,
      "available": true
    },
    "free_shipping": {
      "raw_score": 10.0,
      "final_score": 10.0,
      "max_points": 10,
      "input_value": true,
      "available": true
    },
    "installments": {
      "raw_score": 10.0,
      "final_score": 10.0,
      "max_points": 10,
      "input_value": { "quantity": 12, "interest_free": true },
      "available": true
    },
    "title": {
      "raw_score": 8.0,
      "final_score": 8.0,
      "max_points": 10,
      "input_value": "Notebook Lenovo IdeaPad 3 15.6\" i5 8GB 256GB SSD",
      "available": true
    }
  }
}
```

---

## Requisitos de Implementação

### Versão Python (`deal_scorer.py`)

- Classe `DealScorer` com método `score(offer: dict) -> dict`
- Método auxiliar para cada critério: `_score_discount()`, `_score_badge()`, etc.
- A lista de marcas deve ser carregada de uma constante (facilmente editável)
- Type hints completos
- Docstrings em português
- Testes unitários com `pytest` cobrindo:
  - Oferta completa (todos os campos)
  - Oferta mínima (só título)
  - Oferta sem nenhum dado (edge case)
  - Validação da redistribuição de pontos
  - Cada critério individualmente
  - Edge cases: desconto negativo, rating > 5, review_count negativo, badge como array

### Versão JavaScript (`dealScorer.js`)

- Função exportável `scoreDeal(offer)` que retorna o mesmo formato de output
- Compatível com ambiente Node.js do n8n (sem dependências externas, sem import/export ESM — usar `module.exports` ou ser self-contained)
- Mesma lógica e mesmos resultados que a versão Python
- Incluir um bloco de exemplo de uso no contexto de um nó Code do n8n:
  ```javascript
  // Exemplo de uso no n8n Code node
  const offer = $input.first().json;
  const result = scoreDeal(offer);
  return [{ json: result }];
  ```

### Ambas as versões

- Todas as constantes (pesos, limiares, lista de marcas) devem ser configuráveis no topo do arquivo
- Logging opcional dos cálculos intermediários (útil para debug)
- Arredondar score final para 1 casa decimal
- Garantir que o score nunca ultrapasse 100.0 nem fique abaixo de 0.0

---

## Arquivos a serem criados

```
dealflow-scoring/
├── python/
│   ├── deal_scorer.py        # Módulo principal
│   ├── test_deal_scorer.py   # Testes com pytest
│   └── example_usage.py      # Script de exemplo
├── javascript/
│   ├── dealScorer.js          # Módulo principal (compatível n8n)
│   └── test_dealScorer.js     # Testes básicos
└── README.md                  # Documentação do sistema de scoring
```
