import os
import re
import requests
from rdflib import Graph

g = Graph()
g.parse("output.ttl", format="turtle")


def build_prompt(question: str) -> str:
    return f"""
You are a SPARQL expert. Convert the user question into a SPARQL SELECT query.

Constraints:
- Use only these prefixes:
  PREFIX schema: <http://schema.org/>
  PREFIX ex:     <http://example.org/vocab#>
- Use only these classes/properties:
  schema:City, schema:GasStation
  schema:name, schema:streetAddress, schema:addressLocality, schema:postalCode
  schema:price, schema:priceValidUntil
  ex:population, ex:fuelType, ex:locatedInCommune, ex:depName, ex:regName
- Output ONLY the SPARQL query.

User question:
{question}
""".strip()


OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    raise RuntimeError(
        "OPENROUTER_API_KEY manquante. Définis-la dans tes variables d'environnement."
    )

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "openai/gpt-4o-mini"

def _strip_code_fences(text: str) -> str:
    # enlève ```sparql ... ``` ou ``` ... ```
    text = text.strip()
    text = re.sub(r"^```(?:sparql)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()

def llm_generate_sparql(prompt: str) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": "You generate ONLY SPARQL SELECT queries."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
    }

    r = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=60)
    r.raise_for_status()

    data = r.json()
    raw = data["choices"][0]["message"]["content"]
    sparql = _strip_code_fences(raw)

    # mini garde-fous (évite que le LLM sorte autre chose)
    if "SELECT" not in sparql.upper():
        raise ValueError(f"Le modèle n'a pas renvoyé une requête SELECT:\n{sparql}")
    if any(k in sparql.upper() for k in ["INSERT", "DELETE", "CONSTRUCT", "ASK", "DESCRIBE"]):
        raise ValueError(f"Requête non autorisée (seulement SELECT attendu):\n{sparql}")

    return sparql

#question = "Liste les stations situées dans la commune de Nice avec leur adresse."
question = "Donne la station la moins chère d'une ville."
#question = "Donne les stations qui proposent du E85 et dans quelle commune sont-elles ?."

prompt = build_prompt(question)

sparql = llm_generate_sparql(prompt)
print("SPARQL générée:\n", sparql)