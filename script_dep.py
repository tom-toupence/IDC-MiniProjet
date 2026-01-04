import json

# 1. Charger le gros JSON
with open("MiniProjet/prix-carburants-quotidien.json", encoding="utf-8") as f:
    data = json.load(f)

# 2. Filtrer : ici on garde seulement le département 06
subset = [s for s in data if s.get("dep_code") == "06"]

print("Stations retenues :", len(subset))

# 3. Sauver un JSON réduit
with open("MiniProjet/prix-carburants-Departement06.json", "w", encoding="utf-8") as f:
    json.dump(subset, f, ensure_ascii=False, indent=2)
