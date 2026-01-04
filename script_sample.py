import json

with open("MiniProjet/prix-carburants-quotidien.json", encoding="utf-8") as f:
    data = json.load(f)

sample = data[:00]  # on garde les 200 premières stations

with open("MiniProjet/prix-carburants-200.json", "w", encoding="utf-8") as f:
    json.dump(sample, f, ensure_ascii=False, indent=2)
