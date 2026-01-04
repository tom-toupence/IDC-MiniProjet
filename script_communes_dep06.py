import csv

# Filtrer les communes du département 06
communes_06 = []

with open("communes-france-2025.csv", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get("dep_code") == "06":
            communes_06.append(row)

print(f"Communes du département 06 trouvées : {len(communes_06)}")

# Écrire le CSV filtré
with open("communes-departement06.csv", "w", encoding="utf-8", newline="") as f:
    if communes_06:
        writer = csv.DictWriter(f, fieldnames=communes_06[0].keys())
        writer.writeheader()
        writer.writerows(communes_06)

print("✅ Fichier communes-departement06.csv créé")
