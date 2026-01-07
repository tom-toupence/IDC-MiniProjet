import os
import re
import requests

MAPPING_TTL = r"""
@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .
@prefix rr:     <http://www.w3.org/ns/r2rml#> .
@prefix rml:    <http://semweb.mmlab.be/ns/rml#> .
@prefix ql:     <http://semweb.mmlab.be/ns/ql#> .
@prefix schema: <http://schema.org/> .
@prefix dbo:    <http://dbpedia.org/ontology/> .
@prefix ex:     <http://example.org/vocab#> .

@base <http://example.org/> .

# TriplesMap pour les communes

<#CommunesMap> a rr:TriplesMap ;
    rml:logicalSource [
        a rml:LogicalSource ;
        rml:source "communes-france-2025.csv" ;
        rml:referenceFormulation ql:CSV ;
    ] ;

    rr:subjectMap [
        rr:template "http://example.org/commune/{code_insee}" ;
        rr:class schema:City ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:name ;
        rr:objectMap [ rml:reference "nom_standard" ] ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate dbo:inseeCode ;
        rr:objectMap [ rml:reference "code_insee" ] ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:postalCode ;
        rr:objectMap [ rml:reference "code_postal" ] ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate dbo:department ;
        rr:objectMap [ rml:reference "dep_nom" ] ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate dbo:region ;
        rr:objectMap [ rml:reference "reg_nom" ] ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:population ;
        rr:objectMap [
            rml:reference "population" ;
            rr:datatype xsd:integer
        ]
    ] .

# TriplesMap pour les stations (physiques, sans prix)
# On groupe par ID de station pour éviter les doublons

<#StationsMap> a rr:TriplesMap ;
    rml:logicalSource [
        a rml:LogicalSource ;
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*]" ;
    ] ;

    rr:subjectMap [
        rr:template "http://example.org/station/{id}" ;
        rr:class schema:GasStation ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:identifier ;
        rr:objectMap [ rml:reference "id" ] ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:streetAddress ;
        rr:objectMap [ rml:reference "adresse" ] ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:latitude ;
        rr:objectMap [
            rml:reference "geom.lat" ;
            rr:datatype xsd:decimal
        ]
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:longitude ;
        rr:objectMap [
            rml:reference "geom.lon" ;
            rr:datatype xsd:decimal
        ]
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:containedInPlace ;
        rr:objectMap [
            rr:parentTriplesMap <#CommunesMap> ;
            rr:joinCondition [
                rr:child  "com_arm_code" ;
                rr:parent "code_insee" ; 
            ]
        ]
    ] .

# TriplesMap pour les prix des carburants (offres de vente)

<#PricesMap> a rr:TriplesMap ;
    rml:logicalSource [
        a rml:LogicalSource ;
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*]" ;
    ] ;

    rr:subjectMap [
        rr:template "http://example.org/station/{id}/fuel/{prix_nom}" ;
        rr:class schema:Offer ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:name ;
        rr:objectMap [ rml:reference "prix_nom" ] ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:price ;
        rr:objectMap [
            rml:reference "prix_valeur" ;
            rr:datatype xsd:decimal
        ]
    ] ;

    rr:predicateObjectMap [
        rr:predicate schema:offeredBy ;
        rr:objectMap [
            rr:template "http://example.org/station/{id}" ;
            rr:termType rr:IRI
        ]
    ] .

# TriplesMap pour les instances d'horaires uniques
# Crée une instance par combinaison unique d'heures d'ouverture/fermeture

<#HorairesPursMap> a rr:TriplesMap ;
    rml:logicalSource [
        a rml:LogicalSource ;
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        # On itère sur tous les jours pour créer les blocs horaires
        rml:iterator "$[*].horaires.jour[*]" ;
    ] ;
    
    rr:subjectMap [
        # L'ID ne dépend QUE des heures (ex: Horaire_0800_2000)
        # Pas de station_id ici, pour permettre le partage !
        rr:template "http://example.org/horaire/{horaire_id}" ;
        rr:class schema:OpeningHoursSpecification ;
    ] ;
    
    rr:predicateObjectMap [
        rr:predicate schema:opens ;
        rr:objectMap [
            rml:reference "horaire.@ouverture" ;
            rr:datatype xsd:time
        ]
    ] ;
    
    rr:predicateObjectMap [
        rr:predicate schema:closes ;
        rr:objectMap [
            rml:reference "horaire.@fermeture" ;
            rr:datatype xsd:time
        ]
    ] .
    # Note : Pas de schema:dayOfWeek ici !

# TriplesMap pour les jours fermés et non définis
# Traite spécifiquement les horaires sans heures (Ferme et NonDefini)

# --- LUNDI ---
<#LundiMap> a rr:TriplesMap ;
    rml:logicalSource [
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        # On filtre uniquement les Lundis (ID=1)
        rml:iterator "$[*].horaires.jour[?(@['@id'] == '1')]" ;
    ] ;
    rr:subjectMap [ rr:template "http://example.org/station/{station_id}" ] ;
    rr:predicateObjectMap [
        rr:predicate ex:openingHoursMonday ;
        rr:objectMap [ 
            rr:template "http://example.org/horaire/{horaire_id}" ;
            rr:termType rr:IRI 
        ]
    ] .

# --- MARDI ---
<#MardiMap> a rr:TriplesMap ;
    rml:logicalSource [
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*].horaires.jour[?(@['@id'] == '2')]" ;
    ] ;
    rr:subjectMap [ rr:template "http://example.org/station/{station_id}" ] ;
    rr:predicateObjectMap [
        rr:predicate ex:openingHoursTuesday ;
        rr:objectMap [ 
            rr:template "http://example.org/horaire/{horaire_id}" ;
            rr:termType rr:IRI 
        ]
    ] .

# --- MERCREDI ---
<#MercrediMap> a rr:TriplesMap ;
    rml:logicalSource [
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*].horaires.jour[?(@['@id'] == '3')]" ;
    ] ;
    rr:subjectMap [ rr:template "http://example.org/station/{station_id}" ] ;
    rr:predicateObjectMap [
        rr:predicate ex:openingHoursWednesday ;
        rr:objectMap [ 
            rr:template "http://example.org/horaire/{horaire_id}" ;
            rr:termType rr:IRI 
        ]
    ] .

# --- JEUDI ---
<#JeudiMap> a rr:TriplesMap ;
    rml:logicalSource [
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*].horaires.jour[?(@['@id'] == '4')]" ;
    ] ;
    rr:subjectMap [ rr:template "http://example.org/station/{station_id}" ] ;
    rr:predicateObjectMap [
        rr:predicate ex:openingHoursThursday ;
        rr:objectMap [ 
            rr:template "http://example.org/horaire/{horaire_id}" ;
            rr:termType rr:IRI 
        ]
    ] .

# --- VENDREDI ---
<#VendrediMap> a rr:TriplesMap ;
    rml:logicalSource [
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*].horaires.jour[?(@['@id'] == '5')]" ;
    ] ;
    rr:subjectMap [ rr:template "http://example.org/station/{station_id}" ] ;
    rr:predicateObjectMap [
        rr:predicate ex:openingHoursFriday ;
        rr:objectMap [ 
            rr:template "http://example.org/horaire/{horaire_id}" ;
            rr:termType rr:IRI 
        ]
    ] .

# --- SAMEDI ---
<#SamediMap> a rr:TriplesMap ;
    rml:logicalSource [
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*].horaires.jour[?(@['@id'] == '6')]" ;
    ] ;
    rr:subjectMap [ rr:template "http://example.org/station/{station_id}" ] ;
    rr:predicateObjectMap [
        rr:predicate ex:openingHoursSaturday ;
        rr:objectMap [ 
            rr:template "http://example.org/horaire/{horaire_id}" ;
            rr:termType rr:IRI 
        ]
    ] .

# --- DIMANCHE ---
<#DimancheMap> a rr:TriplesMap ;
    rml:logicalSource [
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*].horaires.jour[?(@['@id'] == '7')]" ;
    ] ;
    rr:subjectMap [ rr:template "http://example.org/station/{station_id}" ] ;
    rr:predicateObjectMap [
        rr:predicate ex:openingHoursSunday ;
        rr:objectMap [ 
            rr:template "http://example.org/horaire/{horaire_id}" ;
            rr:termType rr:IRI 
        ]
    ] .

# TriplesMap pour le flag 24/7 automatisé

<#Stations24hMap> a rr:TriplesMap ;
    rml:logicalSource [
        a rml:LogicalSource ;
        rml:source "prix-carburants-quotidien-preprocessed.json" ;
        rml:referenceFormulation ql:JSONPath ;
        rml:iterator "$[*]" ;
    ] ;

    rr:subjectMap [
        rr:template "http://example.org/station/{id}" ;
    ] ;

    rr:predicateObjectMap [
        rr:predicate ex:automatise24h ;
        rr:objectMap [
            rml:reference "horaires.@automate-24-24" ;
            rr:datatype xsd:string
        ]
    ] .
"""

def build_prompt(question: str) -> str:
    return f"""
{MAPPING_TTL}

This is mapping.ttl. When executed it generates an RDF graph (output.ttl).
You must query the generated RDF output by generating a SPARQL SELECT query.
Return only the SPARQL query with the needed prefixes.
Question:
\"\"\"{question}\"\"\"
""".strip()


OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    raise RuntimeError("OPENROUTER_API_KEY manquante. Définis-la dans tes variables d'environnement.")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "openai/gpt-4o-mini"

def llm_generate_sparql(prompt: str) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": "You output ONLY a SPARQL SELECT query. No markdown. No backticks. No explanations."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
    }

    r = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=60)
    r.raise_for_status()

    sparql = r.json()["choices"][0]["message"]["content"].strip()

    # garde-fous 
    if "```" in sparql:
        raise ValueError(f"Le modèle a renvoyé du markdown (```), refusé:\n{sparql}")
    if "SELECT" not in sparql.upper():
        raise ValueError(f"Le modèle n'a pas renvoyé une requête SELECT:\n{sparql}")
    if any(k in sparql.upper() for k in ["INSERT", "DELETE", "CONSTRUCT", "ASK", "DESCRIBE"]):
        raise ValueError(f"Requête non autorisée (seulement SELECT attendu):\n{sparql}")

    return sparql


question = "Donne les stations qui proposent du carburant E85 et le nom de leur commune"
#question = "Donne les 10 communes avec le plus de stations : nom de commune et nombre de stations."
#question = "Donne les 10 offres les moins chères en France : carburant, prix, station et nom de la commune."

prompt = build_prompt(question)
sparql = llm_generate_sparql(prompt)

print(sparql)