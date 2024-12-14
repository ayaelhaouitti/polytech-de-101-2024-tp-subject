# Introduction à la Data Ingénierie - Projet ETL de Gestion des Données de Vélos en Libre-Service

Ce projet vise à construire un pipeline ETL complet pour l'ingestion, la consolidation et l'agrégation des données de disponibilité des stations de vélos en libre-service dans plusieurs grandes villes françaises. Le projet met en pratique les concepts clés de la data ingénierie enseignés pendant le cours.

## Objectifs du Projet

- Créer un pipeline ETL fonctionnel basé sur les données open-source de Paris.
- Ajouter et intégrer les données des villes de Nantes et Toulouse.
- Résoudre les problèmes rencontrés lors de l'intégration.
- Remplacer et enrichir les données des villes avec les données officielles de l’API gouvernementale française "Open Data Communes".
- Construire une base de données locale en utilisant DuckDB.
- Mettre en place une modélisation dimensionnelle pour faciliter l’analyse des données.

---

## Structure du Projet

### **1. Ingestion des Données**

Le fichier `data_ingestion.py` contient les fonctions qui récupèrent les données à partir des APIs open-source et les stockent localement dans des fichiers JSON.

#### **Ajout des villes de Nantes et Toulouse :**

- Création de fonctions similaires à celles de Paris.
- Résolution des problèmes de format de données spécifiques aux APIs de Nantes et Toulouse.

```python
from datetime import datetime
import requests
import os

def get_nantes_realtime_bicycle_data():
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/records"
    response = requests.request("GET", url)
    serialize_data(response.text, "nantes_realtime_bicycle_data.json")

def get_toulouse_realtime_bicycle_data():
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/records"
    response = requests.request("GET", url)
    serialize_data(response.text, "toulouse_realtime_bicycle_data.json")
```

### **2. Consolidation des Données**

Les fichiers JSON sont ensuite consolidés dans une base de données locale DuckDB. Le fichier `data_consolidation.py` contient des fonctions pour créer et remplir les tables de consolidation.

#### **Problèmes Rencontrés :**

- Incohérence dans les formats de données des différentes villes.
- Absence initiale des codes INSEE pour Nantes et Toulouse.
- Correction via l'utilisation de l'API "Open Data Communes".

Exemple de consolidation :

```python
from datetime import date
import duckdb
import pandas as pd

def consolidate_city_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open(f"data/raw_data/{date.today().strftime('%Y-%m-%d')}/commune_data.json", "r") as file:
        commune_data = json.load(file)
    commune_df = pd.DataFrame(commune_data)
    con.register('commune_df', commune_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM commune_df;")
```

### **3. Agrégation des Données**

Les données consolidées sont ensuite agrégées dans une modélisation dimensionnelle utilisant trois tables principales : `DIM_CITY`, `DIM_STATION`, et `FACT_STATION_STATEMENT`. Le fichier `data_agregation.py` contient des fonctions SQL correspondantes.

#### **Problèmes Résolus :**

- Problème de jointure incorrecte sur le champ `CITY_CODE`.
- Ajout de la ville dans les tables de dimensions.

Exemple d’agrégation corrigée :

```python
def agregate_fact_station_statements():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    sql_statement = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
    SELECT
        CONSOLIDATE_STATION_STATEMENT.STATION_ID,
        CONSOLIDATE_STATION.CITY_CODE AS CITY_ID,
        CONSOLIDATE_STATION_STATEMENT.BICYCLE_DOCKS_AVAILABLE,
        CONSOLIDATE_STATION_STATEMENT.BICYCLE_AVAILABLE,
        CONSOLIDATE_STATION_STATEMENT.LAST_STATEMENT_DATE,
        CURRENT_DATE AS CREATED_DATE
    FROM CONSOLIDATE_STATION_STATEMENT
    JOIN CONSOLIDATE_STATION ON CONSOLIDATE_STATION.ID = CONSOLIDATE_STATION_STATEMENT.STATION_ID;
    """
    con.execute(sql_statement)
```

---

## Installation et Exécution

### **Pré-requis**
- Python 3.9+
- Virtualenv

### **Installation**
```bash
git clone https://github.com/monrepo/projet-etl-velos.git
cd projet-etl-velos
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### **Exécution**
```bash
python src/main.py
```

---

## Requêtes SQL Utilisables

### **1. Nombre d’emplacements disponibles de vélos dans une ville**
```sql
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm
INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM FACT_STATION_STATEMENT)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) IN ('paris', 'nantes', 'toulouse');
```

### **2. Moyenne des vélos disponibles par station**
```sql
SELECT ds.name, ds.code, ds.address, tmp.avg_bike_available
FROM DIM_STATION ds
JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_bike_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```

---

## Conclusion

Le projet ETL final fonctionne correctement, consolidant les données de Paris, Nantes et Toulouse, tout en permettant des analyses détaillées sur les disponibilités de vélos dans ces villes. La structure modulaire du pipeline permet d'ajouter facilement de nouvelles villes et de nouvelles sources de données.

