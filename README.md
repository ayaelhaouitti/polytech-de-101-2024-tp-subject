# **Projet ETL - Gestion des Données de Vélos en Libre-Service** 🚲

Ce projet propose un pipeline ETL complet pour l'ingestion, la consolidation et l'agrégation des données de vélos en libre-service dans plusieurs villes françaises : **Paris**, **Nantes**, et **Toulouse**. Il vise à appliquer les concepts clés de la data ingénierie enseignés au cours, en utilisant des technologies modernes comme **DuckDB**, **Pandas** et des **APIs Open Data**.

---

## **🎯 Objectifs du Projet**

- **Ingestion des Données :** Collecter les données open-source de Paris, Nantes et Toulouse.
- **Consolidation des Données :** Organiser et structurer les données dans une base DuckDB.
- **Enrichissement des Données :** Ajouter les informations officielles depuis l’API gouvernementale "Open Data Communes".
- **Modélisation Dimensionnelle :** Créer un modèle pour faciliter l'analyse.
- **Agrégation des Données :** Produire des statistiques utiles sur la disponibilité des vélos.

---

## **📁 Structure du Projet**

```
/src
    ├── data_agregation.py          # Agrégation des données
    ├── data_consolidation.py       # Consolidation et stockage des données
    ├── data_ingestion.py           # Récupération des données API
    └── main.py                     # Script principal ETL
/data
    ├── raw_data/                   # Fichiers JSON récupérés
    ├── duckdb/                     # Base de données DuckDB
    │   └── mobility_analysis.duckdb
/sql_statements
    ├── create_agregate_tables.sql  # Script SQL pour créer les tables d'agrégation
    └── create_consolidate_tables.sql # Script SQL pour créer les tables de consolidation
.venv/                              # Environnement virtuel Python
.gitignore                          # Fichiers ignorés par Git
requirements.txt                    # Dépendances Python
README.md                           # Documentation
```

---

## **🔄 Pipeline ETL : Étapes du Processus**

### **1. Ingestion des Données** 🗂️

Les fichiers JSON sont collectés via des **APIs Open Data**. Chaque fonction d’ingestion télécharge les données et les stocke localement avec une structure organisée par date. 

**Fonctions principales :**
- `get_paris_realtime_bicycle_data()`
- `get_nantes_realtime_bicycle_data()`
- `get_toulouse_realtime_bicycle_data()`
- `get_commune_data()`

---

### **2. Consolidation des Données** 📊

Les données JSON sont transformées et insérées dans **DuckDB** à l'aide de Pandas. Chaque ville est traitée indépendamment pour tenir compte des spécificités de leurs APIs respectives.

**Difficultés rencontrées :**
- **Formats Incohérents :** Les colonnes entre les différentes APIs (Paris, Nantes, Toulouse) ont dû être harmonisées.
- **Absence de Codes INSEE :** Les données brutes des villes de Nantes et Toulouse ne contenaient pas de code INSEE, ce qui a nécessité l’enrichissement via l’API des communes françaises.

**Exemple : Consolidation des Données des Villes**
```python
def consolidate_city_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open(f"data/raw_data/{date.today().strftime('%Y-%m-%d')}/commune_data.json", "r") as file:
        commune_data = json.load(file)
    commune_df = pd.DataFrame(commune_data)
    con.register('commune_df', commune_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM commune_df;")
```

---

### **3. Agrégation des Données** 📈

Une **modélisation dimensionnelle** est mise en place avec trois tables principales :
- **DIM_CITY** : Informations sur les villes.
- **DIM_STATION** : Informations sur les stations.
- **FACT_STATION_STATEMENT** : Données sur les disponibilités de vélos en temps réel.

**Difficultés rencontrées :**
- **Jointures Incorrectes :** Les jointures SQL initiales sur les champs `CITY_CODE` et `STATION_ID` ont dû être corrigées pour garantir l’intégrité des données.

**Exemple d’Agrégation :**
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

## **🚀 Installation et Exécution**

### **🛠️ Pré-requis**
- **Python 3.9+**
- **Virtualenv** (optionnel)

### **📦 Installation**
```bash
git clone https://github.com/ayaelhaouitti/polytech-de-101-2024-tp-subject.git
cd polytech-de-101-2024-tp-subject
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### **▶️ Exécution du Pipeline ETL**
```bash
python src/main.py
```

---

## **📊 Requêtes SQL Utiles**

### **1. Nombre d'emplacements disponibles de vélos dans une ville**
```sql
-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');
```

### **2. Nombre de vélos disponibles en moyenne dans chaque station**
```sql
-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```

---

## **📌 Conclusion**

Le projet ETL permet de **centraliser, consolider et analyser** les données de vélos en libre-service pour les villes de Paris, Nantes et Toulouse. Grâce à son architecture modulaire, ce pipeline peut facilement intégrer de nouvelles villes et s’adapter à d'autres sources de données.

---

💻 **Auteurs :**  
- Aya Elhaouitti  
- Clément Fortin