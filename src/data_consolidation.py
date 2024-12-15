import json
from datetime import datetime, date
import duckdb
import pandas as pd

# Date actuelle
today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1

def create_consolidate_tables():
    """
    But : Créer les tables de consolidation dans la base de données DuckDB à partir d'un fichier SQL.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_data():
    """
    But : Consolider les données des stations de vélos pour Paris, Nantes et Toulouse, 
    en les insérant ou remplaçant dans la table CONSOLIDATE_STATION.
    """
    today_date = date.today().strftime("%Y-%m-%d")
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Charger les codes INSEE des communes depuis le fichier JSON
    with open(f"data/raw_data/{today_date}/commune_data.json", "r") as file:
        commune_data = json.load(file)
    commune_codes = {commune['nom'].lower(): commune['code'] for commune in commune_data}

    # Traitement pour Paris
    data = {}
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"PARIS-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]]

    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")

    # Traitement pour Nantes
    try:
        with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
            data = json.load(fd)['results']

        nantes_raw_data_df = pd.json_normalize(data)
        nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"NANTES-{x}")
        nantes_raw_data_df["created_date"] = today_date
        nantes_raw_data_df["city_name"] = "Nantes"
        nantes_raw_data_df["city_code"] = commune_codes.get("nantes", None)

        nantes_station_data_df = nantes_raw_data_df[[
            "id",
            "number",
            "name",
            "city_name",
            "city_code",
            "address",
            "position.lon",
            "position.lat",
            "status",
            "created_date",
            "bike_stands"
        ]]

        nantes_station_data_df.rename(columns={
            "number": "code",
            "position.lon": "longitude",
            "position.lat": "latitude",
            "status": "status",
            "bike_stands": "capacity"
        }, inplace=True)

        con.register('nantes_station_data_df', nantes_station_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_data_df;")
    except Exception as e:
        print(f"Erreur Nantes: {e}")

    # Traitement pour Toulouse
    try:
        with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
            data = json.load(fd)['results']

        toulouse_raw_data_df = pd.json_normalize(data)
        toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"TOULOUSE-{x}")
        toulouse_raw_data_df["created_date"] = today_date
        toulouse_raw_data_df["city_name"] = "Toulouse"
        toulouse_raw_data_df["city_code"] = commune_codes.get("toulouse", None)

        toulouse_station_data_df = toulouse_raw_data_df[[
            "id",
            "number",
            "name",
            "city_name",
            "city_code",
            "address",
            "position.lon",
            "position.lat",
            "status",
            "created_date",
            "bike_stands"
        ]]

        toulouse_station_data_df.rename(columns={
            "number": "code",
            "position.lon": "longitude",
            "position.lat": "latitude",
            "status": "status",
            "bike_stands": "capacity"
        }, inplace=True)

        con.register('toulouse_station_data_df', toulouse_station_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM toulouse_station_data_df;")
    except Exception as e:
        print(f"Erreur Toulouse: {e}")

    con.close()

def consolidate_city_data():
    """
    But : Consolider les données des villes (Paris, Nantes, Toulouse) et les insérer dans la table CONSOLIDATE_CITY.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Charger les données pour Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        paris_data = json.load(fd)
    paris_df = pd.json_normalize(paris_data)
    paris_df["nb_inhabitants"] = None  # Placeholder si les données de population ne sont pas disponibles
    paris_df = paris_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    paris_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)
    paris_df["created_date"] = date.today()

    # Charger les données pour Nantes et Toulouse
    with open(f"data/raw_data/{today_date}/commune_data.json", "r") as file:
        commune_data = json.load(file)
    commune_df = pd.DataFrame(commune_data)
    commune_df.rename(columns={'nom': 'name', 'code': 'id', 'population': 'nb_inhabitants'}, inplace=True)

    # Filtrer pour obtenir seulement Nantes et Toulouse
    nantes_toulouse_df = commune_df[commune_df['id'].isin(['44109', '31555'])]
    nantes_toulouse_df['created_date'] = date.today()

    # Combiner les données de Paris, Nantes et Toulouse
    combined_df = pd.concat([paris_df, nantes_toulouse_df])
    combined_df.drop_duplicates(inplace=True)

    print(combined_df)

    # Insérer les données combinées dans la base de données
    con.register('combined_df', combined_df)
    con.execute("INSERT INTO CONSOLIDATE_CITY SELECT * FROM combined_df;")
    con.close()

def consolidate_station_statement_data():
    """
    But : Consolider les données des états des stations (vélos disponibles, docks disponibles, etc.) 
    pour Paris, Nantes et Toulouse dans la table CONSOLIDATE_STATION_STATEMENT.
    """
    today_date = date.today().strftime("%Y-%m-%d")
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Traitement pour Paris
    try:
        with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
            data = json.load(fd)

        paris_raw_data_df = pd.json_normalize(data)
        paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"PARIS-{x}")
        paris_raw_data_df["created_date"] = today_date
        
        paris_station_statement_data_df = paris_raw_data_df[[
            "station_id",
            "numdocksavailable",
            "numbikesavailable",
            "duedate",
            "created_date"
        ]]
        
        paris_station_statement_data_df.rename(columns={
            "numdocksavailable": "bicycle_docks_available",
            "numbikesavailable": "bicycle_available",
            "duedate": "last_statement_date",
        }, inplace=True)
        
        con.register("paris_station_statement_data_df", paris_station_statement_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")
    except Exception as e:
        print(f"Erreur Paris: {e}")

        # Traitement pour Nantes
    try:
        with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
            data = json.load(fd)['results']

        nantes_raw_data_df = pd.json_normalize(data)
        nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"NANTES-{x}")
        nantes_raw_data_df["created_date"] = today_date

        nantes_station_statement_data_df = nantes_raw_data_df[[
            "station_id",
            "available_bike_stands",
            "available_bikes",
            "last_update",
            "created_date"
        ]]

        nantes_station_statement_data_df.rename(columns={
            "available_bike_stands": "bicycle_docks_available",
            "available_bikes": "bicycle_available",
            "last_update": "last_statement_date",
        }, inplace=True)

        con.register("nantes_station_statement_data_df", nantes_station_statement_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM nantes_station_statement_data_df;")
    except Exception as e:
        print(f"Erreur Nantes: {e}")

    # Traitement pour Toulouse
    try:
        with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
            data = json.load(fd)['results']

        toulouse_raw_data_df = pd.json_normalize(data)
        toulouse_raw_data_df["station_id"] = toulouse_raw_data_df["number"].apply(lambda x: f"TOULOUSE-{x}")
        toulouse_raw_data_df["created_date"] = today_date

        toulouse_station_statement_data_df = toulouse_raw_data_df[[
            "station_id",
            "available_bike_stands",
            "available_bikes",
            "last_update",
            "created_date"
        ]]

        toulouse_station_statement_data_df.rename(columns={
            "available_bike_stands": "bicycle_docks_available",
            "available_bikes": "bicycle_available",
            "last_update": "last_statement_date",
        }, inplace=True)

        con.register("toulouse_station_statement_data_df", toulouse_station_statement_data_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM toulouse_station_statement_data_df;")
    except Exception as e:
        print(f"Erreur Toulouse: {e}")

    # Fermeture de la connexion à la base de données
    con.close()

   
