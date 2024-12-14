import duckdb

# Ouvrir la base de données DuckDB
conn = duckdb.connect('data/duckdb/mobility_analysis.duckdb')

# Requête 1 : Nb d'emplacements disponibles de vélos dans une ville
query_1 = """
SELECT dm.NAME AS City_Name, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm
INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM FACT_STATION_STATEMENT)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) IN ('paris', 'nantes', 'vincennes', 'toulouse');
"""

# Exécuter la requête 1
result_1 = conn.execute(query_1).fetchall()
print("Resultat de la requête 1 :")
print(result_1)

# Requête 2 : Nb moyen de vélos disponibles par station
query_2 = """
SELECT ds.NAME AS Station_Name, ds.CODE AS Station_Code, ds.ADDRESS AS Station_Address, 
       tmp.AVG_BICYCLE_AVAILABLE
FROM DIM_STATION ds
INNER JOIN (
    SELECT STATION_ID, AVG(BICYCLE_AVAILABLE) AS AVG_BICYCLE_AVAILABLE
    FROM FACT_STATION_STATEMENT
    GROUP BY STATION_ID
) tmp ON ds.ID = tmp.STATION_ID;
"""

# Exécuter la requête 2
result_2 = conn.execute(query_2).fetchall()
print("\nResultat de la requête 2 :")
print(result_2)

# Fermer la connexion
conn.close()
