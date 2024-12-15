import duckdb

def create_agregate_tables():
    """ But: Créer les tables agrégées en exécutant des instructions SQL """
    # Connexion à la base de données DuckDB en mode écriture
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Lecture des instructions SQL à partir d'un fichier externe
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        
        # Exécution de chaque instruction SQL séparée par un point-virgule
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def agregate_dim_station():
    """ But: Agréger les données des stations dans la table DIM_STATION """
    # Connexion à la base de données DuckDB en mode écriture
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Instruction SQL pour insérer ou remplacer les données dans la table DIM_STATION
    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT 
        ID,
        CODE,
        NAME,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITTY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """
    
    # Exécution de la requête
    con.execute(sql_statement)

def agregate_dim_city():
    """ But: Agréger les données des villes dans la table DIM_CITY """
    # Connexion à la base de données DuckDB en mode écriture
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Instruction SQL pour insérer ou remplacer les données dans la table DIM_CITY
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """
    
    # Exécution de la requête
    con.execute(sql_statement)

def agregate_fact_station_statements():
    """ But: Agréger les déclarations des stations dans la table FACT_STATION_STATEMENT """
    # Connexion à la base de données DuckDB en mode écriture
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Instruction SQL pour insérer ou remplacer les données dans la table FACT_STATION_STATEMENT
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
    JOIN CONSOLIDATE_STATION 
        ON CONSOLIDATE_STATION_STATEMENT.STATION_ID = CONSOLIDATE_STATION.ID
    WHERE 
        CONSOLIDATE_STATION_STATEMENT.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION_STATEMENT)
        AND CONSOLIDATE_STATION.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """
    
    # Exécution de la requête
    con.execute(sql_statement)
