from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements
)
from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data
)
from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_toulouse_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data,
    get_commune_data
)

def main():
    """
    But : Automatiser le processus complet de traitement des données de mobilité urbaine.
    Ce processus inclut trois phases principales :
    - Ingestion des données à partir des API
    - Consolidation des données pour structurer l'information
    - Agrégation des données pour analyse et reporting
    """
    print("Process start.")

    # **Phase 1 : Ingestion des données**
    print("Data ingestion started.")
    try:
        get_paris_realtime_bicycle_data()        # Récupération des données de Paris
        get_toulouse_realtime_bicycle_data()    # Récupération des données de Toulouse
        get_nantes_realtime_bicycle_data()      # Récupération des données de Nantes
        get_commune_data()                      # Récupération des informations sur les communes
        print("Data ingestion ended.")
    except Exception as e:
        print(f"Erreur lors de l'ingestion des données : {e}")

    # **Phase 2 : Consolidation des données**
    print("Consolidation data started.")
    try:
        create_consolidate_tables()             # Création des tables de consolidation
        consolidate_city_data()                 # Consolidation des données des villes
        consolidate_station_data()              # Consolidation des stations de vélos
        consolidate_station_statement_data()    # Consolidation des états des stations
        print("Consolidation data ended.")
    except Exception as e:
        print(f"Erreur lors de la consolidation des données : {e}")

    # **Phase 3 : Agrégation des données**
    print("Agregate data started.")
    try:
        create_agregate_tables()                # Création des tables d'agrégation
        agregate_dim_city()                     # Agrégation des dimensions de ville
        agregate_dim_station()                 # Agrégation des dimensions de station
        agregate_fact_station_statements()     # Agrégation des faits concernant les stations
        print("Agregate data ended.")
    except Exception as e:
        print(f"Erreur lors de l'agrégation des données : {e}")

    print("Process completed successfully.")

if __name__ == "__main__":
    main()
