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
    consolidate_station_statement_data,
    consolidate_geo_city_data,  # Nouvelle fonction
    consolidate_nantes_station_data # Nouvelle fonction
)
from data_ingestion import (
    get_paris_realtime_bicycle_data,
    get_nantes_realtime_bicycle_data # Nouvelle fonction
)

def main():
    print("Process start.")
    
    # Data ingestion
    print("Data ingestion started.")
    get_paris_realtime_bicycle_data()
    get_nantes_realtime_bicycle_data()  # Nouvelle fonction
    print("Data ingestion ended.")

    # Data consolidation
    print("Consolidation data started.")
    create_consolidate_tables()
    consolidate_geo_city_data()  # Nouvelle fonction
    consolidate_nantes_station_data()  # Nouvelle fonction
    consolidate_station_data()
    consolidate_station_statement_data()
    print("Consolidation data ended.")

    # Data aggregation
    print("Agregate data started.")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statements()
    print("Agregate data ended.")