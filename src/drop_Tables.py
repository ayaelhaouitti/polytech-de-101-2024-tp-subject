import duckdb

def drop_all_tables():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    try:
        tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
        for table in tables:
            con.execute(f"DROP TABLE IF EXISTS {table[0]}")
            print(f"Table {table[0]} dropped.")
    except Exception as e:
        print(f"Error dropping tables: {e}")
    finally:
        con.close()

drop_all_tables()
