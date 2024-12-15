import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2
TOULOUSE_CITY_CODE = 3

def create_consolidate_tables():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    # Consolidation logic for Paris Bicycle data
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
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

    # Insert Paris data into CONSOLIDATE_STATION
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")
    
    # Consolidation logic for Nantes Bicycle data
    with open(f"data/raw_data/{today_date}/nante_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["city_code"] = NANTES_CITY_CODE
    nantes_raw_data_df["created_date"] = date.today()

    nantes_station_data_df = nantes_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
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
        "name": "name",
        "contract_name": "city_name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity"
    }, inplace=True)

    # Ensure proper casting for 'code' and 'city_code'
    nantes_station_data_df["code"] = nantes_station_data_df["code"].astype(str)
    nantes_station_data_df["city_code"] = nantes_station_data_df["city_code"].astype(str)

    # Insert Nantes data into CONSOLIDATE_STATION
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_data_df;")

    # Consolidation logic for Toulouse Bicycle data
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = today_date
      # Static value for Toulouse
    toulouse_raw_data_df["city_code"] = 31555 

    toulouse_station_data_df = toulouse_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
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
        "name": "name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "status": "status",
        "bike_stands": "capacity",
        "city_name" : "contract_name"
    }, inplace=True)


    # Convert status to 1 (OPEN) or 0 (otherwise) for Toulouse data
    toulouse_station_data_df["status"] = toulouse_station_data_df["status"].apply(lambda x: "OUI" if x == "OPEN" else "NON")

   # Insérez les données dans DuckDB
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM toulouse_station_data_df;")
#___________________________________________________________________________________________
def consolidate_city_data():
    # Connexion à la base DuckDB
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Charger les données de `commune_data.json`
    with open(f"data/raw_data/{today_date}/commune_data.json", "r") as fd:
        commune_data = json.load(fd)
    commune_df = pd.DataFrame(commune_data)

    # ____________ Traitement pour Paris _______________
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json", "r") as fd:
        paris_data = json.load(fd)

    paris_raw_df = pd.json_normalize(paris_data)
    paris_raw_df["nb_inhabitants"] = None  # Initialement None, sera rempli ensuite

    # Récupérer les données spécifiques pour Paris depuis commune_data
    paris_info = commune_df[commune_df["nom"] == "Paris"].iloc[0].to_dict()  # Trouver la ligne pour Paris
    paris_population = int(paris_info.get("population", 0))  # Convertir population en entier

    # Compléter les données pour Paris
    paris_city_data_df = paris_raw_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    paris_city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)
    paris_city_data_df.drop_duplicates(inplace=True)
    paris_city_data_df["nb_inhabitants"] = paris_population  # Mettre à jour avec la vraie population
    paris_city_data_df["created_date"] = date.today()

    # Insérer les données pour Paris dans la table
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM paris_city_data_df;")

    # _____________ Traitement pour Nantes ________________
    nantes_info = commune_df[commune_df["nom"] == "Nantes"].iloc[0].to_dict()  # Trouver la ligne pour Nantes
    nantes_population = int(nantes_info.get("population", 0))  # Convertir population en entier

    # Construire les données pour Nantes
    nantes_city_data_df = pd.DataFrame({
        "id": [nantes_info.get("code")],
        "name": [nantes_info.get("nom")],
        "nb_inhabitants": [nantes_population],
        "created_date": [date.today()]
    })

    # Insérer les données pour Nantes dans la table
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM nantes_city_data_df;")

    #___________ Traitement pour Toulouse ____________________
    toulouse_info = commune_df[commune_df["nom"] == "Toulouse"].iloc[0].to_dict()  # Trouver la ligne pour Toulouse
    toulouse_population = int(toulouse_info.get("population", 0))  # Convertir population en entier

    # Construire les données pour Toulouse
    toulouse_city_data_df = pd.DataFrame({
        "id": [toulouse_info.get("code")],
        "name": [toulouse_info.get("nom")],
        "nb_inhabitants": [toulouse_population],
        "created_date": [date.today()]
    })

    # Insérer les données pour Toulouse dans la table
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM toulouse_city_data_df;")
#___________________________________________________________________________________________________________________

def consolidate_station_statement_data():
    # Connexion à DuckDB
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    today_date = date.today().strftime('%Y-%m-%d')  # Assurez-vous d'avoir la bonne date du jour
    
    # ___________ Consolidate station statement data for Paris ____________
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = date.today()
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

    # Afficher les 10 premières lignes pour vérifier les données avant insertion
    print("Paris Station Data:")
    print(paris_station_statement_data_df.head(10))

    # Insérer dans la base de données pour Paris
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")
    
    # Consolidate station statement data for Nantes
    with open(f"data/raw_data/{today_date}/nante_realtime_bicycle_data.json") as fd:
        nantes_data = json.load(fd)

    nantes_raw_data_df = pd.json_normalize(nantes_data)

    #_____________ Traitement des données pour Nantes _______________________
    nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()

    # Extraire la date au format "YYYY-MM-DD" depuis "last_update"
    nantes_raw_data_df["last_statement_date"] = pd.to_datetime(nantes_raw_data_df["last_update"]).dt.date

    # Afficher les 10 premières lignes pour vérifier les données avant insertion
    print("Nantes Station Data:")
    print(nantes_raw_data_df.head(10))

    # Sélectionner les colonnes nécessaires pour Nantes
    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_statement_date",
        "created_date"
    ]]

    # Renommer les colonnes pour correspondre à la table de la base de données
    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available"
    }, inplace=True)

    # Afficher les 10 premières lignes après transformation
    print("Nantes Station Transformed Data:")
    print(nantes_station_statement_data_df.head(10))

    # Insérer dans la base de données pour Nantes
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM nantes_station_statement_data_df;")

    # Consolidate station statement data for Toulouse
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        toulouse_data = json.load(fd)

    toulouse_raw_data_df = pd.json_normalize(toulouse_data)

    #________________ Traitement des données pour Toulouse _____________________
    toulouse_raw_data_df["station_id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = date.today()

    # Extraire la date au format "YYYY-MM-DD" depuis "last_update"
    toulouse_raw_data_df["last_statement_date"] = pd.to_datetime(toulouse_raw_data_df["last_update"]).dt.date

    # Afficher les 10 premières lignes pour vérifier les données avant insertion
    print("Toulouse Station Data:")
    print(toulouse_raw_data_df.head(10))

    # Sélectionner les colonnes nécessaires pour Toulouse
    toulouse_station_statement_data_df = toulouse_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_statement_date",
        "created_date"
    ]]

    # Renommer les colonnes pour correspondre à la table de la base de données
    toulouse_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available"
    }, inplace=True)

    # Afficher les 10 premières lignes après transformation
    print("Toulouse Station Transformed Data:")
    print(toulouse_station_statement_data_df.head(10))

    # Insérer dans la base de données pour Toulouse
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM toulouse_station_statement_data_df;")