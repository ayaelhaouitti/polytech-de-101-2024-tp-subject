import os
from datetime import datetime
import requests

def serialize_data(raw_json: str, file_name: str):
    """
    But : Sérialiser les données JSON récupérées en les enregistrant dans un fichier local 
    organisé par date dans le dossier 'data/raw_data'.
    
    Paramètres :
    - raw_json (str) : Données JSON sous forme de chaîne de caractères.
    - file_name (str) : Nom du fichier dans lequel les données doivent être enregistrées.
    """
    today_date = datetime.now().strftime("%Y-%m-%d")
    directory_path = f"data/raw_data/{today_date}"
    
    # Créer le répertoire si inexistant
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    
    # Écrire les données dans le fichier
    with open(f"{directory_path}/{file_name}", "w") as fd:
        fd.write(raw_json)

def get_paris_realtime_bicycle_data():
    """
    But : Récupérer les données de disponibilité des vélos en temps réel pour Paris 
    à partir de l'API de la ville de Paris, puis les sérialiser localement.
    """
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    response = requests.get(url)
    
    # Vérifier la réponse avant sérialisation
    if response.status_code == 200:
        serialize_data(response.text, "paris_realtime_bicycle_data.json")
    else:
        print(f"Erreur lors de la récupération des données de Paris : {response.status_code}")

def get_nantes_realtime_bicycle_data():
    """
    But : Récupérer les données de disponibilité des vélos en temps réel pour Nantes 
    à partir de l'API de Nantes Métropole, puis les sérialiser localement.
    """
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/records"
    response = requests.get(url)
    
    # Vérifier la réponse avant sérialisation
    if response.status_code == 200:
        serialize_data(response.text, "nantes_realtime_bicycle_data.json")
    else:
        print(f"Erreur lors de la récupération des données de Nantes : {response.status_code}")

def get_toulouse_realtime_bicycle_data():
    """
    But : Récupérer les données de disponibilité des vélos en temps réel pour Toulouse 
    à partir de l'API de Toulouse Métropole, puis les sérialiser localement.
    """
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/records"
    response = requests.get(url)
    
    # Vérifier la réponse avant sérialisation
    if response.status_code == 200:
        serialize_data(response.text, "toulouse_realtime_bicycle_data.json")
    else:
        print(f"Erreur lors de la récupération des données de Toulouse : {response.status_code}")

def get_commune_data():
    """
    But : Récupérer les informations sur les communes françaises (nom, code INSEE, population) 
    à partir de l'API gouvernementale française, puis les sérialiser localement.
    """
    url = "https://geo.api.gouv.fr/communes?fields=nom,code,population&format=json"
    response = requests.get(url)
    
    # Vérifier la réponse avant sérialisation
    if response.status_code == 200:
        serialize_data(response.text, "commune_data.json")
    else:
        print(f"Erreur lors de la récupération des données des communes : {response.status_code}")
