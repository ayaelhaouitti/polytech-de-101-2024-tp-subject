import json

def print_nantes_json_structure():
    # Remplacez par le chemin réel du fichier JSON pour Nantes
    file_path = f"data/raw_data/2024-12-10/nantes_realtime_bicycle_data.json"
    
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            print(json.dumps(data, indent=4, ensure_ascii=False))  # Imprime les données formatées
    except FileNotFoundError:
        print(f"Le fichier {file_path} n'existe pas.")
    except json.JSONDecodeError:
        print("Erreur de décodage JSON.")

print_nantes_json_structure()

import json

def print_toulouse_json_structure():
    # Remplacez par le chemin réel du fichier JSON pour Toulouse
    file_path = f"data/raw_data/2024-12-10/toulouse_realtime_bicycle_data.json"
    
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            print(json.dumps(data, indent=4, ensure_ascii=False))  # Imprime les données formatées
    except FileNotFoundError:
        print(f"Le fichier {file_path} n'existe pas.")
    except json.JSONDecodeError:
        print("Erreur de décodage JSON.")

print_toulouse_json_structure()
