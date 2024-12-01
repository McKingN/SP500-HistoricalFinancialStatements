
from collections import defaultdict
import os
import json
import pandas as pd
import requests


def verify_and_find_next_json(directory):
    """
    Vérifie les fichiers JSON dans un dossier et détermine le prochain nom de fichier JSON.

    :param directory: Chemin vers le dossier contenant les fichiers JSON.
    :return: Une chaîne correspondant au prochain fichier JSON ("i.json" ou "j.json").
    """
    # Liste des fichiers dans le dossier
    files = os.listdir(directory)
    
    # Filtrer les fichiers qui ont un nom correspondant à "N.json" avec N un entier
    json_files = [f for f in files if f.endswith('.json') and f[:-5].isdigit()]
    
    if not json_files:
        # Si le dossier est vide ou aucun fichier JSON valide trouvé
        return "1.json"
    
    # Extraire les entiers des noms de fichiers (par exemple, "1.json" -> 1)
    json_indices = sorted(int(f[:-5]) for f in json_files)
    
    # Vérifier s'il manque un numéro dans la séquence de 1 à max
    for i in range(1, max(json_indices) + 1):
        if i not in json_indices:
            return f"{i}.json"
    
    # Si tous les fichiers de 1 à max existent, retourner "max + 1"
    if max(json_indices) == 54 :
        print("Le dossier est complet. Merci pour votre aide")
        return
    return f"{max(json_indices) + 1}.json"

def split_json(input_file, output_folder, chunk_size=8):
    """
    Divise un fichier JSON en plusieurs fichiers avec un nombre limité d'éléments.

    :param input_file: Chemin vers le fichier JSON source.
    :param output_folder: Dossier où enregistrer les fichiers divisés.
    :param chunk_size: Nombre maximal d'éléments par fichier.
    """
    # Vérifie si le dossier de sortie existe, sinon le crée
    os.makedirs(output_folder, exist_ok=True)
    
    # Charge les données du fichier JSON
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Obtenir toutes les clés et les diviser en groupes
    keys = list(data.keys())
    chunks = [keys[i:i + chunk_size] for i in range(0, len(keys), chunk_size)]
    
    # Crée un fichier pour chaque groupe
    for i, chunk in enumerate(chunks, start=1):
        chunk_data = {key: data[key] for key in chunk}
        output_file = os.path.join(output_folder, f"{i}.json")
        
        # Enregistre le groupe dans un fichier JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(chunk_data, f, indent=4)
        
        print(f"Fichier créé : {output_file}")

def fetch_financial_data(ticker, api_key, output_folder):
    """
    Récupère les données financières (état des résultats, bilan, et flux de trésorerie)
    pour un ticker donné via l'API Alpha Vantage. Les données sont sauvegardées dans
    un fichier JSON nommé "ticker.json" avant d'être retournées sous forme de dictionnaire.

    :param ticker: Le symbole boursier du titre.
    :param api_key: Clé API pour accéder à l'API Alpha Vantage.
    :param output_folder: Chemin vers le dossier où sauvegarder les données JSON.
    :return: Un dictionnaire contenant les données financières.
    """
    base_url = "https://www.alphavantage.co/query"
    functions = ["INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW"]
    financial_data = {}

    # Vérifie si le dossier de sortie existe, sinon le crée
    os.makedirs(output_folder, exist_ok=True)

    for func in functions:
        params = {
            "function": func,
            "symbol": ticker,
            "apikey": api_key
        }
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            financial_data[func.lower()] = response.json()
        else:
            print(f"Échec de la récupération de {func} pour {ticker}: {response.status_code}")
            financial_data[func.lower()] = None

    # Chemin du fichier de sortie
    output_file = os.path.join(output_folder, f"{ticker}.json")
    
    # Sauvegarde des données financières dans un fichier JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(financial_data, f, indent=4)
    
    print(f"Données sauvegardées dans {output_file}")

    return financial_data


def process_financial_data(financial_data, ticker):
    """
    Process raw financial data to merge income statement, balance sheet, and cash flow.
    Returns a dictionary of financial data aggregated by fiscal year.
    """
    # Extract reports from the raw financial data
    income_reports = financial_data.get("income_statement", {}).get("annualReports", [])
    balance_reports = financial_data.get("balance_sheet", {}).get("annualReports", [])
    cashflow_reports = financial_data.get("cash_flow", {}).get("annualReports", [])

    # Convert lists to DataFrames
    income_df = pd.DataFrame(income_reports)
    balance_df = pd.DataFrame(balance_reports)
    cashflow_df = pd.DataFrame(cashflow_reports)

    # Merge data on fiscalDateEnding
    combined_df = income_df.merge(balance_df, on="fiscalDateEnding", suffixes=('_income', '_balance'))
    combined_df = combined_df.merge(cashflow_df, on="fiscalDateEnding", suffixes=('', '_cashflow'))

    # Convert numeric fields to floats
    for col in combined_df.columns:
        if col != "fiscalDateEnding":
            combined_df[col] = pd.to_numeric(combined_df[col], errors="coerce")

    # Create a dictionary with fiscalDateEnding as keys
    combined_data = combined_df.set_index("fiscalDateEnding").to_dict(orient="index")

    # Add the ticker to each entry
    for date in combined_data:
        combined_data[date]["ticker"] = ticker

    return combined_data


def aggregate_financial_data(input_json_path, api_key, output_json_path, outDir):
    """
    Aggregate financial data for tickers listed in the input JSON file.
    Outputs a JSON file with combined financial data for all tickers by fiscal year.
    """
    # Load tickers from input JSON file
    with open(input_json_path, "r") as file:
        tickers_data = json.load(file)

    aggregated_data = defaultdict(dict)

    for ticker, details in tickers_data.items():
        print(f"Processing {ticker}...")
        financial_data = fetch_financial_data(ticker, api_key, outDir)
        if all(financial_data.values()):  # Check if all API calls were successful
            processed_data = process_financial_data(financial_data, ticker)
            for date, data in processed_data.items():
                aggregated_data[date][ticker] = data
        else:
            print(f"Skipping {ticker} due to missing data.")

    # Save aggregated data to JSON
    with open(output_json_path, "w") as output_file:
        json.dump(aggregated_data, output_file, indent=4)

    print(f"Aggregated financial data saved to {output_json_path}")

def main(api, components_dir="data\FilteredSP500_components_splitted", results_dir="data\SP500_components_CombinedStatements", saveStockStatementsDir="data\SP500_components_statements"):
    inputFileName = verify_and_find_next_json(results_dir)
    inputFilePath = os.path.join(components_dir, inputFileName)
    outputFilePath = os.path.join(results_dir, inputFileName)
    aggregate_financial_data(input_json_path=inputFilePath, api_key=api, output_json_path=outputFilePath, outDir=saveStockStatementsDir)
