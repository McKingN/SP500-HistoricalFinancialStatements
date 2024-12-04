import os
import json
from collections import defaultdict
from datetime import datetime
from typing import Dict

def merge_quarterly_data(input_folder: str, output_folder: str):
    """
    Combine les données trimestrielles des fichiers JSON dans un format unique par date.

    Args:
        input_folder (str): Chemin vers le dossier contenant les fichiers JSON d'entrée.
        output_folder (str): Chemin vers le dossier où enregistrer les fichiers JSON de sortie.
    """
    # Vérifier que le dossier de sortie existe ou le créer
    os.makedirs(output_folder, exist_ok=True)
    
    # Parcourir tous les fichiers du dossier d'entrée
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename)

            # Charger le fichier JSON
            with open(input_path, "r") as file:
                data = json.load(file)
            
            # Fusionner les données trimestrielles
            merged_data = defaultdict(dict)
            for section in ["income_statement", "balance_sheet", "cash_flow"]:
                if section in data:
                    quarterly_reports = data[section].get("quarterlyReports", [])
                    for report in quarterly_reports:
                        date = report["fiscalDateEnding"]
                        for key, value in report.items():
                            if key != "fiscalDateEnding":  # Éviter de répéter la date
                                merged_data[date][key] = value
            
            # Sauvegarder le fichier JSON résultant
            with open(output_path, "w") as outfile:
                json.dump(merged_data, outfile, indent=4)

def filter_stock_data_by_dates(dates: list[str], input_folder: str, output_folder: str):
    """
    Filtre les données financières des fichiers JSON dans le dossier d'entrée
    pour les dates données, et les écrit dans un nouveau fichier JSON dans le dossier de sortie.

    Args:
        dates (list[str]): Liste des dates pour lesquelles extraire les données.
        input_folder (str): Chemin vers le dossier contenant les fichiers JSON d'entrée.
        output_folder (str): Chemin vers le dossier où enregistrer les fichiers JSON de sortie.
    """
    # Assurez-vous que le dossier de sortie existe
    os.makedirs(output_folder, exist_ok=True)

    # Convertir la liste des dates en objets datetime pour faciliter les comparaisons
    target_dates = sorted([datetime.strptime(date, "%Y-%m-%d") for date in dates])

    # Parcourir les fichiers dans le dossier d'entrée
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            print(f"Processing {filename} datas...")
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename)

            # Charger le fichier JSON
            with open(input_path, "r") as file:
                data = json.load(file)

            # Convertir les dates du dictionnaire en objets datetime
            data_dates = {
                datetime.strptime(date, "%Y-%m-%d"): values for date, values in data.items()
            }

            # Filtrer les données pour chaque date cible
            filtered_data = {}
            for target_date in target_dates:
                # Trouver la date maximale inférieure ou égale à la date cible
                valid_dates = [date for date in data_dates if date <= target_date]
                if valid_dates:
                    closest_date = max(valid_dates)
                    filtered_data[target_date.strftime("%Y-%m-%d")] = data_dates[closest_date]

            # Sauvegarder le fichier JSON filtré
            with open(output_path, "w") as outfile:
                json.dump(filtered_data, outfile, indent=4)


def concat_stocks_datas_byDates(inputDir, pricesFile, infoFile):
    # Chargement des données des prix et des informations
    with open(pricesFile, 'r') as f:
        SP500Prices = json.load(f)
    dates = list(SP500Prices.keys())

    with open(infoFile, 'r') as f:
        SP500ComponentsInfos = json.load(f)
    SP500Components = list(SP500ComponentsInfos.keys())

    # Précharger les données des fichiers d'actions
    stock_data = {}
    for stock in SP500Components:
        filename = f"{stock}.json"
        filepath = os.path.join(inputDir, filename)
        try:
            with open(filepath, 'r') as f:
                stock_data[stock] = json.load(f)
        except FileNotFoundError:
            print(f"Warning: File for stock {stock} not found. Skipping.")
            stock_data[stock] = {}

    # Concaténation des données
    data = defaultdict(dict)
    for date in dates:
        print(f"Processing concatenation on {date}")
        for stock in SP500Components:
            print(f"\tCopying {stock} data")
            if date in stock_data[stock]:
                data[date][stock] = stock_data[stock][date]
                data[date][stock]["Adj Close"] = SP500Prices[date][stock]

    # Écriture des résultats dans un fichier de sortie
    outputFile = "SP500ComponentsDailyFinanDataConcat.json"
    with open(outputFile, 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Data concatenation completed. Output saved to {outputFile}.")


def calculate_financial_ratios(input_file: str, output_file: str) -> Dict:
    """
    Calcule les ratios financiers pour chaque stock à chaque date dans un fichier JSON.
    
    Args:
        input_file (str): Chemin du fichier JSON d'entrée.
        output_file (str): Chemin du fichier JSON de sortie.
        
    Returns:
        Dict: Dictionnaire contenant les ratios financiers.
    """
    def safe_divide(numerator, denominator):
        try:
            return float(numerator) / float(denominator)
        except (ValueError, ZeroDivisionError, TypeError) as e:
            print(e)
            return 0

    def safe_get(metrics, key, default, date, stock):
        value = metrics.get(key, "0")
        if value=="None":
            print(f"[INFO] Missing value for '{key}' on {date}, stock {stock}. Defaulting to {default}.")
            return default
        return float(value)

    with open(input_file, 'r') as file:
        data = json.load(file)

    results = {}

    for date, stocks in data.items():
        results[date] = {}
        for stock, metrics in stocks.items():
            try:
                # Utilisation de safe_get pour gérer les valeurs manquantes
                total_equity = safe_get(metrics, "totalShareholderEquity", 0, date, stock)
                total_debt = safe_get(metrics, "shortLongTermDebtTotal", 0, date, stock)
                net_income = safe_get(metrics, "netIncome", 0, date, stock)
                operating_cashflow = safe_get(metrics, "operatingCashflow", 0, date, stock)
                total_revenue = safe_get(metrics, "totalRevenue", 0, date, stock)
                dividends_paid = safe_get(metrics, "dividendPayoutCommonStock", 0, date, stock)
                shares_outstanding = safe_get(metrics, "commonStockSharesOutstanding", 0, date, stock)
                adj_close = safe_get(metrics, "Adj Close", 0, date, stock)

                # Calcul des ratios
                book_value_per_share = safe_divide(total_equity, shares_outstanding)
                roe = safe_divide(net_income, total_equity)  # Return on Equity
                debt_to_equity = safe_divide(total_debt, total_equity)  # Debt-to-Equity
                profit_margin = safe_divide(net_income, total_revenue)  # Profit Margin
                operating_cashflow_debt = safe_divide(operating_cashflow, total_debt)  # Operating Cashflow to Debt
                market_to_book = safe_divide(adj_close, book_value_per_share)  # Market-to-Book
                market_cap = shares_outstanding * adj_close
                dividend_rate = safe_divide(dividends_paid, shares_outstanding)
                dividend_yield = safe_divide(dividend_rate, adj_close)  # Dividend Yield

                results[date][stock] = {
                    "ROE": roe,
                    "DebtToEquity": debt_to_equity,
                    "ProfitMargin": profit_margin,
                    "OperatingCashFlowDebt": operating_cashflow_debt,
                    "MarketToBook": market_to_book,
                    "DividendYield": dividend_yield,
                    "MarketCap": market_cap,
                    "Adj Close": adj_close
                }
            except Exception as e:
                results[date][stock] = {
                    "error": f"Failed to calculate metrics for {stock}: {str(e)}"
                }

    with open(output_file, 'w') as file:
        json.dump(results, file, indent=4)

    return results



if __name__ == '__main__':
    input_path = "SP500ComponentsDailyFinanDataConcat.json"
    o_path = "SP500ComponentsDailyFinanRatios.json"
    with open(o_path, 'r') as f:
        results = json.load(f)
    with open("filtered_sp500_components.json", 'r') as f:
        infos = json.load(f)

