import glob
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import config


def scrap_from_url(url_wiki:str):
    """Function to scrap data from a website, in this case : Wikipedia.
    
    Input : url of the wikipedia website

    Output : dataframe with the information on a jsonfile.
    """
    html_data = requests.get(url_wiki).text
    soup=BeautifulSoup(html_data, "html.parser")
    data = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])
    for row in soup.find_all('tbody')[3].find_all('tr'):
        col = row.find_all('td')
        if (col != []):
            name = col[1].text.rstrip()
            market_cap = col[2].text.rstrip()
            data = data.append({"Name" : name,
                                "Market Cap (US$ Billion)" : market_cap},
                                ignore_index=True)
    return data.to_json("data/raw/bank_market_cap.json")

def extract_from_api(url_api:str):
    """Function extract data from a an API. Here, APIlayer.

    Input: Link with a personal token inside.

    Output: dataframe in a csv file.
    """
    text = requests.get(url_api).text
    df = pd.read_json(text).reset_index()
    df.rename(columns={"index":"currency"}, inplace=True)
    df.drop(["success", "timestamp",
            "base", "date"], axis=1,
            inplace=True)
    return df.to_csv("data/raw/exchange_rates.csv", index=False)

def extract_from_json(file_to_process:str):
    """Function to open jsonfile.
    
    Input: A JSON file.
    
    Output: Dataframe Pandas."""
    dataframe = pd.read_json(file_to_process)
    return dataframe

# Creation of different files in order to save results
tmpfile = "temp.tmp"
logfile = "log/logfile.txt"
targetfile = "data/processed/bank_market_cap.csv"

def extract():
    """Function to extract informations from a json file. Take json files
    one by one and add to create a dataframe.

    Input: Different JSON file.

    Output: Dataframe with data extract.
    """
    columns=['Name','Market Cap (US$ Billion)']
    extracted_data = pd.DataFrame(columns=columns)
    for jsonfile in glob.glob("data/raw/*1.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    return extracted_data

def create_exchange_rate(data_path:str, currency:str):
    """Function to create a dataframe with the data coming from the API.
    
    Input: Data in a csv format & currency to keep.

    Output: variable containing a float for conversion.
    """
    rate = pd.read_csv(data_path)
    exchange_rate = float(rate["rates"][rate["currency"] == currency].values)
    return exchange_rate

def transform(data, exchange_rate:float, currency:str):
    """Function of data transformation. Take the information coming the exchange rate to make a conversion.
    
    Input: Dataframe, value needed to the transformation & name of the currency.

    Output: Dataframe with the currency changing.
    """
    data["Market Cap (US$ Billion)"] = data["Market Cap (US$ Billion)"] * exchange_rate
    data.rename(columns={"Market Cap (US$ Billion)":f"Market Cap ({currency} Billion)"}, inplace=True)
    return data

def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile, index=False)

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("log/logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

if __name__=="__main__":
    scrap_from_url(config.wiki_url)
    extract_from_api(config.api_url)

    log("ETL Job Started")

    log("Extract phase Started")
    extracted_data = extract()
    exchange_rate = create_exchange_rate("data/raw/exchange_rates.csv", 'GBP')
    log("Extract phase Ended")

    log("Transform phase Started")
    transformed_data = transform(extracted_data, exchange_rate, "GBP")
    log("Transform phase Ended")

    log("Load phase Started")
    load(targetfile,transformed_data)
    log("Load phase Ended")

    log("ETL Job Ended")