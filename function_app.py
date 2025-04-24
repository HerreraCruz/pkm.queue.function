import io
import os
import azure.functions as func
import logging
import json
import requests
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

@app.queue_trigger(
        arg_name="azqueue", 
        queue_name="requests",
        connection="QueueAzureWebJobsStorage"
        ) 
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]

    update_request(id, "inprogress")

    request_info = get_request(id)
    pokemons = get_pokemons(request_info[0]["type"])
    pokemons_with_stats = get_pokemons_stats(pokemons)

    pokemon_bytes = generate_csv_to_blob(pokemons_with_stats)
    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob( blob_name, csv_data=pokemon_bytes)
    logger.info(f"Archivo CSV subido a Blob Storage: {blob_name}")

    url_completa = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"

    update_request(id, "completed", url_completa)


def update_request( id: int, status: str, url:str = None) -> dict:
    payload = {
        "status": status,
        'id': id
    }
    if url:
        payload['url'] = url

    response = requests.put(f"{DOMAIN}/api/request", json=payload)
    return response.json()

def get_request(id: int) -> dict:
    response = requests.get(f"{DOMAIN}/api/request/{id}")
    return response.json()

def get_pokemons(type: str) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"

    response = requests.get(pokeapi_url, timeout=3000)
    data = response.json()
    pokemon_entries = data.get("pokemon", [])

    return [ p["pokemon"] for p in pokemon_entries]

# Obtiene las estadisticas de cada pokemon
def get_pokemons_stats(pokemons) -> list:
    pokemons_with_stats = []
    for pokemon in pokemons:
        try:
            pokemon_info = requests.get(pokemon["url"], timeout=3000)
            pokemon_info.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            data_info = pokemon_info.json()

            stats = {stat["stat"]["name"]: stat["base_stat"] for stat in data_info["stats"]}
            abilities = {ability["ability"]["name"] for ability in data_info["abilities"]}
            
            pokemon_with_stats = {**pokemon, **stats, "abilities": ", ".join(abilities)}
            pokemons_with_stats.append(pokemon_with_stats)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for Pokemon {pokemon['name']}: {e}")
        except KeyError as e:
            logger.error(f"Missing expected data for Pokemon {pokemon['name']}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for Pokemon {pokemon['name']}: {e}")

    return pokemons_with_stats

#transforma la lista de los pokemones a un csv
def generate_csv_to_blob( pokemon_list ) -> bytes:
    df = pd.DataFrame(pokemon_list)
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes

def upload_csv_to_blob( blob_name: str, csv_data: bytes ):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
    except Exception as e:
        logger.error(f"Error al subir el archivo: {e}")
        raise