import json
import requests
import pandas as pd
import datetime
import os
import logging
from utils import write_json,read_json

logger = logging.getLogger(__name__)


def get_current_datasets(ckan_url):
    """Trae info de datasets del portal. Toma un parámetro: una url de un CKAN. Devuelve
    un diccionario con datos de interés de cada dataset."""

    urls = [
        ckan_url + "api/3/action/package_search?rows=1000",
        ckan_url + "api/3/action/package_search?rows=1000&start=1000"
    ]

    full_datasets = {
        dataset['id']: {k: v for k, v in dataset.items() if k != 'resources'}
        for url in urls
        for dataset in requests.get(url).json()['result']['results']
        }

    return full_datasets


def get_current_orgs(ckan_url):
    """Trae info de organizaciones del portal. Toma un parámetro: una url de un CKAN. Devuelve
        un diccionario con el alias y el nombre completo de cada organización presente."""

    url = ckan_url + "api/3/action/organization_list?all_fields=true"
    org_dict = {org['name']: org['display_name'] for org in requests.get(url).json()['result']}
    return org_dict


def scan_organizations(org_dict, file_path):
    last_data = read_json(file_path)
    last_org_dict = last_data['organizations']
    if len(org_dict) > len(last_org_dict):
        diff = {k: org_dict[k] for k in org_dict if k not in last_org_dict}
        response = diff
    else:
        response = None
    return response


def scan_updates(new_data, org_dict, file_path, ckan_url):
    """
    Guarda en un JSON con el estado de un CKAN (datasets disponibles, organizaciones,etc). Si el
     json ya existe (se creó en interaciones anteriores), se procede a usarlo para comparar
     con el CKAN e identificar adiciones en este.

    Parámetros
    ----------
    new_data: dict -  diccionario con datasets
    org_dict:dict - diccionario con organizaciones
    file_path - string con el nombre o ruta del archivo de persistencia. Si no existe se crea con el
    nombre elegido.
    ckan_url - Enlace a un CKAN.

    Returns
    -------
    pandas.DataFrame or None
        Devuelve un DataFrame con los nuevos datasets detectados, incluyendo su ID, título, organismo
        y contacto. Si no se detectan diferencias, devuelve None.
    """

    new_dataset_list = list(new_data.keys())
    if not os.path.exists(file_path):
        data = {
                    "date": datetime.datetime.now().strftime("%d/%m/%Y"),
                    "total_datasets": len(new_dataset_list),
                    "dataset_ids": new_dataset_list,
                    "organizations": org_dict
                }
        write_json(file_path, data)
        return None
    last_data = read_json(file_path)
    last_dataset_list = last_data['dataset_ids']
    base_url = ckan_url+"dataset/"
    if len(new_dataset_list) > len(last_dataset_list):
        diffs = list(set(new_dataset_list)-set(last_dataset_list))
        diffs_df = pd.DataFrame(columns=["id", "title","maintainer", "org", "link", "contact"])
        for diff in diffs:
            id = new_data[diff]['id']
            title = new_data[diff]['title']
            maintainer = new_data[diff]['maintainer']
            org = new_data[diff]['organization']['title']
            link = base_url+id
            contact = new_data[diff]['author_email']
            row = [id,title,maintainer,org,link,contact]
            diffs_df.loc[len(diffs_df)]=row
        response = diffs_df
    else:
        response = None
    return response


def save_ckan_state(data_dict, org_dict, file_path):
    new_dataset_list = list(data_dict.keys())
    new_data = {
        "date": datetime.datetime.now().strftime("%d/%m/%Y"),
        "total_datasets": len(new_dataset_list),
        "dataset_ids": new_dataset_list,
        "organizations": org_dict
    }
    write_json(file_path, new_data)
    logger.info("guardando nuevos datasets y organizaciones en memoria")
