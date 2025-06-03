import unittest
import scanning as sc
from texts import text_one_org, text_one_dataset, text_sev_orgs, text_sev_dataset
from utils import new_org_message, new_data_message
import pandas as pd
import json
import datetime
from main import send_update
import asyncio
from utils import write_json, read_json
from reporting import send_email_report
from dotenv import load_dotenv
import os

load_dotenv()
sender = os.getenv("SENDER_EMAIL")
sender_pass=os.getenv("EMAIL_PASS")
receivers= os.getenv("RECEIVERS")


def get_text(file_path):
    text = None
    ckan_url = "https://datos.gob.ar/"
    data_dict = read_json("new_data_datasets.json")
    org_dict = read_json("new_data_orgs.json")
    updates = sc.scan_updates(data_dict, org_dict, file_path, ckan_url)

    if not isinstance(updates, pd.DataFrame):
        return text
    else:
        org_updates = sc.scan_organizations(org_dict, file_path)
        if isinstance(org_updates, dict):
            org_updates_list = [v for k, v in org_updates.items()]
            org_in_data = updates['org'].tolist()
            org_inter = list(set(org_updates_list) & set(org_in_data))
            if org_inter:
                text = new_org_message(org_updates, org_inter, ckan_url)
                return text
        text = new_data_message(updates)
        return text


class TestCsv(unittest.TestCase):
    def test_scan_updates(self):
        ckan_url = "https://datos.gob.ar/"
        data_dict = read_json("new_data_datasets.json")
        org_dict = read_json("new_data_orgs.json")
        result = sc.scan_updates(data_dict,org_dict, "test_last_data_case1.json",ckan_url)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(3, len(result))

    def test_scan_organizations_case1(self):
        org_dict = read_json("new_data_orgs.json")
        result = sc.scan_organizations(org_dict, "test_last_data_case1.json")
        self.assertIsInstance(result, dict)
        self.assertEqual(1, len(result))


    def test_scan_organizations_case2(self):
        """Prueba para situación en que no hay nodo nuevo: scan_organizations debería
        devolver None"""
        org_dict = read_json("new_data_orgs.json")
        result = sc.scan_organizations(org_dict, "test_last_data_case2.json")
        self.assertIsNone(result)


    def test_send_updates_case1(self):
        """Prueba para escenario de nodo nuevo y 3 datasets nuevos, debería elegir
         texto nada más por nodo"""
        text = get_text("test_last_data_case1.json")
        right_text = text_one_org(["Secretaría de Energía"], "https://datos.gob.ar/dataset?organization=energia")
        asyncio.run(send_update(text))
        self.assertEqual(right_text, text)


    def test_send_updates_case2(self):
        """Prueba para escenario en que hay 1 dataset nuevo pero no hay nodo nuevo: debería elegir
         texto de 1 dataset"""
        text = get_text("test_last_data_case2.json")
        ckan_url = "https://datos.gob.ar/"
        data_dict = read_json("new_data_datasets.json")
        org_dict = read_json("new_data_orgs.json")
        updates = sc.scan_updates(data_dict, org_dict, "test_last_data_case2.json", ckan_url)
        right_text = text_one_dataset(updates)
        asyncio.run(send_update(text))
        self.assertEqual(right_text, text)


    def test_send_updates_case3(self):
        """Prueba para escenario en que hay 3 dataset nuevo pero no hay nodo nuevo: debería elegir
         texto de varios datasets"""
        text = get_text("test_last_data_case3.json")
        ckan_url = "https://datos.gob.ar/"
        data_dict = read_json("new_data_datasets.json")
        org_dict = read_json("new_data_orgs.json")
        updates = sc.scan_updates(data_dict, org_dict,"test_last_data_case3.json", ckan_url)
        right_text = text_sev_dataset(updates)
        asyncio.run(send_update(text))
        self.assertEqual(right_text, text)

    def test_send_updates_case4(self):
        """Prueba para escenario en que hay 3 nodos nuevos, pero sólo dos
        con datos: debería mostrar nodos"""

        text = get_text("test_last_data_case4.json")
        asyncio.run(send_update(text))
        self.assertIn("Secretaría de Energía", text)
        self.assertIn("Autoridad de Cuenca Matanza Riachuelo", text)
        self.assertTrue(text.startswith("🎉 ¡Hay 2 nodos nuevos"))

    def test_persistance(self):
        pass

    def test_send_report(self):
        send_email_report(
            sender_email=sender,
            sender_password=sender_pass,
            recipient_email=receivers,
            subject='Telegram Bot Report',
            body='El bot corrió correctamente')

