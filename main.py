import scanning as sc
from utils import new_org_message, new_data_message
from dotenv import load_dotenv
import os
from telegram import Bot
import asyncio
import logging
import pandas as pd
from reporting import send_email_report


load_dotenv()
channel_username = os.getenv("CHANNEL_USERNAME")
bot_token = os.getenv("BOT_TOKEN")
ckan_url = os.getenv("CKAN_URL")
pers_path = os.getenv("PERS_PATH")
sender = os.getenv("SENDER_EMAIL")
sender_pass=os.getenv("EMAIL_PASS")
receivers= os.getenv("RECEIVERS")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


async def send_update(message):
    bot = Bot(token=bot_token)
    await bot.send_message(chat_id=channel_username, text=message,parse_mode="MarkdownV2")


def main(link_ckan, file_path):
    try:
        data_dict = sc.get_current_datasets(link_ckan)
        org_dict = sc.get_current_orgs(link_ckan)
        updates = sc.scan_updates(data_dict, org_dict, file_path, link_ckan)
        # Si no hay datos nuevos termina el proceso, no compara organizaciones.
        if not isinstance(updates, pd.DataFrame):
            sc.save_ckan_state(data_dict, org_dict, file_path)
            return "No hay nuevos datos en el portal"
        else:
            org_updates = sc.scan_organizations(link_ckan, file_path)
            if isinstance(org_updates, dict):
                #Se asegura que los nodos nuevos tengan alguna data publicada
                org_updates_list = [v for k, v in org_updates.items()]
                org_in_data = updates['org'].tolist()
                org_inter = list(set(org_updates_list) & set(org_in_data))
                if org_inter:
                    text = new_org_message(org_updates, org_inter, link_ckan)
                    sc.save_ckan_state(data_dict, org_dict, file_path)
                    return asyncio.run(send_update(text))
            text = new_data_message(updates)
            sc.save_ckan_state(data_dict, org_dict, file_path)
            return asyncio.run(send_update(text))
    except Exception as e:
        logger.error(f"No se pudo comprobar actualizaciones:{e}")


if __name__ == "__main__":
    try:
        main(ckan_url, pers_path)
        send_email_report(
            sender_email=sender,
            sender_password= sender_pass,
            recipient_email=receivers,
            subject='Telegram Bot Report',
            body='El bot corrió correctamente',
        )
    except Exception as e:
        send_email_report(
            sender_email=sender,
            sender_password=sender_pass,
            recipient_email=receivers,
            subject='Error - Telegram Bot Report',
            body=f'El bot no corrió correctamente:{e}',
        )
