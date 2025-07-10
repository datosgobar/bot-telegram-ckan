import re

import re

def escape_md(text):
    escape_chars = r'_*\[\]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


def text_one_dataset(update_df,org_dict):
    text = (
        f"📢 {escape_md(update_df['maintainer'].iloc[0] + '-' +org_dict[update_df['org'].iloc[0]])} publicó un nuevo dataset:\n\n"
        f"📊 {escape_md(update_df['title'].iloc[0])}\n"
        f"🔗 Podés consultarlo **[acá]({update_df['link'].iloc[0]})**\n"
    )
    return text

def text_sev_dataset(update_df):
    text = f"📈 Hay {len(update_df)} datasets nuevos en el Portal Nacional de Datos Abiertos:\n\n"
    for _, row in update_df.iterrows():
        text += f"🔹 **[{escape_md(row['title'])}]({row['link']})**\n"
    return text

def text_one_org(alias, org_url,org_updates):
    text = (
        f"🎉 {escape_md('¡Excelentes noticias! Tenemos nuevo nodo:')}\n\n"
        f"🏢 **[{escape_md(org_updates[alias])}]({org_url})**\n\n"
    )
    return text

def text_sev_orgs(org_inter, org_updates, ckan_portal):
    text = f"🎉 {escape_md(f'¡Hay {len(org_inter)} nodos nuevos en el Portal Nacional de Datos Abiertos!')}\n\n"
    for org in org_inter:
        alias = org
        org_url = f"{ckan_portal}dataset?organization={alias}"
        text += f"🔹 **[{escape_md(org_updates[org])}]({org_url})**\n"
    return text
