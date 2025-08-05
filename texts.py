import re

import re

def escape_md(text):
    escape_chars = r'_*\[\]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)


def text_one_dataset(update_df,org_dict):
    maintainer = update_df['maintainer'].iloc[0]
    verbose_org = org_dict[update_df['org'].iloc[0]]
    if maintainer.strip() == verbose_org.strip():
        text = (
            f"📢 {escape_md(verbose_org)} publicó un nuevo dataset:\n\n"
            f"📊 {escape_md(update_df['title'].iloc[0])}\n"
            f"🔗 Podés consultarlo **[acá]({update_df['link'].iloc[0]})**\n"
        )
    else:
        text = (
            f"📢 {escape_md(maintainer + ' - '+verbose_org)} publicó un nuevo dataset:\n\n"
            f"📊 {escape_md(update_df['title'].iloc[0])}\n"
            f"🔗 Podés consultarlo **[acá]({update_df['link'].iloc[0]})**\n"
        )
    return text

def text_sev_dataset(update_df,org_dict):
    texts = []
    if len(update_df['maintainer'].unique())>1:
        for maintainer in update_df['maintainer'].unique().tolist():
            maintainer_df = update_df.loc[update_df['maintainer']==maintainer]
            if len(maintainer_df)==1:
                text = text_one_dataset(maintainer_df,org_dict)
                texts.append(text)
            elif len(maintainer_df)>1:
                verbose_org = org_dict[maintainer_df['org'].iloc[0]]
                if maintainer.strip() == verbose_org.strip():
                    text = f"📈 {escape_md(verbose_org)} publicó {len(maintainer_df)} datasets nuevos en el Portal Nacional de Datos Abiertos:\n\n"
                    for _, row in update_df.iterrows():
                        text += f"🔹 **[{escape_md(row['title'])}]({row['link']})**\n"
                else:
                    text = f"📈 {escape_md(maintainer + ' - ' + verbose_org)} publicó {len(maintainer_df)} datasets nuevos en el Portal Nacional de Datos Abiertos:\n\n"
                    for _, row in update_df.iterrows():
                        text += f"🔹 **[{escape_md(row['title'])}]({row['link']})**\n"
                texts.append(text)
    else:
        maintainer = update_df['maintainer'].iloc[0]
        verbose_org = org_dict[update_df['org'].iloc[0]]
        if maintainer == verbose_org:
            text = f"📈 {escape_md(verbose_org)} publicó {len(update_df)} datasets nuevos en el Portal Nacional de Datos Abiertos:\n\n"
            for _, row in update_df.iterrows():
                text += f"🔹 **[{escape_md(row['title'])}]({row['link']})**\n"
        else:
            text = f"📈 {escape_md(maintainer + ' - ' + verbose_org)} publicó {len(update_df)} datasets nuevos en el Portal Nacional de Datos Abiertos:\n\n"
            for _, row in update_df.iterrows():
                text += f"🔹 **[{escape_md(row['title'])}]({row['link']})**\n"
        texts.append(text)
    return texts

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
