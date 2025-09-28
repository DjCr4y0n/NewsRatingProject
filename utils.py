import pandas as pd

import StockMarketDataScraper as sm
from cerebras.cloud.sdk import Cerebras
import time
from google import genai

companies = {
    "mbank": ("109", "MBK.WA"),
    "budimex": ("112", "BDX.WA"),
    "sanpl": ("117", "SPL.WA"),
    "ccc": ("456", "CCC.WA"),
    "kety": ("274", "KTY.WA"),
    "kghm": ("350", "KGH.WA"),
    "lpp": ("380", "LPP.WA"),
    "cdprojekt": ("476", "CDR.WA"),
    "pekao": ("76", "PEO.WA"),
    "pknorlen": ("511", "PKN.WA"),
    "pkobp": ("512", "PKO.WA"),
    "orangepl": ("636", "OPL.WA"),
    "pge": ("503", "PGE.WA"),
    "pzu": ("558", "PZU.WA"),
    "kruk": ("558", "KRU.WA"),
    "alior": ("1180", "ALR.WA"),
    "dinopl": ("1431", "DNP.WA"),
    "pepco": ("1593", "PCO.WA"),
    "zabka": ("1737", "ZAB.WA"),
    "allegro": ("1559", "ALE.WA")
}


def get_stock_price_for_companies(df):
    for idx, row in df.iterrows():
        tck = str(row.ticker) if "ticker" in df.columns else None
        if not tck or tck.lower() == "nan":
            continue
        prices = sm.get_stock_data(tck, row.date)
        for label, value in prices.items():
            df.loc[idx, label] = value
    return df


client = Cerebras(api_key=("csk-vn3d58346whrvj4fcycyvt3drxtnn5ywnc6nknj4nvtc2jr2"))
clientGemini = genai.Client(api_key="AIzaSyDS7jEPic1DaBGeCrALyhTzVdapSmXkj-M")


def get_company_name_from_content(news: str) -> str:
    if not hasattr(get_company_name_from_content, "counter"):
        get_company_name_from_content.counter = 0

    if get_company_name_from_content.counter > 0 and get_company_name_from_content.counter % 30 == 0:
        time.sleep(60)

    prompt = f"""
    Given the following list of companies from the WIG20 index:
    1. mbank
    2. budimex
    3. sanpl
    4. ccc
    5. kety
    6. kghm
    7. lpp
    8. cdprojekt
    9. pekao
    10. pknorlen
    11. pkobp
    12. orangepl
    13. pge
    14. pzu
    15. kruk
    16. alior
    17. dinopl
    18. pepco
    19. zabka
    20. allegro

    You will receive a passage of text. Identify the single company that the passage is about—either mentioned directly or implied. If multiple companies are referenced, 
    choose the one most central to the main topic. If no company fits, answer with the word Nan. Respond with only one word: the company name or Nan. Do not add any punctuation, 
    explanation, extra characters or extra styling.

    Output requirements:
    - Respond with only one word – the company name or “Nan”.
    - Do not include any punctuation, explanation, or extra characters.

    Text:
    {news}
    """

    resp = client.chat.completions.create(
        model="llama-3.3-70b",
        messages=[{"role": "user", "content": prompt}],
    )
    text = resp.choices[0].message.content.strip()
    get_company_name_from_content.counter += 1
    return text.split()[0] if text else "Nan"


def map_company_to_ticker(name: str) -> str:
    if not name:
        return "Nan"
    return companies.get(name.strip().lower(), ("", "Nan"))[1]


def get_rate(title: str, news: str, company: str):
    if not hasattr(get_rate, "counter"):
        get_rate.counter = 0

    if not company or company.strip().lower() == "nan":
        return "Nan"


    if get_rate.counter > 0 and get_rate.counter % 30 == 0:
        time.sleep(60)

    prompt = f"""
    You are an expert financial analyst AI.

    Given a news article’s title, content, and the company name it is about, analyze the potential impact this news could have on the company’s stock market price. 
    Consider sentiment, relevance, financial/operational implications, and investor behavior.

    Output a single integer from 1 to 10, where:
    1 = No impact on stock price
    10 = Extremely strong impact on stock price

    Do not provide any explanation or text besides the number.

    If Company Name is "Nan", then respond with "Nan".

    Input:
    Title: {title}
    Content: {news}
    Company: {company}
    """

    response = client.chat.completions.create(
        model="llama-3.3-70b",
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.choices[0].message.content.strip()

    get_rate.counter += 1

    token = text.split()[0] if text else "Nan"
    if token.isdigit() and 1 <= int(token) <= 10:
        return token
    if token.lower() == "nan":
        return "Nan"
    return "Nan"

