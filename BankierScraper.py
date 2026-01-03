import time
import random
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import utils

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.12.388 Version/12.18',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36'
]

def parse(url, page_number=None):
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[503, 502, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    if page_number is None:
        page = session.get(f'{url}', headers={'User-Agent': random.choice(user_agents)})
    else:
        page = session.get(f'{url}/{page_number}', headers={'User-Agent': random.choice(user_agents)})
    print(page)
    soup = BeautifulSoup(page.text, 'html.parser')
    return soup


def scraping(cutoff):
    break_flag = False
    all_data = []
    for page_number in range(1,99):
        if break_flag:
            break
        time.sleep(random.uniform(2, 5))
        page_content = parse('https://www.bankier.pl/rynki/wiadomosci', page_number)
        articles_list_tag = page_content.find('ul', class_='m-listing-article-list')
        articles_tag = articles_list_tag.select('li[class="m-listing-article-list__item"]')

        for article in articles_tag:
            anchor = article.find('a')
            if anchor:
                post_date = anchor.find('div', class_='m-listing-article-list__date-time').text
                post_date_formatted = datetime.strptime(post_date, "%Y-%m-%d %H:%M")
                if post_date_formatted < cutoff:
                    break_flag = True
                    break
                else:
                    time.sleep(random.uniform(1, 4))
                    all_data.append(gather_content(anchor))
            else:
                print(article)
                print('No anchor for this news (Bankier)')

    df_final = pd.DataFrame(all_data)
    df_final['company_name'] = None
    df_final['ticker'] = None

    return df_final


def gather_content(anchor):
    url = anchor['data-vr-contentbox-url']
    page_content = parse(url)
    main_content_tag = page_content.find('article', class_='o-article')
    print(url)

    title = main_content_tag.find('h1', class_='a-heading').text
    date = main_content_tag.find('span', class_='a-span').text

    article_text_content_tag = main_content_tag.find('section', class_='o-article-content')
    text_paragraphs = article_text_content_tag.find_all('p')

    content = ''
    for pararagraph in text_paragraphs:
        content += pararagraph.text

    final_data = {'date': date, 'link': url, 'title': title, 'content': content}

    return final_data







def main():
    target_day = datetime.today()
    cutoff = target_day - timedelta(days=5)

    df = scraping(cutoff)
    df["company_name"] = df["company_name"].astype("string")
    df["ticker"] = df["ticker"].astype("string")

    for idx, row in df.iterrows():
        content_sample = row["content"]
        company_name = utils.get_company_name_from_content(content_sample)
        ticker = utils.map_company_to_ticker(company_name)

        title_sample = row["title"]
        rate = utils.get_rate(title_sample, content_sample, company_name)

        df.at[idx, "company_name"] = company_name

        df.at[idx, "ticker"] = ticker

        df.at[idx, "rate"] = rate

    df = utils.get_stock_price_for_companies(df)
    print(df)

if __name__ == "__main__":
    main()
