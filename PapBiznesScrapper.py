import httpx
from selectolax.parser import HTMLParser
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import utils


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
}

def get_html(baseurl, page):
    resp = httpx.get(baseurl + str(page), headers=headers, follow_redirects=True)
    html = HTMLParser(resp.text)
    return html


def parse_page(html, cutoff_date):
    rows = []
    break_flag = False

    for news in html.css("div.col-lg-9  ul.newsList li div.textWrapper"):
        date_str = news.css_first(".date").text()
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')

        if date < cutoff_date:
            break_flag = True
            break

        aTags = news.css("a")
        href = aTags[1].attrs["href"]
        newUrl = f"https://biznes.pap.pl{href}"

        newResp = httpx.get(newUrl, headers=headers)
        html = HTMLParser(newResp.text)
        wrapper = html.css_first("article#article")

        newsTitle = wrapper.css_first("h1.articleTitle span").text()
        newsTexts = wrapper.css("p")
        wholeText = ""

        for i in range(1, len(newsTexts)):
            wholeText += newsTexts[i].text()

        rows.append([date_str, newUrl, newsTitle, wholeText])

    return rows, break_flag

def company_profiles_scraping(cutoff):
    all_results = []

    for company, data in utils.companies.items():
        code = data[0]
        ticker = data[1]
        baseurl = f"https://biznes.pap.pl/wiadomosci/firma/{code}?page="

        for page in range(99):
            html = get_html(baseurl, page)
            rows, stop = parse_page(html, cutoff)

            for r in rows:
                r.extend([company, ticker])

            all_results.extend(rows)

            if stop:
                break

    columns = ["date", "link", "title", "content", "company_name", "ticker"]
    df_news = pd.DataFrame(all_results, columns=columns)
    df_news["category"] = "profiles"
    df_news["rate"] = "Nan"
    return df_news

def category_scraping(cutoff):
    all_dfs = []
    categories = {
        "market": "https://biznes.pap.pl/kategoria/rynki?page=",
        "economy": "https://biznes.pap.pl/kategoria/gospodarka?page="
    }
    for category, url in categories.items():
        all_results = []
        for page in range(99):
            html = get_html(url, page)
            rows, stop = parse_page(html, cutoff)
            all_results.extend(rows)
            if stop:
                break

        columns = ["date", "link", "title", "content"]
        df_news = pd.DataFrame(all_results, columns=columns)

        df_news["company_name"] = "Nan"
        df_news["ticker"] = "Nan"
        df_news["rate"] = "Nan"

        df_news["category"] = category
        all_dfs.append(df_news)

    final_df = pd.concat(all_dfs, ignore_index=True)
    return final_df


def main():
    target_day = datetime.today()
    cutoff = target_day - timedelta(days=5)


    df_profiles = company_profiles_scraping(cutoff)
    df_categories = category_scraping(cutoff)


    for idx, row in df_profiles.iterrows():
        title_sample = row["title"]
        content_sample = row["content"]
        company_name = row["company_name"]

        rate = utils.get_rate(title_sample, content_sample, company_name)
        if rate.lower() == "nan":
            df_profiles.at[idx, "rate"] = "Nan"
        else:
            df_profiles.at[idx, "rate"] = rate


    for idx, row in df_categories.iterrows():
        content_sample = row["content"]
        title_sample = row["title"]

        company_name = utils.get_company_name_from_content(content_sample)
        ticker = utils.map_company_to_ticker(company_name)
        rate = utils.get_rate(title_sample, content_sample, company_name)

        df_categories.at[idx, "company_name"] = company_name
        df_categories.at[idx, "ticker"] = ticker

        if rate.lower() == "nan" or rate is None:
            df_categories.at[idx, "rate"] = "Nan"
        else:
            df_categories.at[idx, "rate"] = rate

    df_profiles = utils.get_stock_price_for_companies(df_profiles)
    df_categories = utils.get_stock_price_for_companies(df_categories)

    valid_dfs = [df for df in [df_profiles, df_categories] if not df.empty]

    if valid_dfs:
        df_combined = pd.concat(valid_dfs, ignore_index=True)
    else:
        df_combined = pd.DataFrame()


    return df_combined

if __name__ == "__main__":
    main()
