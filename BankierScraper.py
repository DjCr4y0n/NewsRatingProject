import httpx
from selectolax.parser import HTMLParser
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

    for news in html.css("section#articleList div.article"):
        date_str = news.css_first(".entry-date").text()
        date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')

        if date < cutoff_date:
            break_flag = True
            break

        aTags = news.css("a")
        href = aTags[0].attrs["href"]
        newUrl = f"https://www.bankier.pl{href}"

        newResp = httpx.get(newUrl, headers=headers)
        html = HTMLParser(newResp.text)
        wrapper = html.css_first("article#article")

        newsTitle = wrapper.css_first("h1.a-heading").text()
        newsTexts = wrapper.css("p")
        wholeText = ""

        for i in range(1, len(newsTexts)):
            wholeText += newsTexts[i].text()

        rows.append([date_str, newUrl, newsTitle, wholeText])

    return rows, break_flag

def news_scraping(cutoff):
    url = "https://www.bankier.pl/rynki/wiadomosci/"
    all_rows = []

    for page in range(1, 99):
        html = get_html(url, page)
        rows, stop = parse_page(html, cutoff)
        all_rows.extend(rows)
        if stop:
            break

    df_news = pd.DataFrame(all_rows, columns=["date", "link", "title", "content"])
    df_news["company_name"] = "Nan"
    df_news["ticker"] = "Nan"
    df_news["rate"] = "Nan"
    df_news["category"] = "rynki"
    return df_news



def main():
    target_day = datetime.today()
    cutoff = target_day - timedelta(days=5)

    df = news_scraping(cutoff)
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
    return df

if __name__ == "__main__":
    main()
