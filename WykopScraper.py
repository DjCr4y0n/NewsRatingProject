import requests
import pandas as pd
from datetime import datetime, timedelta
import utils



def get_data(cutoff):
    url = 'https://wykop.pl/api/v3/auth'
    data = {
        'key': 'w5e64b789f84f9dade6f1cc93cae52fb80',
        'secret': '9a5dd04db73ab3315461138e5c51342c'
    }

    response = requests.post(
        url,
        json={'data': data},
        headers={'Accept': 'application/json', 'Content-Type': 'application/json'}
    )
    jwt = response.json()['data']['token']

    tagi = {
        'finanse': 'https://wykop.pl/api/v3/tags/finanse/stream?page=1&limit=40&sort=best&type=all&multimedia=false',
        'technologia': 'https://wykop.pl/api/v3/tags/technologia/stream?page=1&limit=40&sort=best&type=all&multimedia=false',
        'inwestycje': 'https://wykop.pl/api/v3/tags/inwestycje/stream?page=1&limit=40&sort=best&type=all&multimedia=false',
        'gospodarka': 'https://wykop.pl/api/v3/tags/gospodarka/stream?page=1&limit=40&sort=best&type=all&multimedia=false'
    }

    tag_dataframes = {}

    for tag, url in tagi.items():
        response = requests.get(
            url,
            json={'data': data},
            headers={'Accept': 'application/json', 'Authorization': f'Bearer {jwt}'}
        )
        data = response.json()
        df = pd.json_normalize(data['data'])
        df_filtered = df[["created_at", "title", "description", "source.url"]].copy()

        df_filtered["created_at"] = pd.to_datetime(df_filtered["created_at"])
        df_filtered = df_filtered[df_filtered["created_at"] >= cutoff]

        df_filtered = df_filtered.rename(columns={
            "created_at": "date",
            "description": "content",
            "source.url": "link"
        })

        df_filtered["company_name"] = "Nan"
        df_filtered["ticker"] = "Nan"
        df_filtered["category"] = tag

        layout = ['date', 'link', 'title', 'content', 'company_name', 'ticker', 'category']
        df_filtered = df_filtered[layout]

        tag_dataframes[tag] = df_filtered

    return tag_dataframes


def main():
    target_day = datetime.today()
    cutoff = target_day - timedelta(days=5)

    new_dfs = []

    for df in get_data(cutoff).values():
        company_names = []
        tickers = []
        for _, row in df.iterrows():
            content = row["content"]
            company = utils.get_company_name_from_content(content)
            ticker = utils.map_company_to_ticker(company)
            company_names.append(company)
            tickers.append(ticker)

        df["company_name"] = company_names
        df["ticker"] = tickers


        rates = []
        for _, row in df.iterrows():
            title = row["title"]
            content = row["content"]
            company = row["company_name"]
            rate = utils.get_rate(title, content, company)
            rates.append(rate)

        df["rate"] = rates

        df = utils.get_stock_price_for_companies(df)
        new_dfs.append(df)

    df_combined = pd.concat([df for df in new_dfs if not df.empty], ignore_index=True)

    return df_combined


if __name__ == "__main__":
    main()
