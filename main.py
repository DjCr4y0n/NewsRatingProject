import PapBiznesScrapper as Pap
import BankierScraper as Bankier
import pandas as pd
import WykopScraper as Wykop


def main():
    excel = 'news_output.xlsx'
    df_pap = Pap.main()
    df_bankier = Bankier.main()
    df_wykop = Wykop.main()

    df_previous = pd.read_excel(excel)

    df_combined = pd.concat([df_pap, df_bankier, df_wykop], ignore_index=True)

    df_updated = pd.concat([df_previous, df_combined], ignore_index=True)
    
    df_updated.to_excel(excel, index=False, sheet_name="Combined")




if __name__ == "__main__":
    main()