import etl.models
from pathlib    import Path
from etl.session import init_db, Session
from etl.ingest  import ingest_csv
from data_fetch.ratings_dataset import build_ratings_dataset
from data_fetch.build_yields_dataset import build_yields_dataset
from etl.transform import build_final

SCRAPE_START = "2020-01-01"

def main():
    init_db()
    session = Session()
    try:
        data_dir   = Path(__file__).parent.parent / "data"
        ratings_csv = data_dir / "ratings_daily.csv"
        yields_csv  = data_dir / "yields.csv"

        if ratings_csv.exists():
            print(f"-- loading existing CSV → ratings_daily")
            ingest_csv("ratings_daily", ratings_csv, session)
        else:
            print(f"-- scraping ratings from {SCRAPE_START}…")
            build_ratings_dataset(SCRAPE_START, session)

        if yields_csv.exists():
            print(f"-- loading existing CSV → yields")
            ingest_csv("yields", yields_csv, session)
        else:
            print(f"-- scraping yields from {SCRAPE_START}…")
            build_yields_dataset(SCRAPE_START, session)

        print("-- building final DataFrame")
        df_final = build_final(session)

        out_csv = data_dir / "final_dataset.csv"
        df_final.to_csv(out_csv, index=False)
        print(f"✅ ETL completed: {len(df_final)} rows → {out_csv}")

    finally:
        session.close()

if __name__ == "__main__":
    main()
