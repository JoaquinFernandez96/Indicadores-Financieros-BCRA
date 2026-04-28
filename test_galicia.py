"""One-shot scrape of Banco Galicia (00007) — writes 3 CSVs, no DB."""
import asyncio
import pandas as pd

from scrapers.async_fetcher import AsyncFetcher
from scrapers.api_client import extract_indicators
from scrapers.html_parser import scrape_balances_table, scrape_debtors_table

BCO = "00007"
NOMBRE = "Banco Galicia"
EECC_URL = f"https://www.bcra.gob.ar/entidades-financieras-estados-contables/?bco={BCO}"
DEUD_URL = f"https://www.bcra.gob.ar/entidades-financieras-situacion-deudores/?bco={BCO}"


async def main():
    async with AsyncFetcher(max_concurrent=3, timeout=30.0) as fetcher:
        (recs_ind, logo), html_eecc, html_deud = await asyncio.gather(
            extract_indicators(fetcher, BCO, NOMBRE),
            fetcher.get_text(EECC_URL),
            fetcher.get_text(DEUD_URL),
        )

    recs_eecc = scrape_balances_table(html_eecc, BCO, NOMBRE, "Balances") if html_eecc else []
    recs_deud = scrape_debtors_table(html_deud, BCO, NOMBRE, "Deudores") if html_deud else []

    pd.DataFrame(recs_ind).to_csv("galicia_indicadores.csv", index=False)
    pd.DataFrame(recs_eecc).to_csv("galicia_balances.csv", index=False)
    pd.DataFrame(recs_deud).to_csv("galicia_deudores.csv", index=False)

    print(f"indicadores : {len(recs_ind)} records")
    print(f"balances    : {len(recs_eecc)} records")
    print(f"deudores    : {len(recs_deud)} records")
    print(f"logo_url    : {logo}")


asyncio.run(main())
