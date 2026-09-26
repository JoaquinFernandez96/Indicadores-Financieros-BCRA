import asyncio
import logging
import os
from dataclasses import dataclass, field

import pandas as pd

from database_manager import DatabaseManager
from scrapers.api_client import (
    get_entities,
    extract_indicators,
    extract_eecc,
    extract_debtors,
)
from scrapers.async_fetcher import AsyncFetcher

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)

# Set to N > 0 to limit the run to the first N entities (useful for testing)
TEST_MODE_LIMIT: int = 0

_BATCH_SIZE = 20  # flush to DB every N entities


@dataclass
class EntityResult:
    codigo: str
    nombre: str
    logo_url: str | None
    recs_ind: list[dict] = field(default_factory=list)
    recs_eecc: list[dict] = field(default_factory=list)
    recs_deud: list[dict] = field(default_factory=list)


async def _scrape_one(fetcher: AsyncFetcher, entity: dict, idx: int, total: int) -> EntityResult:
    bco: str = entity["codigo"]
    nombre: str = entity["nombre"]
    logger.info("  [%02d/%02d] %s", idx, total, nombre)

    # All 3 API endpoints fire concurrently for this entity
    recs_ind_task = extract_indicators(fetcher, bco, nombre)
    recs_eecc_task = extract_eecc(fetcher, bco, nombre)
    recs_deud_task = extract_debtors(fetcher, bco, nombre)

    (recs_ind, logo_ind), (recs_eecc, logo_eecc), (recs_deud, logo_deud) = await asyncio.gather(
        recs_ind_task, recs_eecc_task, recs_deud_task
    )

    logo = logo_ind or logo_eecc or logo_deud

    logger.info(
        "         ( %d ind | %d eecc | %d deud )",
        len(recs_ind), len(recs_eecc), len(recs_deud),
    )
    return EntityResult(
        codigo=bco,
        nombre=nombre,
        logo_url=logo,
        recs_ind=recs_ind,
        recs_eecc=recs_eecc,
        recs_deud=recs_deud,
    )


def _flush_batch(db: DatabaseManager, results: list[EntityResult]) -> None:
    """Write a batch of EntityResults to the database synchronously."""
    all_ind: list[dict] = []
    all_eecc: list[dict] = []
    all_deud: list[dict] = []
    entity_rows: list[dict] = []

    for r in results:
        all_ind.extend(r.recs_ind)
        all_eecc.extend(r.recs_eecc)
        all_deud.extend(r.recs_deud)
        entity_rows.append({
            "codigo_entidad": int(r.codigo),
            "nombre": r.nombre,
            "logo_url": r.logo_url,
        })

    if all_ind:
        df = pd.DataFrame(all_ind)
        df["fuente"] = "indicadores"
        db.save_observations(df)

    if all_eecc:
        df = pd.DataFrame(all_eecc)
        df["fuente"] = "eecc"
        db.save_observations(df)

    if all_deud:
        df = pd.DataFrame(all_deud)
        df["fuente"] = "deudores"
        db.save_observations(df)

    if entity_rows:
        db.save_entities(pd.DataFrame(entity_rows))


def _chunked(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


async def _main_async() -> None:
    db = DatabaseManager()
    print("\n" + "=" * 60)
    print("  SCRAPER BCRA — ASYNC (httpx + asyncio)")
    print("=" * 60)

    entities = get_entities()
    if not entities:
        logger.error("  [!] No se pudieron cargar las entidades. Abortando.")
        return

    logger.info("  [1/3] Entidades encontradas: %d", len(entities))

    target_entities = entities[:TEST_MODE_LIMIT] if TEST_MODE_LIMIT else entities
    total = len(target_entities)

    async with AsyncFetcher() as fetcher:
        # Sistema Total (codigo AAA00 → stored as codigo_entidad=0)
        logger.info("  [1.5/3] Scrapeando indicadores del Sistema Total (AAA00)...")
        recs_sistema, _ = await extract_indicators(fetcher, "AAA00", "Sistema Total")
        if recs_sistema:
            df_sistema = pd.DataFrame(recs_sistema)
            df_sistema["fuente"] = "indicadores_sistema"
            db.save_observations(df_sistema)
            logger.info("         ( %d indicadores del sistema guardados )", len(recs_sistema))
        else:
            logger.warning("         [!] No se pudieron obtener indicadores del Sistema Total.")

        limit_str = f"LIMITADO A {TEST_MODE_LIMIT}" if TEST_MODE_LIMIT else "TODAS"
        logger.info("  [2/3] Extrayendo datos (%s)...", limit_str)

        # Ensure logos dir exists
        if not os.path.exists("logos"):
            os.makedirs("logos")

        # Process entities in batches; within each batch all entities run concurrently
        for batch in _chunked(list(enumerate(target_entities, 1)), _BATCH_SIZE):
            tasks = [
                _scrape_one(fetcher, entity, idx, total)
                for idx, entity in batch
            ]
            results_raw = await asyncio.gather(*tasks, return_exceptions=True)

            successful: list[EntityResult] = []
            for (idx, entity), result in zip(batch, results_raw):
                if isinstance(result, Exception):
                    logger.error(
                        "  [!] Error en entidad %s (%s): %s",
                        entity["codigo"], entity["nombre"], result,
                    )
                else:
                    successful.append(result)

            if successful:
                _flush_batch(db, successful)

    print("\n  [3/3] Proceso completado en base de datos.")
    print("\n" + "=" * 60)
    print("  PROCESO DE SCRAPPING FINALIZADO")
    print("=" * 60)


def main() -> None:
    """Sync entry point — keeps the public interface unchanged for main.py."""
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
