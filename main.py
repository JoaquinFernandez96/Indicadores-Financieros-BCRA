import time
import os
import scraper
import data_processing

def main():
    print("=" * 60)
    print("      PIPELINE DE EXTRACCIÓN Y ANÁLISIS BCRA")
    print("=" * 60)
    start_time = time.time()
    
    # ---------------------------------------------------------
    # 1. Scraping (Extracción Web)
    # ---------------------------------------------------------
    print("\n>>> FASE 1: Scraping de Datos del Banco Central")
    print("---------------------------------------------------------")
    try:
        scraper.main()
        if not os.path.exists("bcra_dashboard.db"):
            raise FileNotFoundError("El scraper no logró generar bcra_dashboard.db")
    except Exception as e:
        print(f"\n[ERROR CRÍTICO] Fase 1 falló: {e}")
        return

    # ---------------------------------------------------------
    # 2. Análisis y Cruce de Datos
    # ---------------------------------------------------------
    print("\n>>> FASE 2: Cruce de Datos y Generación de Benchmarks")
    print("---------------------------------------------------------")
    try:
        data_processing.main()
    except Exception as e:
        print(f"\n[ERROR CRÍTICO] Fase 2 falló: {e}")
        return

    elapsed_time = round((time.time() - start_time) / 60, 2)
    
    # ---------------------------------------------------------
    # 3. Resumen de Salidas (Outputs)
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    print("          EJECUCIÓN FINALIZADA CON ÉXITO")
    print("=" * 60)
    print(f"Tiempo total de ejecución: {elapsed_time} minutos.\n")
    print("Archivos (Outputs) generados:")
    print("  1. [bcra_dashboard.db]          -> Base de datos unificada con observaciones,")
    print("                                      entidades y benchmarks.")
    print("  2. [bcra_backup_incremental.csv] -> Respaldo incremental de la última")
    print("                                      ejecución del scraper (histórico).")
    print("=" * 60)

if __name__ == "__main__":
    main()
