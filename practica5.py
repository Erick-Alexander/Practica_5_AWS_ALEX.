"""
PRÁCTICA 5: PIPELINE ETL AUTOMATIZADO CON SALIDA PARQUET Y CSV
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import os

def pipeline_etl_completo():
    print("=== 1. FASE DE EXTRACCIÓN (Extract - Raw Data Staging)===")
    raw_data = {
        'id_cliente': [101, 102, 103, 104, 105, 106, 107, 108],
        'cliente': ['Ana Gómez', 'Carlos Ruiz', 'Beatriz Peña', 'David Soto', 'Elena Lara', 'Fernando Mas', 'Gloria Ríos', 'Hugo Vega'],
        'edad': [20, 40, 60, 30, 22, 55, 35, 68],
        'monto_compras': [1200.0, 50000.0, 30000.0, 15000.0, 3500.0, 82000.0, 27000.0, 45000.0]
    }
    df_raw = pd.DataFrame(raw_data)
    print(f"[OK] Total de registros extraídos de la zona Raw: {len(df_raw)}")
    print("\nMuestra inicial de datos extraídos:")
    print(df_raw.head(4))
    print("\n")

    print("=== 2. FASE DE TRANSFORMACIÓN (Transform - Feature Engineering) ===")
    
    # Aquí es donde comienza la fase de transformación, aquella en donde salen los datos del 101 al 107
    print("--> Aplicando Discretización (Binning) en columna 'edad' con pd.cut()...")
    df_raw['categoria_edad'] = pd.cut(
        df_raw['edad'], 
        bins=[0, 25, 50, 100],
        labels=['Joven', 'Adulto', 'Senior']
    )
    print("    Categorías generadas: ['Joven' (0-25], 'Adulto' (25-50], 'Senior' (50-100]]")
    
    # Aquí es donde se establecen los datos minimizados y maximizados para la columna de monto de compras
    print("--> Aplicando Normalización MinMaxScaler sobre 'monto_compras'...")
    scaler = MinMaxScaler()
    df_raw['monto_normalizado'] = scaler.fit_transform(df_raw[['monto_compras']])
    print(f"    Monto Mínimo (${df_raw['monto_compras'].min():,.2f}) -> Escala: {df_raw['monto_normalizado'].min():.6f}")
    print(f"    Monto Máximo (${df_raw['monto_compras'].max():,.2f}) -> Escala: {df_raw['monto_normalizado'].max():.6f}")
    
    # Aquí es donde se deriva la nueva métrica de negocio a partir de la columna de monto normalizado XD.
    print("--> Derivando nueva métrica de negocio 'score_promocional'...")
    df_raw['score_promocional'] = (df_raw['monto_normalizado'] * 100).round(2)
    
    print("\n>>> DATASET TRANSFORMADO Y PROCESADO <<<")
    print(df_raw.to_string(index=False))
    print("\n")

    print("=== 3. FASE DE CARGA (Load - Generación de Archivos Target) ===")
    
    # Aquí es donde se realiza la exportación a Parquet y CSV en el buckets
    parquet_path = "etl_output.parquet"
    df_raw.to_parquet(parquet_path, index=False, engine='pyarrow')
    size_parquet = os.path.getsize(parquet_path)
    print(f"[OK] Archivo Parquet generado: '{parquet_path}' ({size_parquet} bytes)")
    
    # Aquí es donde se completa la exportación a CSV y se obtiene el tamaño del archivo ya generado.
    csv_path = "etl_output.csv"
    df_raw.to_csv(csv_path, index=False)
    size_csv = os.path.getsize(csv_path)
    print(f"[OK] Archivo CSV generado:     '{csv_path}' ({size_csv} bytes)")
    
    print("=== COMPARATIVA DE ALMACENAMIENTO ===")
    print(f"• Tamaño Parquet: {size_parquet} Bytes")
    print(f"• Tamaño CSV:     {size_csv} Bytes")

if __name__ == "__main__":
    pipeline_etl_completo()
