"""
PRÁCTICA 5: PIPELINE ETL AUTOMATIZADO CON SALIDA PARQUET Y CSV A AWS S3
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import boto3
import os

def pipeline_etl_completo():
    print("=== 1. FASE DE EXTRACCIÓN (Extract - Raw Data Staging) ===")
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

    print("=== 3. FASE DE CARGA (Load - Exportación a AWS S3 y Local) ===")
    
    BUCKET_NAME = 'aws-academy-datacenter-bucket-23-02-001'
    
    # Aquí es donde se realiza la exportación a Parquet y CSV de forma local
    parquet_path = "etl_output.parquet"
    df_raw.to_parquet(parquet_path, index=False, engine='pyarrow')
    size_parquet = os.path.getsize(parquet_path)
    print(f"[OK] Archivo Parquet generado localmente: '{parquet_path}' ({size_parquet} bytes)")
    
    csv_path = "etl_output.csv"
    df_raw.to_csv(csv_path, index=False)
    size_csv = os.path.getsize(csv_path)
    print(f"[OK] Archivo CSV generado localmente:     '{csv_path}' ({size_csv} bytes)")
    
    # Aquí se encuentran las credenciales necesarias para la conexión XD.
    AWS_ACCESS_KEY_ID = "INGRESA TU LLAVE DE ACCESO DE AWS SAMURAI"
    AWS_SECRET_ACCESS_KEY = "INGRESA TU LLAVE SECRETA DE ACCESO DE AWS SAMURAI"
    AWS_SESSION_TOKEN = "INGRESA TU TOKEN DE SESIÓN DE AWS SAMURAI"

    print(f"\n--> Conectando a AWS S3 y cargando al bucket: '{BUCKET_NAME}'...")
    try:
        s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN
        )
        
    # Aquí es donde se realiza la exportación a Parquet en el buckets.
        s3_client.upload_file(parquet_path, BUCKET_NAME, f"processed/{parquet_path}")
        print(f"[ÉXITO] Archivo Parquet subido a S3: s3://{BUCKET_NAME}/processed/{parquet_path}")
        
    # Aquí es donde se realiza la exportación a CSV en el buckets.
        s3_client.upload_file(csv_path, BUCKET_NAME, f"processed/{csv_path}")
        print(f"[ÉXITO] Archivo CSV subido a S3:     s3://{BUCKET_NAME}/processed/{csv_path}")
    except Exception as e:
        print(f"[ERROR S3] Falló la subida a S3: {e}")

    print("\n=== COMPARATIVA DE ALMACENAMIENTO ===")
    print(f"• Tamaño Parquet: {size_parquet} Bytes")
    print(f"• Tamaño CSV:     {size_csv} Bytes")

if __name__ == "__main__":
    pipeline_etl_completo()
