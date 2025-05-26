import os
import glob
import pandas as pd


caminho_csv = "G:/DEV/data_science/dados_bolsa_familia/csv_extracao/"


csv_files = glob.glob(os.path.join(caminho_csv, "*.csv"))
for file in csv_files:
    os.remove(file)
    print(f"Arquivo CSV removido: {file}")


parquet_files = glob.glob(os.path.join(caminho_csv, "*.parquet"))
dfs = []

for file in parquet_files:
    df = pd.read_parquet(file)
    dfs.append(df)
    print(f"Arquivo Parquet lido: {file}")

df_final = pd.concat(dfs, ignore_index=True)
output_path = os.path.join(caminho_csv, "NovoBolsaFamilia_2024_final.parquet")
df_final.to_parquet(output_path, index=False)
print(f"Arquivo final salvo em: {output_path}")
