import requests
import os
import zipfile
import pandas as pd

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Caminhos base
caminho_base = os.path.join("G:/DEV/data_science", "dados_bolsa_familia")
pasta_zip = os.path.join(caminho_base, "zips")
pasta_csv = os.path.join(caminho_base, "csv_extracao")

os.makedirs(pasta_zip, exist_ok=True)
os.makedirs(pasta_csv, exist_ok=True)

def baixar_zip(mes: int):
    mes_str = f"{mes:02d}"
    nome_arquivo = f"2024{mes_str}_NovoBolsaFamilia.zip"
    url = f"https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/novo-bolsa-familia/{nome_arquivo}"
    caminho_zip = os.path.join(pasta_zip, nome_arquivo)

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(caminho_zip, 'wb') as f:
            f.write(response.content)
        print(f"Arquivo baixado: {nome_arquivo}")
        return caminho_zip
    else:
        print(f"Erro ao baixar {nome_arquivo}: {response.status_code}")
        return None

def descompactar_csv_do_zip(caminho_zip):
    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
        arquivos_zip = zip_ref.namelist()
        arquivos_csv = [f for f in arquivos_zip if f.lower().endswith('.csv')]
        if not arquivos_csv:
            print("Nenhum arquivo CSV encontrado no ZIP.")
            return []
        extraidos = []
        for arquivo_csv in arquivos_csv:
            zip_ref.extract(arquivo_csv, pasta_csv)
            caminho_extraido = os.path.join(pasta_csv, arquivo_csv)
            print(f"extraído para {arquivo_csv}")
            extraidos.append(caminho_extraido)
        return extraidos

def tratar_e_salvar_parquet(caminho_csv):
    bf = pd.read_csv(caminho_csv, sep=';', encoding='latin1')
    colunas_remover = ['CPF FAVORECIDO', 'NIS FAVORECIDO', 'NOME FAVORECIDO']
    bf = bf.drop(columns=[col for col in colunas_remover if col in bf.columns])
    bf.columns = [
        'mes_competencia',
        'mes_referencia',
        'uf',
        'codigo_municipio',
        'nome_municipio',
        'valor_parcela'
    ]
    bf['mes_competencia'] = pd.to_datetime(bf['mes_competencia'].astype(str) + '01', format='%Y%m%d')
    bf['mes_referencia'] = pd.to_datetime(bf['mes_referencia'].astype(str) + '01', format='%Y%m%d')
    bf['valor_parcela'] = bf['valor_parcela'].str.replace(',', '.').astype(float)

    nome_base = os.path.splitext(os.path.basename(caminho_csv))[0]
    caminho_parquet = os.path.join(pasta_csv, f"{nome_base}.parquet")
    bf.to_parquet(caminho_parquet, index=False)

    print(f"salvo em: {caminho_parquet}")
    return caminho_parquet

# Loop pelos 12 meses
for mes in range(1, 13):
    zip_path = baixar_zip(mes)
    if zip_path:
        csv_paths = descompactar_csv_do_zip(zip_path)
        for csv in csv_paths:
            tratar_e_salvar_parquet(csv)
