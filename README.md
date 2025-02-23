# exemplo_pyspark.py
  Autor: Mauro Gutierres

## Descrição:
   Este é um exemplo de pipeline que lê três arquivos csv, Faz a Extração dos
   dados para dataframes, Transforma executando alterações na estrutura dos
   dataframes e faz o Load gravando os dados em um banco Postgres e em arquivo
   parquet.

## Tecnologias utilizadas:
Postgres, PySpark, arquivos CSV e Parquet.

## Requisitos para testar
- Máquina com linux, Spark e PostgreSQL instalados
- Baixar os arquivos de dados do pelo link abaixo:
https://drive.google.com/drive/folders/16qOfGbagNclD987dASsuIRcrdeZ5yBYA?usp=drive_link
- Descompactar os arquivos em um diretório no servidor
- Baixar e alterar o exemplo_pyspark.py, alterando a variável files_path
  para o caminho dos arquivos de dados
- Criar um banco de dados chamado cadempresa no PostgreSQL
- alterar as informações de conexão com o PostgreSQL para os dados do seu
  banco.