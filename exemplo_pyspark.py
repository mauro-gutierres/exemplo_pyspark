'''
Programa:  exemplo_pyspark
Autor:     Mauro Gutierres
Descrição: pipeline que faz o ETL de arquivos csv gerando um arquivo parquet
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

files_path='/home/mauro/Documents/exemplo_pyspark/data'

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()

'''
Extrai os dados dos arquivos csv
e transforma em dataframes.
'''
estabelecimentos = spark.read.csv(files_path+'/estabelecimentos',sep=';',inferSchema=True)
empresas = spark.read.csv(files_path+'/empresas',sep=';',inferSchema=True)
socios = spark.read.csv(files_path+'/socios',sep=';',inferSchema=True)


'''
Transformation - altera o nome das colunas dos dataframes e o tipo de
dados de duas colunas.
'''
#altera o nome das colunas
estabsColNames = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_da_cidade_no_exterior', 'pais', 'data_de_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_de_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_do_fax', 'fax', 'correio_eletronico','situacao_especial', 'data_da_situacao_especial']
empresasColNames = ['cnpj_basico', 'razao_social_nome_empresarial', 'natureza_juridica', 'qualificacao_do_responsavel','capital_social_da_empresa', 'porte_da_empresa', 'ente_federativo_responsavel']
sociosColNames = ['cnpj_basico', 'identificador_de_socio', 'nome_do_socio_ou_razao_social', 'cnpj_ou_cpf_do_socio', 'qualificacao_do_socio', 'data_de_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_do_representante_legal', 'faixa_etaria']

for index,colName in enumerate(estabsColNames):
    estabelecimentos = estabelecimentos.withColumnRenamed(f"_c{index}",colName)
for index,colName in enumerate(empresasColNames):
    empresas = empresas.withColumnRenamed(f"_c{index}",colName)
for index,colName in enumerate(sociosColNames):
    socios = socios.withColumnRenamed(f"_c{index}",colName)

# altera o separador de casas decimais de virgula para ponto
empresas = empresas.withColumn('capital_social_da_empresa',f.regexp_replace('capital_social_da_empresa',',','.'))

# altera o datatype da coluna para double
empresas = empresas.withColumn('capital_social_da_empresa',empresas['capital_social_da_empresa'].cast(DoubleType()))

# altera os campos data de string para date
#estabelecimentos = estabelecimentos.withColumn("data_situacao_cadastral",f.to_date(estabelecimentos.data_situacao_cadastral.cast(StringType()),'yyyyMMdd')).withColumn("data_de_inicio_atividade",f.to_date(estabelecimentos.data_de_inicio_atividade.cast(StringType()),'yyyyMMdd')).withColumn("data_da_situacao_especial",f.to_date(estabelecimentos.data_da_situacao_especial.cast(StringType()),'yyyyMMdd'))
#socios = socios.withColumn("data_de_entrada_sociedade",f.to_date(socios.data_de_entrada_sociedade.cast(StringType()),'yyyyMMdd'))

'''
Load - Persiste os dataframes em tabelas no postgres e gera arquivo parquet
'''
# grava tabelas no postgres cadempresa
estabelecimentos.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/cadempresa").option("dbtable","estabelecimento").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").save()
empresas.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/cadempresa").option("dbtable","empresa").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").save()
socios.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/cadempresa").option("dbtable","socio").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").save()

# gera arquivo parquet, a partir de join das tabelas
join_df = estabelecimentos.alias('est').join(empresas.alias('emp'),'cnpj_basico', how="inner").join(socios.alias('soc'), 'cnpj_basico', how='inner').select('est.cnpj_basico', 'est.cnpj_ordem','est.nome_fantasia', 'est.identificador_matriz_filial', 'est.data_de_inicio_atividade', 'est.municipio', 'emp.razao_social_nome_empresarial', 'emp.capital_social_da_empresa', 'emp.porte_da_empresa', 'soc.identificador_de_socio', 'soc.nome_do_socio_ou_razao_social', 'soc.qualificacao_do_socio')
join_df.write.format("parquet").save(files_path+"/empresas_out/empresas", mode='overwrite')

spark.stop()




   
