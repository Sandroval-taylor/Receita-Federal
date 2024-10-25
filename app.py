from flask import Flask, render_template, request, jsonify, send_file
from flasgger import Swagger
import psycopg2
import pandas as pd
import io
import xlsxwriter
import json

app = Flask(__name__)
swagger = Swagger(app)
# Configurações do banco de dados
DB_HOST = "postgresql-171633-0.cloudclusters.net"
DB_PORT = "18857"
DB_NAME = "Banco_Receita"
DB_USER = "Sandro"
DB_PASSWORD = "sandro01"
# Variáveis globais para armazenar resultados das pesquisas
socios_Cnpj_Raiz, socios_pais, socios_repre_legal, socios_nome_repre, socios_quali_repre, socios_socios = [], [], [], [], [], []
empresas_Cnpj_Raiz, empresas_nome, empresas_capital_social, empresas_ente_federativo, empresas_quali_responsavel, empresas_natureza_juridica, empresas_porte = [], [], [], [], [], [], []
estabelecimentos_Cnpj_Raiz, estabelecimentos_identificador_matriz_filial, estabelecimentos_nome_fantasia = [], [], []
estabelecimentos_situacao_cadastral, estabelecimentos_data_situacao_cadastral, estabelecimentos_motivo_situacao_cadastral = [], [], []
estabelecimentos_cidade_exterior, estabelecimentos_pais, estabelecimentos_data_de_inicio_de_atividade, estabelecimentos_cnae_principal = [], [], [], []
estabelecimentos_cnae_secundario, estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial = [], [], []
estabelecimentos_data_situacao_especial, estabelecimentos_cnpj, estabelecimentos_endereco, estabelecimentos_telefones = [], [], [], []
pesquisa_por_cnpj = False  # Variável global de controle
def connect_to_db():
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        print("Conexão com o banco de dados bem-sucedida")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
def tratar_cnpj(cnpj_usuario):
    cnpj_tratado = cnpj_usuario.replace(".", "").replace("/", "").replace("-", "")
    cnpj_raiz = cnpj_tratado[:8]
    return cnpj_tratado, cnpj_raiz
def tratar_cpf(cpf_usuario):
    cpf_limpo = cpf_usuario.replace(".", "").replace("-", "")
    return f"***{cpf_limpo[3:9]}**"
def tratar_coringa(coringa_usuario):
    return coringa_usuario.upper()
def formatar_cnpj(cnpj):
    cnpj = str(cnpj).zfill(14)
    return f'{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}'
def limpar_variaveis_globais():
    global socios_Cnpj_Raiz, socios_socios, socios_repre_legal, socios_nome_repre, socios_quali_repre, socios_pais
    global empresas_Cnpj_Raiz, empresas_nome, empresas_capital_social, empresas_ente_federativo, empresas_quali_responsavel, empresas_natureza_juridica, empresas_porte
    global estabelecimentos_Cnpj_Raiz, estabelecimentos_identificador_matriz_filial, estabelecimentos_nome_fantasia, estabelecimentos_situacao_cadastral, estabelecimentos_data_situacao_cadastral
    global estabelecimentos_motivo_situacao_cadastral, estabelecimentos_cidade_exterior, estabelecimentos_pais, estabelecimentos_data_de_inicio_de_atividade, estabelecimentos_cnae_principal
    global estabelecimentos_cnae_secundario, estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial, estabelecimentos_data_situacao_especial
    global estabelecimentos_cnpj, estabelecimentos_endereco, estabelecimentos_telefones
    socios_Cnpj_Raiz, socios_pais, socios_repre_legal, socios_nome_repre, socios_quali_repre, socios_socios = [], [], [], [], [], []
    empresas_Cnpj_Raiz, empresas_nome, empresas_capital_social, empresas_ente_federativo, empresas_quali_responsavel, empresas_natureza_juridica, empresas_porte = [], [], [], [], [], [], []
    estabelecimentos_Cnpj_Raiz, estabelecimentos_identificador_matriz_filial, estabelecimentos_nome_fantasia = [], [], []
    estabelecimentos_situacao_cadastral, estabelecimentos_data_situacao_cadastral, estabelecimentos_motivo_situacao_cadastral = [], [], []
    estabelecimentos_cidade_exterior, estabelecimentos_pais, estabelecimentos_data_de_inicio_de_atividade, estabelecimentos_cnae_principal = [], [], [], []
    estabelecimentos_cnae_secundario, estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial = [], [], []
    estabelecimentos_data_situacao_especial, estabelecimentos_cnpj, estabelecimentos_endereco, estabelecimentos_telefones = [], [], [], []
## Pesquisas Preliminares
def pesquisar_cnpj(cursor, cnpj): # Pesquisa por CNPJ
    global pesquisa_por_cnpj
    pesquisa_por_cnpj = True
    limpar_variaveis_globais()
    cnpj_tratado, cnpj_raiz = tratar_cnpj(cnpj)
    global estabelecimentos_cnpj, cnpj_r
    try:
        for i in range(10):
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"SELECT \"Cnpj Raiz\" FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj\" = '{cnpj_tratado}'"
            cursor.execute(query_estabelecimentos)
            resultados = cursor.fetchall()
            if resultados:
                for resultado in resultados:
                    cnpj_r = resultado[0]
                    pesquisar_cnpj_raiz(cursor, cnpj_r)
                break
    except Exception as e:
        print(f"Erro durante a pesquisa de CNPJ: {e}")
def pesquisar_por_nome_e_cpf(cursor, nome, cpf): # Pesquisa por Nome e CPF
    global pesquisa_por_cnpj
    pesquisa_por_cnpj = False
    limpar_variaveis_globais()
    nome_upper = nome.upper()
    cnpj_raizes = set()
    try:
        for i in range(10):
            tabela_socios = f"socios{i}"
            query = f"""
            SELECT "Cnpj Raiz" FROM \"public\".\"{tabela_socios}\" 
            WHERE UPPER(\"Socios\") LIKE '%{nome_upper}%' AND \"Socios\" LIKE '%{cpf}%'
            """
            cursor.execute(query)
            resultados = cursor.fetchall()
            if resultados:
                for resultado in resultados:
                    cnpj_raizes.add(resultado[0])
    except Exception as e:
        print(f"Erro durante a pesquisa por Nome e CPF: {e}")
    for cnpj_r in cnpj_raizes:
        pesquisar_cnpj_raiz(cursor, cnpj_r)   
    sincronizar_tamanhos_por_cnpj_raiz()# Sincronizar os tamanhos das listas de sócios e empresas com os estabelecimentos
def pesquisar_coringa(cursor, coringa):
    global pesquisa_por_cnpj
    pesquisa_por_cnpj = False  # Indica que a pesquisa atual NÃO é por CNPJ
    limpar_variaveis_globais()
    coringa_upper = tratar_coringa(coringa)
    cnpj_raizes = set()
    try:
        for i in range(10):
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"""
            SELECT "Cnpj Raiz" FROM \"public\".\"{tabela_estabelecimentos}\" 
            WHERE UPPER("Nome Fantasia") LIKE '%{coringa_upper}%'
            OR UPPER("Correio Eletronico") LIKE '%{coringa_upper}%'
            OR UPPER("Telefones") LIKE '%{coringa_upper}%'
            """
            cursor.execute(query_estabelecimentos)
            resultados = cursor.fetchall()
            if resultados:
                for resultado in resultados:
                    cnpj_raizes.add(resultado[0])
        for i in range(10):
            tabela_socios = f"socios{i}"
            query_socios = f"""
            SELECT "Cnpj Raiz" FROM \"public\".\"{tabela_socios}\"
            WHERE UPPER("Socios") LIKE '%{coringa_upper}%'
            """
            cursor.execute(query_socios)
            resultados = cursor.fetchall()
            if resultados:
                for resultado in resultados:
                    cnpj_raizes.add(resultado[0])
    except Exception as e:
        print(f"Erro durante a pesquisa coringa: {e}")   
    for cnpj_r in cnpj_raizes:
        pesquisar_cnpj_raiz(cursor, cnpj_r)
    sincronizar_tamanhos_por_cnpj_raiz()# Sincronizar os tamanhos das listas de sócios e empresas com os estabelecimentos
####### Pesquisas nas tabelas de Sócios, Empresas e Estabelecimentos com base no CNPJ Raiz ######
def pesquisar_cnpj_raiz(cursor, cnpj_r): ## Função para chamar as pesquisas nas tabelas 
    pesquisar_cnpj_raiz_socios(cursor, cnpj_r)
    pesquisar_cnpj_raiz_empresas(cursor, cnpj_r)
    pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_r)
def pesquisar_cnpj_raiz_socios(cursor, cnpj_r): ## Consulta na tabela de Sócios
    global socios_Cnpj_Raiz, socios_socios, socios_repre_legal, socios_nome_repre, socios_quali_repre, socios_pais
    try:
        for i in range(10):  # Executa a consulta na tabela de sócios
            tabela_socios = f"socios{i}"
            query_socios = f"SELECT * FROM \"public\".\"{tabela_socios}\" WHERE \"Cnpj Raiz\" = '{cnpj_r}'"
            cursor.execute(query_socios)
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:
                    # Verificar duplicação ou replicar conforme o tipo de pesquisa
                    if pesquisa_por_cnpj:  # Replicar os dados para todas as linhas se a pesquisa for por CNPJ
                        socios_Cnpj_Raiz.append(linha[0])
                        socios_pais.append(linha[1])
                        socios_repre_legal.append(linha[2])
                        socios_nome_repre.append(linha[3])
                        socios_quali_repre.append(linha[4])
                        socios_socios.append(linha[5])
                    elif linha[0] not in socios_Cnpj_Raiz:  # Adicionar apenas se não houver duplicação nas outras pesquisas
                        socios_Cnpj_Raiz.append(linha[0])
                        socios_pais.append(linha[1])
                        socios_repre_legal.append(linha[2])
                        socios_nome_repre.append(linha[3])
                        socios_quali_repre.append(linha[4])
                        socios_socios.append(linha[5])
                break  # Para após o primeiro resultado encontrado
    except Exception as e:
        print(f"Erro durante a consulta de sócios: {e}")
def pesquisar_cnpj_raiz_empresas(cursor, cnpj_r): ## Consulta na tabela de Empresas
    global empresas_Cnpj_Raiz, empresas_nome, empresas_capital_social, empresas_ente_federativo, empresas_quali_responsavel, empresas_natureza_juridica, empresas_porte
    try:
        for i in range(10):  # Executa a consulta na tabela de empresas
            tabela_empresas = f"empresas{i}"
            query_empresas = f"SELECT * FROM \"public\".\"{tabela_empresas}\" WHERE \"Cnpj Raiz\" = '{cnpj_r}'"
            cursor.execute(query_empresas)
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:
                    # Verificar duplicação ou replicar conforme o tipo de pesquisa
                    if pesquisa_por_cnpj:  # Replicar os dados para todas as linhas se a pesquisa for por CNPJ
                        empresas_Cnpj_Raiz.append(linha[0])
                        empresas_nome.append(linha[1])
                        empresas_capital_social.append(linha[2])
                        empresas_ente_federativo.append(linha[3])
                        empresas_quali_responsavel.append(linha[4])
                        empresas_natureza_juridica.append(linha[5])
                        empresas_porte.append(linha[6])
                    elif linha[0] not in empresas_Cnpj_Raiz:  # Adicionar apenas se não houver duplicação nas outras pesquisas
                        empresas_Cnpj_Raiz.append(linha[0])
                        empresas_nome.append(linha[1])
                        empresas_capital_social.append(linha[2])
                        empresas_ente_federativo.append(linha[3])
                        empresas_quali_responsavel.append(linha[4])
                        empresas_natureza_juridica.append(linha[5])
                        empresas_porte.append(linha[6])
                break  # Para após o primeiro resultado encontrado                     
    except Exception as e:
        print(f"Erro durante a consulta de empresas: {e}")
def pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_r): ## Consulta na tabela de Estabelecimentos
    global estabelecimentos_Cnpj_Raiz, estabelecimentos_identificador_matriz_filial, estabelecimentos_nome_fantasia
    global estabelecimentos_situacao_cadastral, estabelecimentos_data_situacao_cadastral, estabelecimentos_motivo_situacao_cadastral
    global estabelecimentos_cidade_exterior, estabelecimentos_pais, estabelecimentos_data_de_inicio_de_atividade, estabelecimentos_cnae_principal
    global estabelecimentos_cnae_secundario, estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial, estabelecimentos_data_situacao_especial
    global estabelecimentos_cnpj, estabelecimentos_endereco, estabelecimentos_telefones

    try:
        for i in range(10):  # Executa a consulta na tabela de estabelecimentos e ordena pelo CNPJ completo
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"""
            SELECT * FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj Raiz\" = '{cnpj_r}' ORDER BY "Cnpj" """
            cursor.execute(query_estabelecimentos)
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:
                    estabelecimentos_Cnpj_Raiz.append(linha[0])
                    estabelecimentos_identificador_matriz_filial.append(linha[1])
                    estabelecimentos_nome_fantasia.append(linha[2])
                    estabelecimentos_situacao_cadastral.append(linha[3])
                    estabelecimentos_data_situacao_cadastral.append(linha[4])
                    estabelecimentos_motivo_situacao_cadastral.append(linha[5])
                    estabelecimentos_cidade_exterior.append(linha[6])
                    estabelecimentos_pais.append(linha[7])
                    estabelecimentos_data_de_inicio_de_atividade.append(linha[8])
                    estabelecimentos_cnae_principal.append(linha[9])
                    estabelecimentos_cnae_secundario.append(linha[10])
                    estabelecimentos_correio_eletronico.append(linha[11])
                    estabelecimentos_situacao_especial.append(linha[12])
                    estabelecimentos_data_situacao_especial.append(linha[13])
                    estabelecimentos_cnpj.append(linha[14])  # Coluna CNPJ completo
                    estabelecimentos_endereco.append(linha[15])
                    estabelecimentos_telefones.append(linha[16])
                    if pesquisa_por_cnpj:  # Só replicar na pesquisa por CNPJ
                        # Replicar dados de sócios
                        socios_Cnpj_Raiz.append(socios_Cnpj_Raiz[-1] if socios_Cnpj_Raiz else None)
                        socios_pais.append(socios_pais[-1] if socios_pais else None)
                        socios_repre_legal.append(socios_repre_legal[-1] if socios_repre_legal else None)
                        socios_nome_repre.append(socios_nome_repre[-1] if socios_nome_repre else None)
                        socios_quali_repre.append(socios_quali_repre[-1] if socios_quali_repre else None)
                        socios_socios.append(socios_socios[-1] if socios_socios else None)
                        # Replicar dados de empresas
                        empresas_Cnpj_Raiz.append(empresas_Cnpj_Raiz[-1] if empresas_Cnpj_Raiz else None)
                        empresas_nome.append(empresas_nome[-1] if empresas_nome else None)
                        empresas_capital_social.append(empresas_capital_social[-1] if empresas_capital_social else None)
                        empresas_ente_federativo.append(empresas_ente_federativo[-1] if empresas_ente_federativo else None)
                        empresas_quali_responsavel.append(empresas_quali_responsavel[-1] if empresas_quali_responsavel else None)
                        empresas_natureza_juridica.append(empresas_natureza_juridica[-1] if empresas_natureza_juridica else None)
                        empresas_porte.append(empresas_porte[-1] if empresas_porte else None)
       # Ordenar todas as listas globais com base na lista estabelecimentos_cnpj
        dados_completos = list(zip(
            estabelecimentos_cnpj, estabelecimentos_Cnpj_Raiz, estabelecimentos_identificador_matriz_filial,
            estabelecimentos_nome_fantasia, estabelecimentos_situacao_cadastral, estabelecimentos_data_situacao_cadastral,
            estabelecimentos_motivo_situacao_cadastral, estabelecimentos_cidade_exterior, estabelecimentos_pais,
            estabelecimentos_data_de_inicio_de_atividade, estabelecimentos_cnae_principal, estabelecimentos_cnae_secundario,
            estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial, estabelecimentos_data_situacao_especial,
            estabelecimentos_endereco, estabelecimentos_telefones
        ))
        # Ordenar os dados com base no CNPJ (primeiro campo)
        dados_completos.sort(key=lambda x: x[0])
        # Desempacotar os dados de volta para as listas globais, agora ordenados, convertendo de tuplas para listas
        (
            estabelecimentos_cnpj, estabelecimentos_Cnpj_Raiz, estabelecimentos_identificador_matriz_filial,
            estabelecimentos_nome_fantasia, estabelecimentos_situacao_cadastral, estabelecimentos_data_situacao_cadastral,
            estabelecimentos_motivo_situacao_cadastral, estabelecimentos_cidade_exterior, estabelecimentos_pais,
            estabelecimentos_data_de_inicio_de_atividade, estabelecimentos_cnae_principal, estabelecimentos_cnae_secundario,
            estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial, estabelecimentos_data_situacao_especial,
            estabelecimentos_endereco, estabelecimentos_telefones
        ) = map(list, zip(*dados_completos))  # Usar map(list, ...) para converter as tuplas de volta para listas
    except Exception as e:
        print(f"Erro durante a consulta de estabelecimentos: {e}")
def sincronizar_tamanhos_por_cnpj_raiz(): # Sincronização de dados para Nome e CPF
    indices_por_cnpj = {}
    for idx, cnpj_raiz in enumerate(estabelecimentos_Cnpj_Raiz):
        if cnpj_raiz not in indices_por_cnpj:
            indices_por_cnpj[cnpj_raiz] = []
        indices_por_cnpj[cnpj_raiz].append(idx)
    novos_socios_Cnpj_Raiz, novos_socios_pais, novos_socios_repre_legal = [], [], []
    novos_socios_nome_repre, novos_socios_quali_repre, novos_socios_socios = [], [], []
    novos_empresas_Cnpj_Raiz, novos_empresas_nome, novos_empresas_capital_social = [], [], []
    novos_empresas_ente_federativo, novos_empresas_quali_responsavel = [], []
    novos_empresas_natureza_juridica, novos_empresas_porte = [], []
    for cnpj_raiz, indices in indices_por_cnpj.items():
        try:
            idx_socios = socios_Cnpj_Raiz.index(cnpj_raiz)
            dados_socios = (
                socios_Cnpj_Raiz[idx_socios], socios_pais[idx_socios], socios_repre_legal[idx_socios],
                socios_nome_repre[idx_socios], socios_quali_repre[idx_socios], socios_socios[idx_socios]
            )
        except ValueError:
            dados_socios = (None, None, None, None, None, None)
        try:
            idx_empresas = empresas_Cnpj_Raiz.index(cnpj_raiz)
            dados_empresas = (
                empresas_Cnpj_Raiz[idx_empresas], empresas_nome[idx_empresas], empresas_capital_social[idx_empresas],
                empresas_ente_federativo[idx_empresas], empresas_quali_responsavel[idx_empresas],
                empresas_natureza_juridica[idx_empresas], empresas_porte[idx_empresas]
            )
        except ValueError:
            dados_empresas = (None, None, None, None, None, None, None)
        for _ in indices:
            novos_socios_Cnpj_Raiz.append(dados_socios[0])
            novos_socios_pais.append(dados_socios[1])
            novos_socios_repre_legal.append(dados_socios[2])
            novos_socios_nome_repre.append(dados_socios[3])
            novos_socios_quali_repre.append(dados_socios[4])
            novos_socios_socios.append(dados_socios[5])
            novos_empresas_Cnpj_Raiz.append(dados_empresas[0])
            novos_empresas_nome.append(dados_empresas[1])
            novos_empresas_capital_social.append(dados_empresas[2])
            novos_empresas_ente_federativo.append(dados_empresas[3])
            novos_empresas_quali_responsavel.append(dados_empresas[4])
            novos_empresas_natureza_juridica.append(dados_empresas[5])
            novos_empresas_porte.append(dados_empresas[6])
    socios_Cnpj_Raiz[:] = novos_socios_Cnpj_Raiz
    socios_pais[:] = novos_socios_pais
    socios_repre_legal[:] = novos_socios_repre_legal
    socios_nome_repre[:] = novos_socios_nome_repre
    socios_quali_repre[:] = novos_socios_quali_repre
    socios_socios[:] = novos_socios_socios
    empresas_Cnpj_Raiz[:] = novos_empresas_Cnpj_Raiz
    empresas_nome[:] = novos_empresas_nome
    empresas_capital_social[:] = novos_empresas_capital_social
    empresas_ente_federativo[:] = novos_empresas_ente_federativo
    empresas_quali_responsavel[:] = novos_empresas_quali_responsavel
    empresas_natureza_juridica[:] = novos_empresas_natureza_juridica
    empresas_porte[:] = novos_empresas_porte
def sincronizar_tamanhos(): # Sincronização do tamanho das listas para garantir que sócios e empresas sejam replicados para cada estabelecimento
    while len(empresas_Cnpj_Raiz) < len(estabelecimentos_Cnpj_Raiz):
        empresas_Cnpj_Raiz.append(None)
        empresas_nome.append(None)
        empresas_capital_social.append(None)
        empresas_ente_federativo.append(None)
        empresas_quali_responsavel.append(None)
        empresas_natureza_juridica.append(None)
        empresas_porte.append(None)
    while len(socios_Cnpj_Raiz) < len(estabelecimentos_Cnpj_Raiz):
        socios_Cnpj_Raiz.append(None)
        socios_pais.append(None)
        socios_repre_legal.append(None)
        socios_nome_repre.append(None)
        socios_quali_repre.append(None)
        socios_socios.append(None)
def criar_dataframe():# Função para criar DataFrames com os dados globais coletados nas consultas
    socios_data = {
        "CNPJ Raiz": socios_Cnpj_Raiz,
        "País": socios_pais,
        "Representante Legal": socios_repre_legal,
        "Nome Representante": socios_nome_repre,
        "Qualificação Representante": socios_quali_repre,
        "Sócios": socios_socios}
    empresas_data = {
        "CNPJ Raiz": empresas_Cnpj_Raiz,
        "Nome": empresas_nome,
        "Capital Social": empresas_capital_social,
        "Ente Federativo": empresas_ente_federativo,
        "Qualificação Responsável": empresas_quali_responsavel,
        "Natureza Jurídica": empresas_natureza_juridica,
        "Porte": empresas_porte}
    estabelecimentos_data = {
        "CNPJ Raiz": estabelecimentos_Cnpj_Raiz,
        "Identificador Matriz/Filial": estabelecimentos_identificador_matriz_filial,
        "Nome Fantasia": estabelecimentos_nome_fantasia,
        "Situação Cadastral": estabelecimentos_situacao_cadastral,
        "Data Situação Cadastral": estabelecimentos_data_situacao_cadastral,
        "Motivo Situação Cadastral": estabelecimentos_motivo_situacao_cadastral,
        "CNAE Principal": estabelecimentos_cnae_principal,
        "CNAE Secundário": estabelecimentos_cnae_secundario,
        "Telefones": estabelecimentos_telefones,
        "Correio Eletrônico": estabelecimentos_correio_eletronico,
        "Endereço": estabelecimentos_endereco}
############################         Criar DataFrames            ###############################
    df_socios = pd.DataFrame(socios_data)
    df_empresas = pd.DataFrame(empresas_data)
    df_estabelecimentos = pd.DataFrame(estabelecimentos_data)
    # Retornar os DataFrames
    return df_socios, df_empresas, df_estabelecimentos
@app.route('/export_excel')##Rota para exportar para Excel
def export_excel():
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet()  # Defina o título das colunas abaixo
    worksheet.write('A1', 'Identificador Matriz/Filial')
    worksheet.write('B1', 'Nome da Empresa')
    worksheet.write('C1', 'CNPJ Completo')
    worksheet.write('D1', 'Endereço')
    worksheet.write('E1', 'Sócios')
    worksheet.write('F1', 'Data de Início de Atividade')
    worksheet.write('G1', 'Situação Cadastral')
    worksheet.write('H1', 'Data Situação Cadastral')
    worksheet.write('I1', 'Nome Fantasia')
    worksheet.write('J1', 'Telefones')
    worksheet.write('K1', 'Correio Eletrônico')
    worksheet.write('L1', 'CNAE Principal')
    worksheet.write('M1', 'CNAE Secundário')
    worksheet.write('N1', 'Natureza Jurídica')
    worksheet.write('O1', 'Capital Social')
    worksheet.write('P1', 'Porte')
    worksheet.write('Q1', 'CNPJ Raiz')
    worksheet.write('R1', 'País dos Sócios')
    worksheet.write('S1', 'Representante Legal')
    worksheet.write('T1', 'Nome do Representante')
    worksheet.write('U1', 'Qualificação do Representante')
    worksheet.write('V1', 'Motivo Situação Cadastral')
    worksheet.write('W1', 'Cidade no Exterior')
    worksheet.write('X1', 'País dos Estabelecimentos')
    worksheet.write('Y1', 'Situação Especial')
    worksheet.write('Z1', 'Data Situação Especial')
    worksheet.write('AA1', 'Ente Federativo')
    worksheet.write('AB1', 'Qualificação do Responsável')
    row = 1  # Começar na segunda linha, já que a primeira é dos cabeçalhos
    # Garantir que todos os dados estejam preenchidos corretamente antes de escrever no Excel
    for i in range(len(estabelecimentos_Cnpj_Raiz)):  # Iterar sobre os estabelecimentos
        worksheet.write(row, 0, estabelecimentos_identificador_matriz_filial[i])  # Identificador Matriz/Filial
        worksheet.write(row, 1, empresas_nome[i] if i < len(empresas_nome) else '')  # Nome da Empresa
        worksheet.write(row, 2, formatar_cnpj(estabelecimentos_cnpj[i]))  # CNPJ Completo
        worksheet.write(row, 3, estabelecimentos_endereco[i])  # Endereço
        worksheet.write(row, 4, socios_socios[i] if i < len(socios_socios) else '')  # Sócios
        worksheet.write(row, 5, estabelecimentos_data_de_inicio_de_atividade[i])  # Data de Início de Atividade
        worksheet.write(row, 6, estabelecimentos_situacao_cadastral[i])  # Situação Cadastral
        worksheet.write(row, 7, estabelecimentos_data_situacao_cadastral[i])  # Data Situação Cadastral
        worksheet.write(row, 8, estabelecimentos_nome_fantasia[i])  # Nome Fantasia
        worksheet.write(row, 9, estabelecimentos_telefones[i])  # Telefones
        worksheet.write(row, 10, estabelecimentos_correio_eletronico[i])  # Correio Eletrônico
        worksheet.write(row, 11, estabelecimentos_cnae_principal[i])  # CNAE Principal
        worksheet.write(row, 12, estabelecimentos_cnae_secundario[i])  # CNAE Secundário
        worksheet.write(row, 13, empresas_natureza_juridica[i] if i < len(empresas_natureza_juridica) else '')  # Natureza Jurídica
        worksheet.write(row, 14, empresas_capital_social[i] if i < len(empresas_capital_social) else '')  # Capital Social
        worksheet.write(row, 15, empresas_porte[i] if i < len(empresas_porte) else '')  # Porte
        worksheet.write(row, 16, estabelecimentos_Cnpj_Raiz[i])  # CNPJ Raiz
        worksheet.write(row, 17, socios_pais[i] if i < len(socios_pais) else '')  # País dos Sócios
        worksheet.write(row, 18, socios_repre_legal[i] if i < len(socios_repre_legal) else '')  # Representante Legal
        worksheet.write(row, 19, socios_nome_repre[i] if i < len(socios_nome_repre) else '')  # Nome do Representante
        worksheet.write(row, 20, socios_quali_repre[i] if i < len(socios_quali_repre) else '')  # Qualificação do Representante
        worksheet.write(row, 21, estabelecimentos_motivo_situacao_cadastral[i])  # Motivo Situação Cadastral
        worksheet.write(row, 22, estabelecimentos_cidade_exterior[i])  # Cidade no Exterior
        worksheet.write(row, 23, estabelecimentos_pais[i])  # País dos Estabelecimentos
        worksheet.write(row, 24, estabelecimentos_situacao_especial[i])  # Situação Especial
        worksheet.write(row, 25, estabelecimentos_data_situacao_especial[i])  # Data Situação Especial
        worksheet.write(row, 26, empresas_ente_federativo[i] if i < len(empresas_ente_federativo) else '')  # Ente Federativo
        worksheet.write(row, 27, empresas_quali_responsavel[i] if i < len(empresas_quali_responsavel) else '')  # Qualificação do Responsável
        row += 1  # Passa para a próxima linha após cada estabelecimento
    workbook.close()
    output.seek(0)
    return send_file(output, download_name='resultado.xlsx', as_attachment=True)
@app.route("/api/pesquisa", methods=["POST"]) ##Rota para Swagger
def api_pesquisa():
    """
    Endpoint para realizar pesquisas no banco de dados e retornar resultados em JSON
    ---
    parameters:
      - name: cnpj
        in: formData
        type: string
        required: false
        description: CNPJ para realizar a pesquisa
      - name: nome
        in: formData
        type: string
        required: false
        description: Nome para realizar a pesquisa
      - name: cpf
        in: formData
        type: string
        required: false
        description: CPF para realizar a pesquisa
      - name: coringa
        in: formData
        type: string
        required: false
        description: Palavra-chave coringa para realizar a pesquisa
    responses:
      200:
        description: Resultados da pesquisa em JSON
    """
    cnpj = request.form.get("cnpj")
    nome = request.form.get("nome")
    cpf = request.form.get("cpf")
    coringa = request.form.get("coringa")
    
    conn = connect_to_db()
    if conn:
        cursor = conn.cursor()
        if cnpj:
            pesquisar_cnpj(cursor, cnpj)
        elif nome and cpf:
            cpf_tratado = tratar_cpf(cpf)
            pesquisar_por_nome_e_cpf(cursor, nome, cpf_tratado)
        elif coringa:
            pesquisar_coringa(cursor, coringa)

        # Gerar JSON de resultado
        resultado_json = {
            "sócios": {
                "CNPJ Raiz": socios_Cnpj_Raiz,
                "País": socios_pais,
                "Representante Legal": socios_repre_legal,
                "Nome Representante": socios_nome_repre,
                "Qualificação Representante": socios_quali_repre,
                "Sócios": socios_socios
            },
            "empresas": {
                "CNPJ Raiz": empresas_Cnpj_Raiz,
                "Nome": empresas_nome,
                "Capital Social": empresas_capital_social,
                "Ente Federativo": empresas_ente_federativo,
                "Qualificação Responsável": empresas_quali_responsavel,
                "Natureza Jurídica": empresas_natureza_juridica,
                "Porte": empresas_porte
            },
            "estabelecimentos": {
                "CNPJ Raiz": estabelecimentos_Cnpj_Raiz,
                "Identificador Matriz/Filial": estabelecimentos_identificador_matriz_filial,
                "Nome Fantasia": estabelecimentos_nome_fantasia,
                "Situação Cadastral": estabelecimentos_situacao_cadastral,
                "Data Situação Cadastral": estabelecimentos_data_situacao_cadastral,
                "Motivo Situação Cadastral": estabelecimentos_motivo_situacao_cadastral,
                "Cidade Exterior": estabelecimentos_cidade_exterior,
                "País": estabelecimentos_pais,
                "Data Início Atividade": estabelecimentos_data_de_inicio_de_atividade,
                "CNAE Principal": estabelecimentos_cnae_principal,
                "CNAE Secundário": estabelecimentos_cnae_secundario,
                "Correio Eletrônico": estabelecimentos_correio_eletronico,
                "Situação Especial": estabelecimentos_situacao_especial,
                "Data Situação Especial": estabelecimentos_data_situacao_especial,
                "CNPJ Completo": [formatar_cnpj(cnpj) for cnpj in estabelecimentos_cnpj],
                "Endereço": estabelecimentos_endereco,
                "Telefones": estabelecimentos_telefones
            }
        }

        return jsonify(resultado_json)

    return jsonify({"error": "Falha na conexão com o banco de dados"}), 500
@app.route("/", methods=["GET", "POST"])  # Rota Principal
def index():
    if request.method == "POST":
        cnpj = request.form.get("cnpj")
        nome = request.form.get("nome")
        cpf = request.form.get("cpf")
        coringa = request.form.get("coringa")
        conn = connect_to_db()  # Conectar ao banco de dados
        if conn:
            cursor = conn.cursor()  # Verificar qual pesquisa realizar com base nos dados fornecidos
            if cnpj:
                pesquisar_cnpj(cursor, cnpj)  # Realiza a pesquisa por CNPJ
            elif nome and cpf:
                cpf_tratado = tratar_cpf(cpf)  # Tratamento do CPF antes da consulta
                pesquisar_por_nome_e_cpf(cursor, nome, cpf_tratado)  # Realiza a pesquisa por Nome e CPF
            elif coringa:
                pesquisar_coringa(cursor, coringa)  # Realiza a pesquisa coringa
            
            # Criar um dicionário para converter as variáveis em JSON
            resultado_json = {
                "sócios": {
                    "CNPJ Raiz": socios_Cnpj_Raiz,
                    "País": socios_pais,
                    "Representante Legal": socios_repre_legal,
                    "Nome Representante": socios_nome_repre,
                    "Qualificação Representante": socios_quali_repre,
                    "Sócios": socios_socios
                },
                "empresas": {
                    "CNPJ Raiz": empresas_Cnpj_Raiz,
                    "Nome": empresas_nome,
                    "Capital Social": empresas_capital_social,
                    "Ente Federativo": empresas_ente_federativo,
                    "Qualificação Responsável": empresas_quali_responsavel,
                    "Natureza Jurídica": empresas_natureza_juridica,
                    "Porte": empresas_porte
                },
                "estabelecimentos": {
                    "CNPJ Raiz": estabelecimentos_Cnpj_Raiz,
                    "Identificador Matriz/Filial": estabelecimentos_identificador_matriz_filial,
                    "Nome Fantasia": estabelecimentos_nome_fantasia,
                    "Situação Cadastral": estabelecimentos_situacao_cadastral,
                    "Data Situação Cadastral": estabelecimentos_data_situacao_cadastral,
                    "Motivo Situação Cadastral": estabelecimentos_motivo_situacao_cadastral,
                    "Cidade Exterior": estabelecimentos_cidade_exterior,
                    "País": estabelecimentos_pais,
                    "Data Início Atividade": estabelecimentos_data_de_inicio_de_atividade,
                    "CNAE Principal": estabelecimentos_cnae_principal,
                    "CNAE Secundário": estabelecimentos_cnae_secundario,
                    "Correio Eletrônico": estabelecimentos_correio_eletronico,
                    "Situação Especial": estabelecimentos_situacao_especial,
                    "Data Situação Especial": estabelecimentos_data_situacao_especial,
                    "CNPJ Completo": [formatar_cnpj(cnpj) for cnpj in estabelecimentos_cnpj],
                    "Endereço": estabelecimentos_endereco,
                    "Telefones": estabelecimentos_telefones
                }
            }
            
            # Exibir o resultado como JSON no console
            print(json.dumps(resultado_json, indent=4, ensure_ascii=False))

    estabelecimentos_cnpj_formatado = [formatar_cnpj(cnpj) for cnpj in estabelecimentos_cnpj]  # Formatar CNPJ antes de enviar para o front-end
    
    return render_template(
        "index.html",  # Passar os dados formatados para o front-end
        socios_Cnpj_Raiz=socios_Cnpj_Raiz, socios_pais=socios_pais, socios_repre_legal=socios_repre_legal, socios_nome_repre=socios_nome_repre, 
        socios_quali_repre=socios_quali_repre, socios_socios=socios_socios,
        empresas_Cnpj_Raiz=empresas_Cnpj_Raiz,empresas_nome=empresas_nome, empresas_capital_social=empresas_capital_social, empresas_ente_federativo=empresas_ente_federativo,
        empresas_quali_responsavel=empresas_quali_responsavel, empresas_natureza_juridica=empresas_natureza_juridica,empresas_porte=empresas_porte,
        estabelecimentos_Cnpj_Raiz=estabelecimentos_Cnpj_Raiz, estabelecimentos_identificador_matriz_filial=estabelecimentos_identificador_matriz_filial,
        estabelecimentos_nome_fantasia=estabelecimentos_nome_fantasia, estabelecimentos_situacao_cadastral=estabelecimentos_situacao_cadastral,
        estabelecimentos_data_situacao_cadastral=estabelecimentos_data_situacao_cadastral, estabelecimentos_motivo_situacao_cadastral=estabelecimentos_motivo_situacao_cadastral,
        estabelecimentos_cidade_exterior=estabelecimentos_cidade_exterior, estabelecimentos_pais=estabelecimentos_pais,
        estabelecimentos_data_de_inicio_de_atividade=estabelecimentos_data_de_inicio_de_atividade,estabelecimentos_cnae_principal=estabelecimentos_cnae_principal,
        estabelecimentos_cnae_secundario=estabelecimentos_cnae_secundario, estabelecimentos_correio_eletronico=estabelecimentos_correio_eletronico,
        estabelecimentos_situacao_especial=estabelecimentos_situacao_especial, estabelecimentos_data_situacao_especial=estabelecimentos_data_situacao_especial,
        estabelecimentos_cnpj=estabelecimentos_cnpj_formatado,# <<<<Enviar o CNPJ formatado
        estabelecimentos_endereco=estabelecimentos_endereco, estabelecimentos_telefones=estabelecimentos_telefones,
    )
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
