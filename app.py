from flask import Flask, render_template, request, send_file
import psycopg2
import pandas as pd
import io
import xlsxwriter

app = Flask(__name__)

# Configurações do banco de dados
DB_HOST = "postgresql-171633-0.cloudclusters.net"
DB_PORT = "18857"
DB_NAME = "Banco_Receita"
DB_USER = "Sandro"
DB_PASSWORD = "sandro01"

# Variáveis globais para armazenar resultados das pesquisas
socios_Cnpj_Raiz = []
socios_pais = []
socios_repre_legal = [] 
socios_nome_repre = [] 
socios_quali_repre = [] 
socios_socios = [] 

empresas_Cnpj_Raiz = []
empresas_nome = []
empresas_capital_social = []
empresas_ente_federativo = []
empresas_quali_responsavel = []
empresas_natureza_juridica = []
empresas_porte = []

estabelecimentos_Cnpj_Raiz = []
estabelecimentos_identificador_matriz_filial = []
estabelecimentos_nome_fantasia = []
estabelecimentos_situacao_cadastral = []
estabelecimentos_data_situacao_cadastral = []
estabelecimentos_motivo_situacao_cadastral = []
estabelecimentos_cidade_exterior = []
estabelecimentos_pais = []
estabelecimentos_data_de_inicio_de_atividade = []
estabelecimentos_cnae_principal = []
estabelecimentos_cnae_secundario = []
estabelecimentos_correio_eletronico = []
estabelecimentos_situacao_especial = []
estabelecimentos_data_situacao_especial = []
estabelecimentos_cnpj = []
estabelecimentos_endereco = []
estabelecimentos_telefones = []

# Função para conectar ao banco de dados PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Conexão com o banco de dados bem-sucedida")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para tratar o CNPJ digitado pelo usuário
def tratar_cnpj(cnpj_usuario):
    cnpj_tratado = cnpj_usuario.replace(".", "").replace("/", "").replace("-", "")
    cnpj_raiz = cnpj_tratado[:8]  # Os primeiros 8 dígitos do CNPJ formam o CNPJ raiz
    return cnpj_tratado, cnpj_raiz

# Função para tratar o CPF
def tratar_cpf(cpf_usuario):
    cpf_limpo = cpf_usuario.replace(".", "").replace("-", "")
    return f"***{cpf_limpo[3:9]}**"

# Função para tratar o coringa
def tratar_coringa(coringa_usuario):
    return coringa_usuario.upper()

# Função para pesquisar CNPJ e retornar CNPJ Raiz nas tabelas de Estabelecimentos
def pesquisar_cnpj(cursor, cnpj):

    # Limpar variáveis globais para evitar mistura de resultados
    limpar_variaveis_globais()

    cnpj_tratado, cnpj_raiz = tratar_cnpj(cnpj)
    global estabelecimentos_cnpj, cnpj_r
    
    try:
        for i in range(10):
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"SELECT \"Cnpj Raiz\" FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj\" = '{cnpj_tratado}'"
            cursor.execute(query_estabelecimentos)
            resultado = cursor.fetchone()
            if resultado:
                cnpj_r = resultado[0]  # Armazena o CNPJ Raiz para pesquisas futuras
                pesquisar_cnpj_raiz(cursor, cnpj_r)
                break
    except Exception as e:
        print(f"Erro durante a pesquisa de CNPJ: {e}")

#Função para limpar todas as variáveis globais antes de uma nova pesquisa
def limpar_variaveis_globais():
    global socios_Cnpj_Raiz, socios_socios, socios_repre_legal, socios_nome_repre, socios_quali_repre, socios_pais
    global empresas_Cnpj_Raiz, empresas_nome, empresas_capital_social, empresas_ente_federativo, empresas_quali_responsavel, empresas_natureza_juridica, empresas_porte
    global estabelecimentos_Cnpj_Raiz, estabelecimentos_identificador_matriz_filial, estabelecimentos_nome_fantasia, estabelecimentos_situacao_cadastral, estabelecimentos_data_situacao_cadastral, estabelecimentos_motivo_situacao_cadastral, estabelecimentos_cidade_exterior, estabelecimentos_pais, estabelecimentos_data_de_inicio_de_atividade, estabelecimentos_cnae_principal, estabelecimentos_cnae_secundario, estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial, estabelecimentos_data_situacao_especial, estabelecimentos_cnpj, estabelecimentos_endereco, estabelecimentos_telefones

    # Resetando variáveis globais
    socios_Cnpj_Raiz = []
    socios_socios = []
    socios_repre_legal = []
    socios_nome_repre = []
    socios_quali_repre = []
    socios_pais = []

    empresas_Cnpj_Raiz = []
    empresas_nome = []
    empresas_capital_social = []
    empresas_ente_federativo = []
    empresas_quali_responsavel = []
    empresas_natureza_juridica = []
    empresas_porte = []

    estabelecimentos_Cnpj_Raiz = []
    estabelecimentos_identificador_matriz_filial = []
    estabelecimentos_nome_fantasia = []
    estabelecimentos_situacao_cadastral = []
    estabelecimentos_data_situacao_cadastral = []
    estabelecimentos_motivo_situacao_cadastral = []
    estabelecimentos_cidade_exterior = []
    estabelecimentos_pais = []
    estabelecimentos_data_de_inicio_de_atividade = []
    estabelecimentos_cnae_principal = []
    estabelecimentos_cnae_secundario = []
    estabelecimentos_correio_eletronico = []
    estabelecimentos_situacao_especial = []
    estabelecimentos_data_situacao_especial = []
    estabelecimentos_cnpj = []
    estabelecimentos_endereco = []
    estabelecimentos_telefones = []        

# Função para pesquisa por Nome e CPF nas tabelas de Sócios
def pesquisar_por_nome_e_cpf(cursor, nome, cpf):
    
    # Limpar variáveis globais para evitar mistura de resultados
    limpar_variaveis_globais()
    
    nome_upper = nome.upper()
    global cnpj_r

    try:
        for i in range(10):
            tabela_socios = f"socios{i}"
            query = f"""
            SELECT "Cnpj Raiz" FROM \"public\".\"{tabela_socios}\" 
            WHERE UPPER(\"Socios\") LIKE '%{nome_upper}%'
            AND \"Socios\" LIKE '%{cpf}%'
            """
            cursor.execute(query)
            resultados = cursor.fetchall()  # Obtém todos os resultados
            if resultados:
                for resultado in resultados:
                    cnpj_r = resultado[0]
                    pesquisar_cnpj_raiz(cursor, cnpj_r)  # Pesquisa para cada CNPJ Raiz encontrado
    except Exception as e:
        print(f"Erro durante a pesquisa por Nome e CPF: {e}")


####        Função para realizar a pesquisa Coringa (flexível)      ###

def pesquisar_coringa(cursor, coringa):
    
    # Limpar variáveis globais para evitar mistura de resultados
    limpar_variaveis_globais()
    
    coringa_upper = tratar_coringa(coringa)
    global cnpj_r

    try:
        # Primeiro, pesquisa na tabela de Estabelecimentos
        for i in range(10):
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"""
            SELECT "Cnpj Raiz"
            FROM \"public\".\"{tabela_estabelecimentos}\" 
            WHERE UPPER("Nome Fantasia") LIKE '%{coringa_upper}%'
            OR UPPER("Correio Eletronico") LIKE '%{coringa_upper}%'
            OR UPPER("Telefones") LIKE '%{coringa_upper}%'
            """
            cursor.execute(query_estabelecimentos)
            resultados_estabelecimentos = cursor.fetchall()  # Coletar todos os resultados
            if resultados_estabelecimentos:
                for resultado in resultados_estabelecimentos:
                    cnpj_r = resultado[0]
                    pesquisar_cnpj_raiz(cursor, cnpj_r)

        # Segundo, pesquisa na tabela de Sócios
        for i in range(10):
            tabela_socios = f"socios{i}"
            query_socios = f"""
            SELECT "Cnpj Raiz"
            FROM \"public\".\"{tabela_socios}\"
            WHERE UPPER("Socios") LIKE '%{coringa_upper}%'
            """
            cursor.execute(query_socios)
            resultados_socios = cursor.fetchall()
            if resultados_socios:
                for resultado in resultados_socios:
                    cnpj_r = resultado[0]
                    pesquisar_cnpj_raiz(cursor, cnpj_r)

    except Exception as e:
        print(f"Erro durante a pesquisa coringa: {e}")


# Funções para realizar consultas nas tabelas de Sócios, Empresas e Estabelecimentos
def pesquisar_cnpj_raiz(cursor, cnpj_r):
    pesquisar_cnpj_raiz_socios(cursor, cnpj_r)
    pesquisar_cnpj_raiz_empresas(cursor, cnpj_r)
    pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_r)

def pesquisar_cnpj_raiz_socios(cursor, cnpj_r):
    global socios_Cnpj_Raiz
    global socios_socios
    global socios_repre_legal
    global socios_nome_repre
    global socios_quali_repre
    global socios_pais
    try:
        for i in range(10):
            tabela_socios = f"socios{i}"
            query_socios = f"SELECT * FROM \"public\".\"{tabela_socios}\" WHERE \"Cnpj Raiz\" = '{cnpj_r}'"
            cursor.execute(query_socios)
            resultados = cursor.fetchall()
            for linha in resultados:
                socios_Cnpj_Raiz.append(linha[0])
                socios_pais.append(linha[1])
                socios_repre_legal.append(linha[2])
                socios_nome_repre.append(linha[3])
                socios_quali_repre.append(linha[4])
                socios_socios.append(linha[5])

    except Exception as e:
        print(f"Erro durante a consulta de sócios: {e}")

def pesquisar_cnpj_raiz_empresas(cursor, cnpj_r):
    global empresas_Cnpj_Raiz
    global empresas_nome
    global empresas_capital_social
    global empresas_ente_federativo 
    global empresas_quali_responsavel
    global empresas_natureza_juridica
    global empresas_porte
    try:
        for i in range(10):
            tabela_empresas = f"empresas{i}"
            query_empresas = f"SELECT * FROM \"public\".\"{tabela_empresas}\" WHERE \"Cnpj Raiz\" = '{cnpj_r}'"
            cursor.execute(query_empresas)
            resultados = cursor.fetchall()
            for linha in resultados:
                empresas_Cnpj_Raiz.append(linha[0])
                empresas_nome.append(linha[1])
                empresas_capital_social.append(linha[2])
                empresas_ente_federativo.append(linha[3])
                empresas_quali_responsavel.append(linha[4])
                empresas_natureza_juridica.append(linha[5])
                empresas_porte.append(linha[6])
    except Exception as e:
        print(f"Erro durante a consulta de empresas: {e}")

def pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_r):
    global estabelecimentos_Cnpj_Raiz
    global estabelecimentos_identificador_matriz_filial
    global estabelecimentos_nome_fantasia
    global estabelecimentos_situacao_cadastral
    global estabelecimentos_data_situacao_cadastral
    global estabelecimentos_motivo_situacao_cadastral
    global estabelecimentos_cidade_exterior
    global estabelecimentos_pais
    global estabelecimentos_data_de_inicio_de_atividade
    global estabelecimentos_cnae_principal
    global estabelecimentos_cnae_secundario
    global estabelecimentos_correio_eletronico
    global estabelecimentos_situacao_especial
    global estabelecimentos_data_situacao_especial
    global estabelecimentos_cnpj    
    global estabelecimentos_endereco    
    global estabelecimentos_telefones

    try:
        for i in range(10):
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"SELECT * FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj Raiz\" = '{cnpj_r}'"
            cursor.execute(query_estabelecimentos)
            resultados = cursor.fetchall()
            for linha in resultados:
                estabelecimentos_Cnpj_Raiz.append(linha[0])
                estabelecimentos_identificador_matriz_filial.append(linha[1])  # Identificador Matriz/Filial
                estabelecimentos_nome_fantasia.append(linha[2])  # Nome Fantasia
                estabelecimentos_situacao_cadastral.append(linha[3])  # Situação Cadastral
                estabelecimentos_data_situacao_cadastral.append(linha[4])  # Data Situação Cadastral
                estabelecimentos_motivo_situacao_cadastral.append(linha[5])  # Motivo Situação Cadastral
                estabelecimentos_cidade_exterior.append(linha[6])  # Cidade Exterior
                estabelecimentos_pais.append(linha[7])  # País
                estabelecimentos_data_de_inicio_de_atividade.append(linha[8])  # Data de Início de Atividade
                estabelecimentos_cnae_principal.append(linha[9])  # CNAE Principal
                estabelecimentos_cnae_secundario.append(linha[10])  # CNAE Secundário
                estabelecimentos_correio_eletronico.append(linha[11])  # Correio Eletrônico
                estabelecimentos_situacao_especial.append(linha[12])  # Situação Especial
                estabelecimentos_data_situacao_especial.append(linha[13])  # Data Situação Especial
                estabelecimentos_cnpj.append(linha[14])  # CNPJ
                estabelecimentos_endereco.append(linha[15])  # Endereço
                estabelecimentos_telefones.append(linha[16])  # Telefones
    except Exception as e:
        print(f"Erro durante a consulta de estabelecimentos: {e}")

def criar_dataframe():
    # Coletar os dados globais de sócios
    socios_data = {
        "CNPJ Raiz": socios_Cnpj_Raiz,
        "País": socios_pais,
        "Representante Legal": socios_repre_legal,
        "Nome Representante": socios_nome_repre,
        "Qualificação Representante": socios_quali_repre,
        "Sócios": socios_socios
    }

    # Coletar os dados globais de empresas
    empresas_data = {
        "CNPJ Raiz": empresas_Cnpj_Raiz,
        "Nome": empresas_nome,
        "Capital Social": empresas_capital_social,
        "Ente Federativo": empresas_ente_federativo,
        "Qualificação Responsável": empresas_quali_responsavel,
        "Natureza Jurídica": empresas_natureza_juridica,
        "Porte": empresas_porte
    }

    # Coletar os dados globais de estabelecimentos
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
        "Endereço": estabelecimentos_endereco
    }

    # Criar DataFrames
    df_socios = pd.DataFrame(socios_data)
    df_empresas = pd.DataFrame(empresas_data)
    df_estabelecimentos = pd.DataFrame(estabelecimentos_data)

    # Retornar os DataFrames
    return df_socios, df_empresas, df_estabelecimentos

############################################################################################
####################            Rota para exportar para Excel           ####################
############################################################################################

# Adiciona o início da função export_excel
@app.route('/export_excel')
def export_excel():
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet()

    # Defina o título das colunas
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

    row = 1  # Começar na segunda linha

    # Agrupar os dados de acordo com CNPJ Raiz
    dados_unificados = {}

    # Unifica dados de sócios
    for i in range(len(socios_Cnpj_Raiz)):
        cnpj_raiz = socios_Cnpj_Raiz[i]
        cnpj = socios_Cnpj_Raiz[i]
        nome_empresa = empresas_nome[i]
        
        if cnpj_raiz not in dados_unificados:
            dados_unificados[cnpj_raiz] = {
                "cnpj": cnpj,
                "nome_empresa": nome_empresa,  # Guardar o nome da empresa
                "pais_socios": socios_pais[i],
                "repre_legal": socios_repre_legal[i],
                "nome_repre": socios_nome_repre[i],
                "quali_repre": socios_quali_repre[i],
                "socios": socios_socios[i],
                "empresas": [],
                "estabelecimentos": []
            }
        else:
            # Concatenar os dados dos sócios se o CNPJ Raiz já existir
            dados_unificados[cnpj_raiz]["socios"] += f", {socios_socios[i]}"
    # Unifica dados de empresas
    for i in range(len(empresas_Cnpj_Raiz)):
        cnpj_raiz = empresas_Cnpj_Raiz[i]
        if cnpj_raiz in dados_unificados:
            dados_unificados[cnpj_raiz]["empresas"].append({
                "nome": empresas_nome[i],
                "capital": empresas_capital_social[i],
                "ente_federativo": empresas_ente_federativo[i],
                "quali_responsavel": empresas_quali_responsavel[i],
                "natureza_juridica": empresas_natureza_juridica[i],
                "porte": empresas_porte[i]
            })

    # Unifica dados de estabelecimentos
    for i in range(len(estabelecimentos_Cnpj_Raiz)):
        cnpj_raiz = estabelecimentos_Cnpj_Raiz[i]
        if cnpj_raiz in dados_unificados:
            dados_unificados[cnpj_raiz]["estabelecimentos"].append({
                "identificador": estabelecimentos_identificador_matriz_filial[i],
                "nome_fantasia": estabelecimentos_nome_fantasia[i],
                "situacao_cadastral": estabelecimentos_situacao_cadastral[i],
                "data_situacao": estabelecimentos_data_situacao_cadastral[i],
                "motivo_situacao": estabelecimentos_motivo_situacao_cadastral[i],
                "cidade_exterior": estabelecimentos_cidade_exterior[i],
                "pais": estabelecimentos_pais[i],
                "data_inicio": estabelecimentos_data_de_inicio_de_atividade[i],
                "cnae_principal": estabelecimentos_cnae_principal[i],
                "cnae_secundario": estabelecimentos_cnae_secundario[i],
                "correio": estabelecimentos_correio_eletronico[i],
                "situacao_especial": estabelecimentos_situacao_especial[i],
                "data_especial": estabelecimentos_data_situacao_especial[i],
                "cnpj": estabelecimentos_cnpj[i],
                "endereco": estabelecimentos_endereco[i],
                "telefones": estabelecimentos_telefones[i]
            })        
            # Se o nome da empresa atual estiver vazio, copiar o nome de outra linha com o mesmo CNPJ Raiz
            if not nome_empresa and dados_unificados[cnpj_raiz]["nome_empresa"]:
                nome_empresa = dados_unificados[cnpj_raiz]["nome_empresa"]
            elif nome_empresa and not dados_unificados[cnpj_raiz]["nome_empresa"]:
                dados_unificados[cnpj_raiz]["nome_empresa"] = nome_empresa  # Atualizar se encontrar um nome válido

     # Adiciona os dados unificados na planilha
    for cnpj_raiz, dados in dados_unificados.items():
        # Adiciona dados de sócios
        worksheet.write(row, 16, dados["cnpj"])  # CNPJ específico
        worksheet.write(row, 17, dados["pais_socios"])
        worksheet.write(row, 18, dados["repre_legal"])
        worksheet.write(row, 19, dados["nome_repre"])
        worksheet.write(row, 20, dados["quali_repre"])
        worksheet.write(row, 4, dados["socios"])

        # Adiciona dados de empresas (concatenando os nomes se houver mais de uma)
        if dados["empresas"]:
            for empresa in dados["empresas"]:
                worksheet.write(row, 1, empresa["nome"])
                worksheet.write(row, 14, empresa["capital"])
                worksheet.write(row, 26, empresa["ente_federativo"])
                worksheet.write(row, 27, empresa["quali_responsavel"])
                worksheet.write(row, 13, empresa["natureza_juridica"])
                worksheet.write(row, 15, empresa["porte"])
        else:
            # Se não houver empresas, deixa as colunas em branco
            worksheet.write(row, 1, '')
            worksheet.write(row, 14, '')
            worksheet.write(row, 26, '')
            worksheet.write(row, 27, '')
            worksheet.write(row, 13, '')
            worksheet.write(row, 15, '')

        # Adiciona dados de estabelecimentos (concatenando as informações)
        if dados["estabelecimentos"]:
            for est in dados["estabelecimentos"]:
                worksheet.write(row, 0, est["identificador"])
                worksheet.write(row, 8, est["nome_fantasia"])
                worksheet.write(row, 6, est["situacao_cadastral"])
                worksheet.write(row, 7, est["data_situacao"])
                worksheet.write(row, 21, est["motivo_situacao"])
                worksheet.write(row, 17, est["cidade_exterior"])
                worksheet.write(row, 23, est["pais"])
                worksheet.write(row, 5, est["data_inicio"])
                worksheet.write(row, 11, est["cnae_principal"])
                worksheet.write(row, 12, est["cnae_secundario"])
                worksheet.write(row, 10, est["correio"])
                worksheet.write(row, 24, est["situacao_especial"])
                worksheet.write(row, 25, est["data_especial"])
                worksheet.write(row, 2, est["cnpj"])
                worksheet.write(row, 3, est["endereco"])
                worksheet.write(row, 9, est["telefones"])
                row += 1  # Passa para a próxima linha após cada estabelecimento
        else:
            # Se não houver estabelecimentos, deixa as colunas em branco
            worksheet.write(row, 0, '')
            worksheet.write(row, 8, '')
            worksheet.write(row, 6, '')
            worksheet.write(row, 7, '')
            worksheet.write(row, 21, '')
            worksheet.write(row, 17, '')
            worksheet.write(row, 23, '')
            worksheet.write(row, 5, '')
            worksheet.write(row, 11, '')
            worksheet.write(row, 12, '')
            worksheet.write(row, 10, '')
            worksheet.write(row, 24, '')
            worksheet.write(row, 25, '')
            worksheet.write(row, 2, '')
            worksheet.write(row, 3, '')
            worksheet.write(row, 9, '')
            row += 1  # Passa para a próxima linha mesmo se não houver dados

    workbook.close()
    output.seek(0)
    return send_file(output, download_name='resultado.xlsx', as_attachment=True)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        cnpj = request.form.get("cnpj")
        nome = request.form.get("nome")
        cpf = request.form.get("cpf")
        coringa = request.form.get("coringa")

        # Conectando ao banco de dados e executando suas funções de pesquisa
        conn = connect_to_db()  # Certifique-se de que essa função está definida
        if conn:
            cursor = conn.cursor()

            # Pesquisas condicionais com base nos parâmetros de entrada
            if cnpj:
                pesquisar_cnpj(cursor, cnpj)  # Certifique-se de que essa função está definida
            elif nome and cpf:
                cpf_tratado = tratar_cpf(cpf)  # Certifique-se de que essa função está definida
                pesquisar_por_nome_e_cpf(cursor, nome, cpf_tratado)  # Certifique-se de que essa função está definida
            elif coringa:
                pesquisar_coringa(cursor, coringa)  # Certifique-se de que essa função está definida

    return render_template(
        "index.html",
        socios_Cnpj_Raiz=socios_Cnpj_Raiz,
        socios_pais=socios_pais,
        socios_repre_legal=socios_repre_legal,
        socios_nome_repre=socios_nome_repre,
        socios_quali_repre=socios_quali_repre,
        socios_socios=socios_socios,
        empresas_Cnpj_Raiz=empresas_Cnpj_Raiz,
        empresas_nome=empresas_nome,
        empresas_capital_social=empresas_capital_social,
        empresas_ente_federativo=empresas_ente_federativo,
        empresas_quali_responsavel=empresas_quali_responsavel,
        empresas_natureza_juridica=empresas_natureza_juridica,
        empresas_porte=empresas_porte,
        estabelecimentos_Cnpj_Raiz=estabelecimentos_Cnpj_Raiz,
        estabelecimentos_identificador_matriz_filial=estabelecimentos_identificador_matriz_filial,
        estabelecimentos_nome_fantasia=estabelecimentos_nome_fantasia,
        estabelecimentos_situacao_cadastral=estabelecimentos_situacao_cadastral,
        estabelecimentos_data_situacao_cadastral=estabelecimentos_data_situacao_cadastral,
        estabelecimentos_motivo_situacao_cadastral=estabelecimentos_motivo_situacao_cadastral,
        estabelecimentos_cidade_exterior=estabelecimentos_cidade_exterior,
        estabelecimentos_pais=estabelecimentos_pais,
        estabelecimentos_data_de_inicio_de_atividade=estabelecimentos_data_de_inicio_de_atividade,
        estabelecimentos_cnae_principal=estabelecimentos_cnae_principal,
        estabelecimentos_cnae_secundario=estabelecimentos_cnae_secundario,
        estabelecimentos_correio_eletronico=estabelecimentos_correio_eletronico,
        estabelecimentos_situacao_especial=estabelecimentos_situacao_especial,
        estabelecimentos_data_situacao_especial=estabelecimentos_data_situacao_especial,
        estabelecimentos_cnpj=estabelecimentos_cnpj,
        estabelecimentos_endereco=estabelecimentos_endereco,
        estabelecimentos_telefones=estabelecimentos_telefones,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
