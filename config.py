from werkzeug.utils import secure_filename
import psycopg2

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
pesquisa_por_cnpj = False
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

def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST, 
        port=DB_PORT, 
        dbname=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD
    )

def template():
    return {
        "socios_Cnpj_Raiz": socios_Cnpj_Raiz,
        "socios_pais": socios_pais,
        "socios_repre_legal": socios_repre_legal,
        "socios_nome_repre": socios_nome_repre,
        "socios_quali_repre": socios_quali_repre,
        "socios_socios": socios_socios,
        "empresas_Cnpj_Raiz": empresas_Cnpj_Raiz,
        "empresas_nome": empresas_nome,
        "empresas_capital_social": empresas_capital_social,
        "empresas_ente_federativo": empresas_ente_federativo,
        "empresas_quali_responsavel": empresas_quali_responsavel,
        "empresas_natureza_juridica": empresas_natureza_juridica,
        "empresas_porte": empresas_porte,
        "estabelecimentos_Cnpj_Raiz": estabelecimentos_Cnpj_Raiz,
        "estabelecimentos_identificador_matriz_filial": estabelecimentos_identificador_matriz_filial,
        "estabelecimentos_nome_fantasia": estabelecimentos_nome_fantasia,
        "estabelecimentos_situacao_cadastral": estabelecimentos_situacao_cadastral,
        "estabelecimentos_data_situacao_cadastral": estabelecimentos_data_situacao_cadastral,
        "estabelecimentos_motivo_situacao_cadastral": estabelecimentos_motivo_situacao_cadastral,
        "estabelecimentos_cidade_exterior": estabelecimentos_cidade_exterior,
        "estabelecimentos_pais": estabelecimentos_pais,
        "estabelecimentos_data_de_inicio_de_atividade": estabelecimentos_data_de_inicio_de_atividade,
        "estabelecimentos_cnae_principal": estabelecimentos_cnae_principal,
        "estabelecimentos_cnae_secundario": estabelecimentos_cnae_secundario,
        "estabelecimentos_correio_eletronico": estabelecimentos_correio_eletronico,
        "estabelecimentos_situacao_especial": estabelecimentos_situacao_especial,
        "estabelecimentos_data_situacao_especial": estabelecimentos_data_situacao_especial,
        "estabelecimentos_cnpj": estabelecimentos_cnpj,
        "estabelecimentos_endereco": estabelecimentos_endereco,
        "estabelecimentos_telefones": estabelecimentos_telefones
    }
