from werkzeug.utils import secure_filename
import psycopg2

# Configurações do banco de dados
DB_HOST = "postgresql-188561-0.cloudclusters.net"
DB_PORT = "10029"
DB_NAME = "Negativo"
DB_USER = "sandro"
DB_PASSWORD = "sandro01"

# Funções utilitárias
def tratar_cnpj(cnpj_usuario):
    cnpj_limpo = cnpj_usuario.replace(".", "").replace("/", "").replace("-", "")# Remove caracteres indesejados e extrai o CNPJ Raiz
    if len(cnpj_limpo) != 14:
        raise ValueError("CNPJ inválido. O CNPJ deve conter 14 dígitos.")
    return cnpj_limpo[:8]  # Retorna apenas os 8 primeiros dígitos (CNPJ Raiz)

#Formata um CNPJ consolidado no formato XX.XXX.XXX/XXXX-XX.
def formatar_cnpj(cnpj): 
    if len(cnpj) != 14:
        return cnpj  # Retorna o valor original se não tiver o tamanho esperado
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"

def formatar_data(data): #Formata uma data no formato YYYYMMDD para DD/MM/YYYY.
    if len(data) != 8:
        return data  # Retorna o valor original se não tiver o tamanho esperado
    ano = data[:4]
    mes = data[4:6]
    dia = data[6:]
    return f"{dia}/{mes}/{ano}"


def tratar_cpf(cpf_usuario):
    cpf_limpo = cpf_usuario.replace(".", "").replace("-", "")
    return f"***{cpf_limpo[3:9]}**"

def tratar_coringa(coringa_usuario):
    return coringa_usuario.upper()

# Função para limpar variáveis globais
def limpar_variaveis_globais():
    global socios_Cnpj_Raiz, socios_identificador_socio, socios_nome, socios_cpf_cnpj, socios_qualificacao, socios_data_entrada_sociedade
    global socios_pais, socios_representante_legal, socios_nome_representante, socios_qualificacao_representante, socios_faixa_etaria

    global empresas_Cnpj_Raiz, empresas_nome, empresas_capital_social, empresas_ente_federativo
    global empresas_qualificacao_responsavel, empresas_natureza_juridica, empresas_porte

    global estabelecimentos_Cnpj_Raiz, estabelecimentos_cnpj_ordem, estabelecimentos_cnpj_dv
    global estabelecimentos_identificador_matriz_filial, estabelecimentos_nome_fantasia, estabelecimentos_situacao_cadastral
    global estabelecimentos_data_situacao_cadastral, estabelecimentos_motivo_situacao_cadastral, estabelecimentos_cidade_exterior
    global estabelecimentos_pais, estabelecimentos_data_inicio_atividade, estabelecimentos_cnae_principal, estabelecimentos_cnae_secundario
    global estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial, estabelecimentos_data_situacao_especial
    global estabelecimentos_endereco, estabelecimentos_telefones, estabelecimentos_cnpj_consolidado
    global estabelecimentos_Tipo_de_Logradouro, estabelecimentos_Logradouro, estabelecimentos_Numero
    global estabelecimentos_Complemento, estabelecimentos_Bairro, estabelecimentos_Cep, estabelecimentos_UF, estabelecimentos_municipio
    global estabelecimentos_DDD1, estabelecimentos_Telefone1,  estabelecimentos_DDD2, estabelecimentos_Telefone2
    global estabelecimentos_DDD_Fax, estabelecimentos_Fax 

    socios_Cnpj_Raiz, socios_identificador_socio, socios_nome, socios_cpf_cnpj, socios_qualificacao, socios_data_entrada_sociedade = [], [], [], [], [], []
    socios_pais, socios_representante_legal, socios_nome_representante, socios_qualificacao_representante, socios_faixa_etaria = [], [], [], [], []

    empresas_Cnpj_Raiz, empresas_nome, empresas_capital_social, empresas_ente_federativo = [], [], [], []
    empresas_qualificacao_responsavel, empresas_natureza_juridica, empresas_porte = [], [], []

    estabelecimentos_Cnpj_Raiz, estabelecimentos_cnpj_ordem, estabelecimentos_cnpj_dv = [], [], []
    estabelecimentos_identificador_matriz_filial, estabelecimentos_nome_fantasia, estabelecimentos_situacao_cadastral = [], [], []
    estabelecimentos_data_situacao_cadastral, estabelecimentos_motivo_situacao_cadastral, estabelecimentos_cidade_exterior = [], [], []
    estabelecimentos_pais, estabelecimentos_data_inicio_atividade, estabelecimentos_cnae_principal, estabelecimentos_cnae_secundario = [], [], [], []
    estabelecimentos_correio_eletronico, estabelecimentos_situacao_especial, estabelecimentos_data_situacao_especial = [], [], []
    estabelecimentos_endereco, estabelecimentos_telefones, estabelecimentos_cnpj_consolidado = [], [], []
    estabelecimentos_Tipo_de_Logradouro, estabelecimentos_Logradouro, estabelecimentos_Numero = [], [], []
    estabelecimentos_Complemento, estabelecimentos_Bairro, estabelecimentos_Cep, estabelecimentos_UF, estabelecimentos_municipio = [], [], [], [], []
    estabelecimentos_DDD1, estabelecimentos_Telefone1,  estabelecimentos_DDD2, estabelecimentos_Telefone2 = [], [], [], []
    estabelecimentos_DDD_Fax, estabelecimentos_Fax = [], []


def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST, 
        port=DB_PORT, 
        dbname=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD
    )
