from flask import Flask, render_template, request, send_file, redirect, url_for
import psycopg2
import pandas as pd
from io import BytesIO
import os
import webbrowser

# Inicializa a aplicação Flask
app = Flask(__name__)

# Configurações do banco de dados PostgreSQL
DB_HOST = "postgresql-171633-0.cloudclusters.net"
DB_PORT = "18857"
DB_NAME = "Banco_Receita"
DB_USER = "Sandro"
DB_PASSWORD = "sandro01"

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
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
    
# Função para obter os nomes das colunas de uma tabela específica
def get_column_names(cursor, table_name):
    cursor.execute(f"SELECT * FROM \"public\".\"{table_name}\" LIMIT 0")
    return [desc[0] for desc in cursor.description]

# Função para formatar o CPF/CNPJ dependendo do número de dígitos
def formatar_cpf_cnpj(cpf):
    cpf = cpf.replace(".", "").replace("-", "").replace("/", "")
    if len(cpf) == 11:
        cpf_formatado = "***" + cpf[3:9] + "**"
    elif len(cpf) == 14:
        cpf_formatado = cpf
    else:
        raise ValueError("CPF/CNPJ deve conter 11 ou 14 dígitos")
    return cpf_formatado

# Função para limpar o CNPJ, removendo qualquer caractere que não seja um dígito
def limpar_cnpj(cnpj):
    return ''.join(filter(str.isdigit, cnpj))

# Função para pesquisar usando um valor coringa em várias colunas e tabelas
def pesquisar_coringa(cursor, coringa):
    cnpj_raiz_lista = []

    # Pesquisando nas tabelas de Empresas
    for i in range(10):
        tabela_empresas = f"Empresas{i}"
        query_empresas = f"""
            SELECT "Cnpj Raiz" FROM "public"."{tabela_empresas}"
            WHERE "Nome da Empresa" LIKE '%{coringa}%'
        """
        cursor.execute(query_empresas)
        resultados_empresas = cursor.fetchall()
        cnpj_raiz_lista.extend([res[0] for res in resultados_empresas])

    # Pesquisando nas tabelas de Estabelecimentos
    for i in range(10):
        tabela_estabelecimentos = f"Estabelecimentos{i}"
        query_estabelecimentos = f"""
            SELECT "Cnpj Raiz" FROM "public"."{tabela_estabelecimentos}"
            WHERE "Nome Fantasia" LIKE '%{coringa}%'
            OR "Correio Eletronico" LIKE '%{coringa}%'
            OR "Endereco" LIKE '%{coringa}%'
            OR "Telefones" LIKE '%{coringa}%'
        """
        cursor.execute(query_estabelecimentos)
        resultados_estabelecimentos = cursor.fetchall()
        cnpj_raiz_lista.extend([res[0] for res in resultados_estabelecimentos])

    # Pesquisando nas tabelas de Sócios
    for i in range(10):
        tabela_socios = f"Socios{i}"
        query_socios = f"""
            SELECT "Cnpj Raiz" FROM "public"."{tabela_socios}"
            WHERE "Socios" LIKE '%{coringa}%'
        """
        cursor.execute(query_socios)
        resultados_socios = cursor.fetchall()
        cnpj_raiz_lista.extend([res[0] for res in resultados_socios])

    # Remover duplicados
    cnpj_raiz_lista = list(set(cnpj_raiz_lista))

    return cnpj_raiz_lista

# Função para pesquisar sócios pelo nome e CPF/CNPJ formatado nas tabelas de Sócios
def pesquisar_por_nome_e_nome_usuario(cursor, nome, cpf):
    resultados_totais = []
    for i in range(10):
        tabela = f"Socios{i}"
        query = f"SELECT * FROM \"public\".\"{tabela}\" WHERE \"Socios\" LIKE '%{nome}%' AND \"Socios\" LIKE '%{cpf}%'"
        cursor.execute(query)
        resultados = cursor.fetchall()
        if resultados:
            resultados_totais.extend(resultados)
    return resultados_totais

# Função para pesquisar o CNPJ completo nas tabelas de Estabelecimentos
def pesquisar_cnpj_raiz(cursor, cnpj):
    cnpj_limpo = limpar_cnpj(cnpj)
    cnpj_raiz = cnpj_limpo[:8]
    resultados_totais = {}

    tabela_estabelecimentos = "Estabelecimentos0"
    colunas = get_column_names(cursor, tabela_estabelecimentos)
    
    coluna_cnpj = None
    for coluna in colunas:
        if "cnpj" in coluna.lower() and "raiz" not in coluna.lower():
            coluna_cnpj = coluna
            break
    
    if not coluna_cnpj:
        return {"erro": "Coluna CNPJ não encontrada nas tabelas de Estabelecimentos."}

    for i in range(10):
        tabela_estabelecimentos = f"Estabelecimentos{i}"
        query = f"SELECT * FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"{coluna_cnpj}\" = '{cnpj_limpo}'"
        cursor.execute(query)
        resultados_estabelecimentos = cursor.fetchall()

        if resultados_estabelecimentos:
            resultados_totais[cnpj_raiz] = [(resultado, tabela_estabelecimentos) for resultado in resultados_estabelecimentos]

    return resultados_totais

# Função para pesquisar o CNPJ raiz nas tabelas de Sócios
def pesquisar_cnpj_raiz_socios(cursor, cnpj_raiz_lista):
    resultados_totais = {}
    for i in range(10):
        tabela_socios = f"Socios{i}"
        for cnpj_raiz in cnpj_raiz_lista:
            query_socios = f"SELECT * FROM \"public\".\"{tabela_socios}\" WHERE \"Cnpj Raiz\" = '{cnpj_raiz}'"
            cursor.execute(query_socios)
            resultados_socios = cursor.fetchall()
            if resultados_socios:
                if cnpj_raiz not in resultados_totais:
                    resultados_totais[cnpj_raiz] = []
                resultados_totais[cnpj_raiz].extend([(resultado, tabela_socios) for resultado in resultados_socios])
    return resultados_totais

# Função para pesquisar o CNPJ raiz nas tabelas de Empresas
def pesquisar_cnpj_raiz_empresas(cursor, cnpj_raiz_lista):
    resultados_totais = {}
    for i in range(10):
        tabela_empresas = f"Empresas{i}"
        for cnpj_raiz in cnpj_raiz_lista:
            query_empresas = f"SELECT * FROM \"public\".\"{tabela_empresas}\" WHERE \"Cnpj Raiz\" = '{cnpj_raiz}'"
            cursor.execute(query_empresas)
            resultados_empresas = cursor.fetchall()
            if resultados_empresas:
                if cnpj_raiz not in resultados_totais:
                    resultados_totais[cnpj_raiz] = []
                resultados_totais[cnpj_raiz].extend([(resultado, tabela_empresas) for resultado in resultados_empresas])
    return resultados_totais

# Função para pesquisar o CNPJ raiz nas tabelas de Estabelecimentos
def pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_raiz_lista):
    resultados_totais = {}
    for i in range(10):
        tabela_estabelecimentos = f"Estabelecimentos{i}"
        for cnpj_raiz in cnpj_raiz_lista:
            query_estabelecimentos = f"SELECT * FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj Raiz\" = '{cnpj_raiz}'"
            cursor.execute(query_estabelecimentos)
            resultados_estabelecimentos = cursor.fetchall()
            if resultados_estabelecimentos:
                if cnpj_raiz not in resultados_totais:
                    resultados_totais[cnpj_raiz] = []
                resultados_totais[cnpj_raiz].extend([(resultado, tabela_estabelecimentos) for resultado in resultados_estabelecimentos])
    return resultados_totais

# Rota principal da aplicação
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        nome = request.form.get('nome')
        cpf = request.form.get('cpf')
        cnpj = request.form.get('cnpj')
        coringa = request.form.get('coringa')
        folder_path = request.form.get('folder_path')  # Adicionado para o caminho da pasta

        if folder_path:
            return redirect(url_for('unify_excel', folder_path=folder_path))

        conn = connect_to_db()
        if not conn:
            return "Erro ao conectar com o banco de dados"
        
        cursor = conn.cursor()
        resultados_combinados = {}

        if nome and cpf:
            try:
                cpf_formatado = formatar_cpf_cnpj(cpf)
            except ValueError as e:
                return f"Erro: {str(e)}"
            resultados_socios = pesquisar_por_nome_e_nome_usuario(cursor, nome.upper(), cpf_formatado)
            cnpj_raiz_lista = [resultado[0] for resultado in resultados_socios]
            resultados_empresas_cnpj_raiz = pesquisar_cnpj_raiz_empresas(cursor, cnpj_raiz_lista)
            resultados_socios_cnpj_raiz = pesquisar_cnpj_raiz_socios(cursor, cnpj_raiz_lista)
            resultados_estabelecimentos_cnpj_raiz = pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_raiz_lista)
            for cnpj_raiz in cnpj_raiz_lista:
                resultados_combinados[cnpj_raiz] = (
                    resultados_socios_cnpj_raiz.get(cnpj_raiz, []) +
                    resultados_empresas_cnpj_raiz.get(cnpj_raiz, []) +
                    resultados_estabelecimentos_cnpj_raiz.get(cnpj_raiz, [])
                )

        elif cnpj:
            resultados_combinados = pesquisar_cnpj_raiz(cursor, cnpj)
            if resultados_combinados:
                cnpj_raiz_lista = list(resultados_combinados.keys())
                resultados_socios = pesquisar_cnpj_raiz_socios(cursor, cnpj_raiz_lista)
                resultados_empresas = pesquisar_cnpj_raiz_empresas(cursor, cnpj_raiz_lista)
                resultados_estabelecimentos_cnpj_raiz = pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_raiz_lista)
                for cnpj_raiz in cnpj_raiz_lista:
                    resultados_combinados[cnpj_raiz].extend(
                        resultados_socios.get(cnpj_raiz, []) +
                        resultados_empresas.get(cnpj_raiz, []) +
                        resultados_estabelecimentos_cnpj_raiz.get(cnpj_raiz, [])
                    )

        elif coringa:
            cnpj_raiz_lista = pesquisar_coringa(cursor, coringa)
            resultados_socios = pesquisar_cnpj_raiz_socios(cursor, cnpj_raiz_lista)
            resultados_empresas = pesquisar_cnpj_raiz_empresas(cursor, cnpj_raiz_lista)
            resultados_estabelecimentos = pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_raiz_lista)

            for cnpj_raiz in cnpj_raiz_lista:
                resultados_combinados[cnpj_raiz] = (
                    resultados_empresas.get(cnpj_raiz, []) +
                    resultados_estabelecimentos.get(cnpj_raiz, []) +
                    resultados_socios.get(cnpj_raiz, [])
                )

        conn.close()
        return render_template('index.html', resultados=resultados_combinados)

    return render_template('index.html')

# Função para unificar e processar arquivos Excel (originalmente de Unificar.py)
@app.route('/unify_excel', methods=['POST'])
def unify_excel():
    try:
        folder_path = request.form.get('folder_path')
        folder_path = os.path.normpath(folder_path)  # Normaliza o caminho da pasta

        if folder_path:
            try:
                df_list = []
                for file_name in os.listdir(folder_path):
                    if file_name.endswith(('.xlsx', '.xls')):
                        file_path = os.path.join(folder_path, file_name)
                        df = pd.read_excel(file_path, header=None)
                        df = df.iloc[6:]
                        df_list.append(df)

                # Concatena todos os DataFrames em um único DataFrame
                df = pd.concat(df_list, ignore_index=True)

                # Unifica as colunas 6, 7 e 8, separando os valores com "-"
                df[6] = df.apply(lambda row: ' - '.join([str(row.iloc[6]), str(row.iloc[7]), str(row.iloc[8])]), axis=1)

                # Apaga as colunas 5, 7 e 8
                df.drop(columns=[5, 7, 8], inplace=True)

                # Função para unificar as linhas agrupadas
                def unificar_linhas(group):
                    return ' \n '.join(group.astype(str))

                # Agrupa o DataFrame com base nas colunas 3 e 4 e unifica os valores
                condicao = df[3].duplicated(keep=False) & df[4].duplicated(keep=False)
                df[6] = df.groupby([3, 4])[6].transform(lambda x: unificar_linhas(x) if condicao.any() else '')

                # Remove as linhas duplicadas com base nas colunas 3 e 4
                df.drop_duplicates(subset=[3, 4], keep='first', inplace=True)

                # Move a coluna 6 para a primeira posição
                coluna_6 = df.pop(6)
                df.insert(0, 6, coluna_6)

                # Remove nomes duplicados em cada célula da primeira coluna
                df[6] = df[6].apply(lambda x: ' \n '.join(pd.unique(x.split(' \n '))))

                # Converte a coluna 3 para o tipo de dados datetime
                df[3] = pd.to_datetime(df[3], errors='coerce')

                # Ordena o DataFrame com base na coluna 3 em ordem decrescente
                df = df.sort_values(by=[3], ascending=False)

                # Define o caminho para salvar o arquivo Excel unificado
                arquivo_padrao = os.path.join(folder_path, "Unificado.xlsx")
                df.to_excel(arquivo_padrao, index=False, header=False)

                return "Dados unificados e reorganizados salvos com sucesso!"
            except Exception as e:
                return f"Erro ao processar os arquivos: {e}"
        else:
            return "Nenhum diretório foi fornecido."
    except Exception as e:
        return f"Erro ao acessar o diretório: {e}"


# Rota para exportar os resultados em um arquivo Excel
@app.route('/export', methods=['POST'])
def export():
    resultados = request.form.get('resultados')
    resultados_combinados = eval(resultados)

    data_dict = {}
    for cnpj_raiz, dados in resultados_combinados.items():
        if cnpj_raiz not in data_dict:
            data_dict[cnpj_raiz] = {}

        for resultado, origem_tabela in dados:
            prefixo = ''
            if 'Socios' in origem_tabela:
                prefixo = 'Socio_'
            elif 'Empresas' in origem_tabela:
                prefixo = 'Empresa_'
            elif 'Estabelecimentos' in origem_tabela:
                prefixo = 'Estabelecimento_'

            for idx, value in enumerate(resultado):
                nome_coluna = f"{prefixo}Coluna_{idx + 1}"
                if nome_coluna not in data_dict[cnpj_raiz]:
                    data_dict[cnpj_raiz][nome_coluna] = value if value is not None else ''
                else:
                    if value is not None and str(value) not in str(data_dict[cnpj_raiz][nome_coluna]):
                        data_dict[cnpj_raiz][nome_coluna] += f", {value}"

    df = pd.DataFrame.from_dict(data_dict, orient='index').reset_index()
    df.rename(columns={'index': 'CNPJ Raiz'}, inplace=True)

    column_mapping = {
        'Empresa_Coluna_2': 'Nome da Empresa',
        'Empresa_Coluna_3': 'Natureza Jurídica',
        'Empresa_Coluna_4': 'Qualificação do Responsável',
        'Empresa_Coluna_5': 'Capital Social',
        'Empresa_Coluna_6': 'Porte da Empresa',
        'Empresa_Coluna_7': 'Ente Federativo',
        'Estabelecimento_Coluna_2': 'Matriz/Filial',
        'Estabelecimento_Coluna_3': 'Nome Fantasia',
        'Estabelecimento_Coluna_4': 'Situação Cadastral',
        'Estabelecimento_Coluna_5': 'Data Situação Cadastral',
        'Estabelecimento_Coluna_6': 'Motivo Situação Cadastral',
        'Estabelecimento_Coluna_7': 'Nome da Cidade no Exterior',
        'Estabelecimento_Coluna_8': 'País da empresa',
        'Estabelecimento_Coluna_9': 'Data de Início de Atividade',
        'Estabelecimento_Coluna_10': 'CNAE Principal',
        'Estabelecimento_Coluna_11': 'CNAE Secundário',
        'Estabelecimento_Coluna_12': 'Correio Eletrônico',
        'Estabelecimento_Coluna_13': 'Situação Especial',
        'Estabelecimento_Coluna_14': 'Data Situação Especial',
        'Estabelecimento_Coluna_15': 'CNPJ Completo',
        'Estabelecimento_Coluna_16': 'Endereço',
        'Estabelecimento_Coluna_17': 'Telefones',
        'Socio_Coluna_2': 'País do Sócio',
        'Socio_Coluna_3': 'Representante Legal',
        'Socio_Coluna_4': 'Nome do Representante',
        'Socio_Coluna_5': 'Qualificação do Representante',
        'Socio_Coluna_6': 'Sócios'
    }

    df.rename(columns=column_mapping, inplace=True)

    if len(df.columns) > 25:
        df.drop(df.columns[[0, 1, 8, 25]], axis=1, inplace=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)

    output.seek(0)

    return send_file(output, as_attachment=True, download_name='resultados.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

if __name__ == "__main__":
    # Inicie o servidor Flask
    app.run(debug=True, use_reloader=False)  # use_reloader=False para evitar a execução dupla

    # Abra o navegador na URL do Flask
    webbrowser.open("http://127.0.0.1:5000/")

