from flask import Flask, request, render_template, jsonify
import config
from tabelas import pesquisar_cnpj_raiz
import psycopg2
import re

app = Flask(__name__)

def connect_to_db():
    return psycopg2.connect(
        host=config.DB_HOST, 
        port=config.DB_PORT, 
        dbname=config.DB_NAME, 
        user=config.DB_USER, 
        password=config.DB_PASSWORD
    )

def limpar_identificador(identificador):
    """Remove caracteres não numéricos de CPF ou CNPJ."""
    return re.sub(r'\D', '', identificador)

@app.route('/nome_cpf', methods=['POST'])
def pesquisar_nome_cpf():
    nome = request.form.get("nome")
    identificador = request.form.get("identificador")  # Pode ser CPF ou CNPJ
    if not nome or not identificador:
        return jsonify({"error": "Nome e identificador (CPF ou CNPJ) são necessários para a pesquisa"}), 400

    # Limpar o identificador para manter apenas números
    identificador_limpo = limpar_identificador(identificador)

    # Verifica se o identificador é CPF ou CNPJ com base na quantidade de dígitos
    if len(identificador_limpo) == 11:
        tipo = "CPF"
        identificador_tratado = config.tratar_cpf(identificador_limpo)
        valor_pesquisa = identificador_tratado[3:9]  # Extraindo os dígitos desejados do CPF
    elif len(identificador_limpo) == 14:
        tipo = "CNPJ"
        identificador_tratado = config.tratar_cnpj(identificador_limpo)
        
        # Se tratar_cnpj retornar uma tupla, extrair o primeiro valor
        if isinstance(identificador_tratado, tuple):
            identificador_tratado = identificador_tratado[0]
        
        valor_pesquisa = identificador_tratado  # Utiliza o CNPJ completo sem formatação
    else:
        return jsonify({"error": "O identificador deve ter 11 dígitos para CPF ou 14 para CNPJ."}), 400

    # Log de depuração para verificar os valores de nome e identificador
    print(f"Pesquisando com nome: {nome}, identificador ({tipo}): {valor_pesquisa}")

    conn = connect_to_db()
    try:
        cursor = conn.cursor()
        resultado = realizar_pesquisa_por_nome_e_identificador(cursor, nome, valor_pesquisa, tipo)
        return resultado
    finally:
        cursor.close()
        conn.close()

def realizar_pesquisa_por_nome_e_identificador(cursor, nome, valor_pesquisa, tipo):
    config.pesquisa_por_cnpj = False
    config.limpar_variaveis_globais()  
    nome_upper = nome.upper()
    cnpj_raizes = set()
    try:
        for i in range(10):
            tabela_socios = f"socios{i}"
            query = f"""
                SELECT "Cnpj Raiz" FROM "public"."{tabela_socios}" WHERE UPPER("Socios") LIKE %s 
            """
            params = [f"%{nome_upper}%"]

            # Adiciona o filtro para CPF ou CNPJ na coluna "Socios"
            if tipo == "CPF":
                query += ' AND "Socios" LIKE %s'
                params.append(f"%{valor_pesquisa}%")
            else:
                query += ' AND "Socios" LIKE %s'
                params.append(f"%{valor_pesquisa}%")  # Busca o CNPJ tratado na coluna "Socios"

            cursor.execute(query, params)
            resultados = cursor.fetchall()
            if resultados:
                for resultado in resultados:
                    cnpj_raizes.add(resultado[0])  # Extrai o CNPJ Raiz encontrado
    except Exception as e:
        print(f"Erro durante a pesquisa por Nome e {tipo}: {e}")
    if not cnpj_raizes:
        print("Nenhum CNPJ raiz encontrado para o nome e identificador fornecidos.")
    for cnpj_r in cnpj_raizes:
        pesquisar_cnpj_raiz(cursor, cnpj_r)   
    config.sincronizar_tamanhos_por_cnpj_raiz()

    # Formatar o CNPJ nos resultados antes de renderizar o HTML
    config.estabelecimentos_cnpj = [config.formatar_cnpj(cnpj) for cnpj in config.estabelecimentos_cnpj]
    sem_resultado = len(config.estabelecimentos_cnpj) == 0
    return render_template("index.html", sem_resultado=sem_resultado, **config.template())
