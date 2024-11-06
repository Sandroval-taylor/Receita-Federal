from flask import Flask, request, render_template, jsonify
import config
from tabelas import pesquisar_cnpj_raiz
import psycopg2

app = Flask(__name__)

def connect_to_db():
    return psycopg2.connect(
        host=config.DB_HOST, 
        port=config.DB_PORT, 
        dbname=config.DB_NAME, 
        user=config.DB_USER, 
        password=config.DB_PASSWORD
    )

@app.route('/nome_cpf', methods=['POST'])
def pesquisar_nome_cpf():
    nome = request.form.get("nome")
    cpf = request.form.get("cpf")
    if not nome or not cpf:
        return jsonify({"error": "Nome e CPF são necessários para a pesquisa"}), 400
    cpf_tratado = config.tratar_cpf(cpf)
    conn = connect_to_db()
    try:
        cursor = conn.cursor()
        resultado = realizar_pesquisa_por_nome_e_cpf(cursor, nome, cpf_tratado)
        return resultado
    finally:
        cursor.close()
        conn.close()

def realizar_pesquisa_por_nome_e_cpf(cursor, nome, cpf_tratado):
    config.pesquisa_por_cnpj = False
    config.limpar_variaveis_globais()  
    nome_upper = nome.upper()
    cnpj_raizes = set()
    try:
        for i in range(10):
            tabela_socios = f"socios{i}"
            query = f"""
                SELECT "Cnpj Raiz" FROM "public"."{tabela_socios}" WHERE UPPER("Socios") LIKE '%{nome_upper}%' AND "Socios" LIKE '%{cpf_tratado}%'"""
            cursor.execute(query)
            resultados = cursor.fetchall()
            if resultados:
                for resultado in resultados:
                    cnpj_raizes.add(resultado[0])
    except Exception as e:
        print(f"Erro durante a pesquisa por Nome e CPF: {e}")
    if not cnpj_raizes:
        print("Nenhum CNPJ raiz encontrado para o nome e CPF fornecidos.")
    for cnpj_r in cnpj_raizes:
        pesquisar_cnpj_raiz(cursor, cnpj_r)   
    config.sincronizar_tamanhos_por_cnpj_raiz()

    # Formatar o CNPJ nos resultados antes de renderizar o HTML
    config.estabelecimentos_cnpj = [config.formatar_cnpj(cnpj) for cnpj in config.estabelecimentos_cnpj]
    sem_resultado = len(config.estabelecimentos_cnpj) == 0
    return render_template("index.html", sem_resultado=sem_resultado, **config.template())
