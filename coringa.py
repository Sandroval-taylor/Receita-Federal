from flask import request, jsonify, render_template
import config
from tabelas import pesquisar_cnpj_raiz
import psycopg2

def connect_to_db():
    return psycopg2.connect(
        host=config.DB_HOST, 
        port=config.DB_PORT, 
        dbname=config.DB_NAME, 
        user=config.DB_USER, 
        password=config.DB_PASSWORD
    )

def pesquisar_coringa():
    coringa = request.form.get("coringa")
    if not coringa:
        return jsonify({"error": "Termo de pesquisa coringa não fornecido"}), 400

    conn = connect_to_db()
    try:
        cursor = conn.cursor()
        config.pesquisa_por_cnpj = False
        config.limpar_variaveis_globais()

        coringa_formatado = config.tratar_coringa(coringa)
        cnpj_raizes = set()
        
        # Pesquisa nos estabelecimentos e armazena cada CNPJ Raiz encontrado
        for i in range(10):
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"""
                SELECT "Cnpj Raiz" FROM "public"."{tabela_estabelecimentos}"
                WHERE UPPER("Nome Fantasia") LIKE %s OR UPPER("Correio Eletronico") LIKE %s
            """
            cursor.execute(query_estabelecimentos, (f'%{coringa_formatado}%', f'%{coringa_formatado}%'))
            resultados = cursor.fetchall()
            for resultado in resultados:
                cnpj_raizes.add(resultado[0])

        # Realizar as consultas de sócios e empresas para cada CNPJ Raiz encontrado
        for cnpj_r in cnpj_raizes:
            pesquisar_cnpj_raiz(cursor, cnpj_r)
        
        config.sincronizar_tamanhos_por_cnpj_raiz()  # Sincroniza os dados de todas as listas com base no CNPJ Raiz

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Erro durante a pesquisa coringa: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()
    sem_resultado = len(config.estabelecimentos_cnpj) == 0  # Verifica se há resultados
    return render_template("index.html", sem_resultado=sem_resultado, **config.template())
