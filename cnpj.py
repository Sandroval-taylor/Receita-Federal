from flask import request, jsonify, render_template
import config
from tabelas import pesquisar_cnpj_raiz
import psycopg2

# Função principal para pesquisa de CNPJ e renderização de HTML
def pesquisar_cnpj():
    cnpj = request.form.get("cnpj")
    if not cnpj:
        return jsonify({"error": "CNPJ não fornecido"}), 400
    
    conn = config.connect_to_db()
    try:
        cursor = conn.cursor()
        config.pesquisa_por_cnpj = True
        config.limpar_variaveis_globais()
        
        cnpj_tratado, cnpj_raiz = config.tratar_cnpj(cnpj)
        
        # Pesquisa nas tabelas de estabelecimentos
        for i in range(10):
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"SELECT \"Cnpj Raiz\" FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj\" = %s"
            cursor.execute(query_estabelecimentos, (cnpj_tratado,))
            resultados = cursor.fetchall()
            if resultados:
                for resultado in resultados:
                    cnpj_r = resultado[0]
                    pesquisar_cnpj_raiz(cursor, cnpj_r)
                break
    except psycopg2.Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()
    
    # Formatar o CNPJ nos resultados antes de enviar ao template
    config.estabelecimentos_cnpj = [config.formatar_cnpj(cnpj) for cnpj in config.estabelecimentos_cnpj]
    sem_resultado = len(config.estabelecimentos_cnpj) == 0
    return render_template("index.html", sem_resultado=sem_resultado, **config.template())