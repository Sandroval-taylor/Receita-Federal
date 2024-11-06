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
    tipo_pesquisa = request.form.get("tipo_pesquisa")  # Campo para tipo de pesquisa

    # Validação de entrada
    if not coringa or not tipo_pesquisa:
        return jsonify({"error": "Termo de pesquisa ou tipo de pesquisa não fornecido"}), 400

    conn = connect_to_db()
    try:
        cursor = conn.cursor()
        config.pesquisa_por_cnpj = False
        config.limpar_variaveis_globais()

        coringa_formatado = config.tratar_coringa(coringa)
        cnpj_raizes = set()

        # Definir tabela e coluna baseados no tipo de pesquisa
        tabelas_colunas = {
            "email": ("estabelecimentos", "Correio Eletronico"),
            "telefone": ("estabelecimentos", "Telefones"),
            "socios": ("socios", "Socios"),
            "nome": ("empresas", "Nome da Empresa")
        }
        
        tabela, coluna = tabelas_colunas.get(tipo_pesquisa, (None, None))
        if not tabela or not coluna:
            return jsonify({"error": "Tipo de pesquisa inválido"}), 400

        # Construir e executar a consulta com base na tabela e coluna
        if tabela == "estabelecimentos":
            for i in range(10):  # Itera sobre as tabelas dinâmicas de estabelecimentos
                tabela_nome = f"{tabela}{i}"
                query = f'SELECT "Cnpj Raiz" FROM "public"."{tabela_nome}" WHERE UPPER("{coluna}") LIKE %s'
                cursor.execute(query, (f'%{coringa_formatado}%',))
                resultados = cursor.fetchall()
                cnpj_raizes.update(resultado[0] for resultado in resultados)
        else:
            query = f'SELECT "Cnpj Raiz" FROM "public"."{tabela}" WHERE UPPER("{coluna}") LIKE %s'
            cursor.execute(query, (f'%{coringa_formatado}%',))
            resultados = cursor.fetchall()
            cnpj_raizes.update(resultado[0] for resultado in resultados)

        # Consultas adicionais para cada CNPJ Raiz encontrado
        for cnpj_r in cnpj_raizes:
            pesquisar_cnpj_raiz(cursor, cnpj_r)
        
        config.sincronizar_tamanhos_por_cnpj_raiz()

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Erro durante a pesquisa coringa: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

    # Formatar e renderizar resultados
    config.estabelecimentos_cnpj = [config.formatar_cnpj(cnpj) for cnpj in config.estabelecimentos_cnpj]
    sem_resultado = len(config.estabelecimentos_cnpj) == 0
    return render_template("index.html", sem_resultado=sem_resultado, **config.template())
