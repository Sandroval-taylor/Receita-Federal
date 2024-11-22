from flask import request, jsonify
import config
from tabelas import pesquisar_cnpj_raiz
import psycopg2
import json

# Variáveis globais para armazenar o último resultado da pesquisa
resultado_ultima_pesquisa = {}

# Função para conexão com o banco de dados
def connect_to_db():
    return psycopg2.connect(
        host=config.DB_HOST, 
        port=config.DB_PORT, 
        dbname=config.DB_NAME, 
        user=config.DB_USER, 
        password=config.DB_PASSWORD
    )

# Função principal consolidada de pesquisa para o Swagger
def api_pesquisa():
    """
    Pesquisas por CNPJ, NOME & CPF, CORINGA
    ---
    tags:
      - PesquisaS
    parameters:
      - name: cnpj
        in: formData
        type: string
        required: false
        description: "CNPJ para realizar a pesquisa"
      - name: nome
        in: formData
        type: string
        required: false
        description: "Nome para realizar a pesquisa"
      - name: cpf
        in: formData
        type: string
        required: false
        description: "CPF para realizar a pesquisa junto com o nome"
      - name: coringa
        in: formData
        type: string
        required: false
        description: "Palavra-chave para realizar uma pesquisa coringa"
    responses:
      200:
        description: "Resultados da pesquisa em JSON"
        schema:
          type: object
          properties:
            sócios:
              type: object
              properties:
                CNPJ Raiz:
                  type: array
                  items:
                    type: string
                País:
                  type: array
                  items:
                    type: string
                Representante Legal:
                  type: array
                  items:
                    type: string
                Nome Representante:
                  type: array
                  items:
                    type: string
                Qualificação Representante:
                  type: array
                  items:
                    type: string
                Sócios:
                  type: array
                  items:
                    type: string
            empresas:
              type: object
              properties:
                CNPJ Raiz:
                  type: array
                  items:
                    type: string
                Nome:
                  type: array
                  items:
                    type: string
                Capital Social:
                  type: array
                  items:
                    type: string
                Ente Federativo:
                  type: array
                  items:
                    type: string
                Qualificação Responsável:
                  type: array
                  items:
                    type: string
                Natureza Jurídica:
                  type: array
                  items:
                    type: string
                Porte:
                  type: array
                  items:
                    type: string
            estabelecimentos:
              type: object
              properties:
                CNPJ Raiz:
                  type: array
                  items:
                    type: string
                Identificador Matriz/Filial:
                  type: array
                  items:
                    type: string
                Nome Fantasia:
                  type: array
                  items:
                    type: string
                Situação Cadastral:
                  type: array
                  items:
                    type: string
                Data Situação Cadastral:
                  type: array
                  items:
                    type: string
                Motivo Situação Cadastral:
                  type: array
                  items:
                    type: string
                Cidade Exterior:
                  type: array
                  items:
                    type: string
                País:
                  type: array
                  items:
                    type: string
                Data Início Atividade:
                  type: array
                  items:
                    type: string
                CNAE Principal:
                  type: array
                  items:
                    type: string
                CNAE Secundário:
                  type: array
                  items:
                    type: string
                Correio Eletrônico:
                  type: array
                  items:
                    type: string
                Situação Especial:
                  type: array
                  items:
                    type: string
                Data Situação Especial:
                  type: array
                  items:
                    type: string
                CNPJ Completo:
                  type: array
                  items:
                    type: string
                Endereço:
                  type: array
                  items:
                    type: string
                Telefones:
                  type: array
                  items:
                    type: string
      400:
        description: "Erro: Parâmetro de pesquisa não fornecido ou falha na conexão"
    """
    cnpj = request.form.get("cnpj")
    nome = request.form.get("nome")
    cpf = request.form.get("cpf")
    coringa = request.form.get("coringa")

    # Limpa variáveis globais antes de uma nova pesquisa
    config.limpar_variaveis_globais()

    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        if cnpj:
            print(f"Iniciando pesquisa por CNPJ: {cnpj}")
            cnpj_tratado, cnpj_raiz = config.tratar_cnpj(cnpj)
            for i in range(10):
                tabela_estabelecimentos = f"estabelecimentos{i}"
                query_estabelecimentos = f"SELECT \"Cnpj Raiz\" FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj\" = %s"
                cursor.execute(query_estabelecimentos, (cnpj_tratado,))
                resultados = cursor.fetchall()
                for resultado in resultados:
                    pesquisar_cnpj_raiz(cursor, resultado[0])

        elif nome and cpf:
            cpf_tratado = config.tratar_cpf(cpf)
            print(f"Iniciando pesquisa por Nome e CPF: Nome={nome}, CPF={cpf_tratado}")
            nome_upper = nome.upper()
            for i in range(10):
                tabela_socios = f"socios{i}"
                query = f"SELECT \"Cnpj Raiz\" FROM \"public\".\"{tabela_socios}\" WHERE UPPER(\"Socios\") LIKE %s AND \"Socios\" LIKE %s"
                cursor.execute(query, (f"%{nome_upper}%", f"%{cpf_tratado}%"))
                for resultado in cursor.fetchall():
                    pesquisar_cnpj_raiz(cursor, resultado[0])

        elif coringa:
            coringa_formatado = config.tratar_coringa(coringa)
            print(f"Iniciando pesquisa por Coringa: {coringa_formatado}")
            for i in range(10):
                tabela_estabelecimentos = f"estabelecimentos{i}"
                query_estabelecimentos = f"SELECT \"Cnpj Raiz\" FROM \"public\".\"{tabela_estabelecimentos}\" WHERE UPPER(\"Nome Fantasia\") LIKE %s OR UPPER(\"Correio Eletronico\") LIKE %s"
                cursor.execute(query_estabelecimentos, (f'%{coringa_formatado}%', f'%{coringa_formatado}%'))
                for resultado in cursor.fetchall():
                    pesquisar_cnpj_raiz(cursor, resultado[0])

        # Sincronizar e formatar dados
        config.sincronizar_tamanhos_por_cnpj_raiz()

        # Gera o JSON de resultado da pesquisa
        resultado_json = {
            "sócios": {
                "CNPJ Raiz": config.socios_Cnpj_Raiz,
                "País": config.socios_pais,
                "Representante Legal": config.socios_repre_legal,
                "Nome Representante": config.socios_nome_repre,
                "Qualificação Representante": config.socios_quali_repre,
                "Sócios": config.socios_socios
            },
            "empresas": {
                "CNPJ Raiz": list(set(config.empresas_Cnpj_Raiz)),
                "Nome": list(set(config.empresas_nome)),
                "Capital Social": list(set(config.empresas_capital_social)),
                "Ente Federativo": list(set(config.empresas_ente_federativo)),
                "Qualificação Responsável": list(set(config.empresas_quali_responsavel)),
                "Natureza Jurídica": list(set(config.empresas_natureza_juridica)),
                "Porte": list(set(config.empresas_porte))
            },
            "estabelecimentos": {
                "CNPJ Raiz": config.estabelecimentos_Cnpj_Raiz,
                "Identificador Matriz/Filial": config.estabelecimentos_identificador_matriz_filial,
                "Nome Fantasia": config.estabelecimentos_nome_fantasia,
                "Situação Cadastral": config.estabelecimentos_situacao_cadastral,
                "Data Situação Cadastral": config.estabelecimentos_data_situacao_cadastral,
                "Motivo Situação Cadastral": config.estabelecimentos_motivo_situacao_cadastral,
                "Cidade Exterior": config.estabelecimentos_cidade_exterior,
                "País": config.estabelecimentos_pais,
                "Data Início Atividade": config.estabelecimentos_data_de_inicio_de_atividade,
                "CNAE Principal": config.estabelecimentos_cnae_principal,
                "CNAE Secundário": config.estabelecimentos_cnae_secundario,
                "Correio Eletrônico": config.estabelecimentos_correio_eletronico,
                "Situação Especial": config.estabelecimentos_situacao_especial,
                "Data Situação Especial": config.estabelecimentos_data_situacao_especial,
                "CNPJ Completo": [config.formatar_cnpj(cnpj) for cnpj in config.estabelecimentos_cnpj],
                "Endereço": config.estabelecimentos_endereco,
                "Telefones": config.estabelecimentos_telefones
            }
        }

        # Atualiza a variável global com o resultado da última pesquisa
        global resultado_ultima_pesquisa
        resultado_ultima_pesquisa = resultado_json

        # Retorna o JSON como resposta da pesquisa
        return jsonify(resultado_json)

    except psycopg2.Error as e:
        print(f"Erro na pesquisa: {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        conn.close()
