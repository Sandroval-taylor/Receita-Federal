import config

def pesquisar_cnpj_raiz(cursor, cnpj_r):
    """
    Função principal para chamar as pesquisas nas tabelas.
    Evita duplicações ao consolidar os resultados.
    """
    pesquisar_cnpj_raiz_socios(cursor, cnpj_r)
    pesquisar_cnpj_raiz_empresas(cursor, cnpj_r)
    pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_r)

def pesquisar_cnpj_raiz_socios(cursor, cnpj_r):
    try:
        for i in range(10):
            tabela_socios = f"socios{i}"
            query_socios = f"SELECT * FROM \"public\".\"{tabela_socios}\" WHERE \"Cnpj Raiz\" = %s"
            cursor.execute(query_socios, (cnpj_r,))
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:
                    if linha[0] not in config.socios_Cnpj_Raiz:
                        config.socios_Cnpj_Raiz.append(linha[0])
                        config.socios_identificador_socio.append(linha[1])
                        config.socios_nome.append(linha[2])
                        config.socios_cpf_cnpj.append(linha[3])
                        config.socios_qualificacao.append(linha[4])
                        config.socios_data_entrada_sociedade.append(linha[5])
                        config.socios_pais.append(linha[6])
                        config.socios_representante_legal.append(linha[7])
                        config.socios_nome_representante.append(linha[8])
                        config.socios_qualificacao_representante.append(linha[9])
                        config.socios_faixa_etaria.append(linha[10])
    except Exception as e:
        print(f"Erro durante a consulta de sócios: {e}")

def pesquisar_cnpj_raiz_empresas(cursor, cnpj_r):
    try:
        for i in range(10):
            tabela_empresas = f"empresas{i}"
            query_empresas = f"SELECT * FROM \"public\".\"{tabela_empresas}\" WHERE \"Cnpj Raiz\" = %s"
            cursor.execute(query_empresas, (cnpj_r,))
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:
                    if linha[0] not in config.empresas_Cnpj_Raiz:
                        config.empresas_Cnpj_Raiz.append(linha[0])
                        config.empresas_nome.append(linha[1])
                        config.empresas_natureza_juridica.append(linha[2])
                        config.empresas_qualificacao_responsavel.append(linha[3])
                        config.empresas_capital_social.append(linha[4])
                        config.empresas_porte.append(linha[5])
                        config.empresas_ente_federativo.append(linha[6])
                break  # Para após o primeiro resultado encontrado
    except Exception as e:
        print(f"Erro durante a consulta de empresas: {e}")

def pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_r):
    try:
        for i in range(10):
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"SELECT * FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj Raiz\" = %s"
            cursor.execute(query_estabelecimentos, (cnpj_r,))
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:
                    cnpj_completo = f"{str(linha[0]).zfill(8)}{str(linha[1]).zfill(4)}{str(linha[2]).zfill(2)}"
                    cnpj_formatado = config.formatar_cnpj(cnpj_completo)
                    if cnpj_completo not in config.estabelecimentos_cnpj_consolidado:
                        config.estabelecimentos_Cnpj_Raiz.append(linha[0])
                        config.estabelecimentos_cnpj_consolidado.append(cnpj_formatado)
                        config.estabelecimentos_identificador_matriz_filial.append(linha[3])
                        config.estabelecimentos_nome_fantasia.append(linha[4])
                        config.estabelecimentos_situacao_cadastral.append(linha[5])
                        config.estabelecimentos_data_situacao_cadastral.append(config.formatar_data(str(linha[6])))
                        config.estabelecimentos_motivo_situacao_cadastral.append(linha[7])
                        config.estabelecimentos_cidade_exterior.append(linha[8])
                        config.estabelecimentos_pais.append(linha[9])
                        config.estabelecimentos_data_inicio_atividade.append(config.formatar_data(str(linha[10])))
                        config.estabelecimentos_cnae_principal.append(linha[11])
                        config.estabelecimentos_cnae_secundario.append(linha[12])
                        config.estabelecimentos_Tipo_de_Logradouro.append(linha[13])
                        config.estabelecimentos_Logradouro.append(linha[14])
                        config.estabelecimentos_Numero.append(linha[15])
                        config.estabelecimentos_Complemento.append(linha[16])
                        config.estabelecimentos_Bairro.append(linha[17])
                        config.estabelecimentos_Cep.append(linha[18])
                        config.estabelecimentos_UF.append(linha[19])
                        config.estabelecimentos_municipio.append(linha[20])
                        config.estabelecimentos_DDD1.append(linha[21])
                        config.estabelecimentos_Telefone1.append(linha[22])
                        config.estabelecimentos_DDD2.append(linha[23])
                        config.estabelecimentos_Telefone2.append(linha[24])
                        config.estabelecimentos_DDD_Fax.append(linha[25])
                        config.estabelecimentos_Fax.append(linha[26])
                        config.estabelecimentos_correio_eletronico.append(linha[27])
                        config.estabelecimentos_situacao_especial.append(linha[28])
                        config.estabelecimentos_data_situacao_especial.append(linha[29])

        ordenar_estabelecimentos() # Ordenar os dados pela lista de CNPJ
    except Exception as e:
        print(f"Erro durante a consulta de estabelecimentos: {e}")
def ordenar_estabelecimentos():
    try:
        # Zipa todas as listas relacionadas a estabelecimentos
        dados_completos_estabelecimentos = list(zip(
            config.estabelecimentos_cnpj_consolidado, config.estabelecimentos_Cnpj_Raiz,
            config.estabelecimentos_identificador_matriz_filial, config.estabelecimentos_nome_fantasia,
            config.estabelecimentos_situacao_cadastral, config.estabelecimentos_data_situacao_cadastral,
            config.estabelecimentos_motivo_situacao_cadastral, config.estabelecimentos_cidade_exterior,
            config.estabelecimentos_pais, config.estabelecimentos_data_inicio_atividade,
            config.estabelecimentos_cnae_principal, config.estabelecimentos_cnae_secundario,
            config.estabelecimentos_Tipo_de_Logradouro, config.estabelecimentos_Logradouro,
            config.estabelecimentos_Numero, config.estabelecimentos_Complemento,
            config.estabelecimentos_Bairro, config.estabelecimentos_Cep, config.estabelecimentos_UF, config.estabelecimentos_municipio,
            config.estabelecimentos_DDD1, config.estabelecimentos_Telefone1, config.estabelecimentos_DDD2,
            config.estabelecimentos_Telefone2, config.estabelecimentos_DDD_Fax, config.estabelecimentos_Fax,
            config.estabelecimentos_correio_eletronico, config.estabelecimentos_situacao_especial,
            config.estabelecimentos_data_situacao_especial
        ))

        # Ordena os dados baseando-se no primeiro campo, CNPJ Consolidado
        dados_completos_estabelecimentos.sort(key=lambda x: x[0])

        # Descompacta os dados ordenados de volta para as listas globais
        (
            config.estabelecimentos_cnpj_consolidado, config.estabelecimentos_Cnpj_Raiz,
            config.estabelecimentos_identificador_matriz_filial, config.estabelecimentos_nome_fantasia,
            config.estabelecimentos_situacao_cadastral, config.estabelecimentos_data_situacao_cadastral,
            config.estabelecimentos_motivo_situacao_cadastral, config.estabelecimentos_cidade_exterior,
            config.estabelecimentos_pais, config.estabelecimentos_data_inicio_atividade,
            config.estabelecimentos_cnae_principal, config.estabelecimentos_cnae_secundario,
            config.estabelecimentos_Tipo_de_Logradouro, config.estabelecimentos_Logradouro,
            config.estabelecimentos_Numero, config.estabelecimentos_Complemento,
            config.estabelecimentos_Bairro, config.estabelecimentos_Cep, config.estabelecimentos_UF, config.estabelecimentos_municipio,
            config.estabelecimentos_DDD1, config.estabelecimentos_Telefone1, config.estabelecimentos_DDD2,
            config.estabelecimentos_Telefone2, config.estabelecimentos_DDD_Fax, config.estabelecimentos_Fax,
            config.estabelecimentos_correio_eletronico, config.estabelecimentos_situacao_especial,
            config.estabelecimentos_data_situacao_especial
        ) = map(list, zip(*dados_completos_estabelecimentos))

    except ValueError as e:
        print(f"Erro ao ordenar estabelecimentos: {e}")
    except Exception as e:
        print(f"Erro inesperado ao ordenar estabelecimentos: {e}")
