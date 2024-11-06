import config

def pesquisar_cnpj_raiz(cursor, cnpj_r):# Função principal para chamar as pesquisas nas tabelas    
    pesquisar_cnpj_raiz_socios(cursor, cnpj_r)
    pesquisar_cnpj_raiz_empresas(cursor, cnpj_r)
    pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_r)
def pesquisar_cnpj_raiz_socios(cursor, cnpj_r):    
    try:# Consulta na tabela de Sócios
        for i in range(10):  # Executa a consulta na tabela de sócios
            tabela_socios = f"socios{i}"
            query_socios = f"SELECT * FROM \"public\".\"{tabela_socios}\" WHERE \"Cnpj Raiz\" = %s"
            cursor.execute(query_socios, (cnpj_r,))
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:                  
                    if config.pesquisa_por_cnpj:  #  # Verificar duplicação ou replicar conforme o tipo de pesquisa Replicar os dados para todas as linhas se a pesquisa for por CNPJ
                        config.socios_Cnpj_Raiz.append(linha[0])
                        config.socios_pais.append(linha[1])
                        config.socios_repre_legal.append(linha[2])
                        config.socios_nome_repre.append(linha[3])
                        config.socios_quali_repre.append(linha[4])
                        config.socios_socios.append(linha[5])
                    elif linha[0] not in config.socios_Cnpj_Raiz:  # Evitar duplicação em outras pesquisas
                        config.socios_Cnpj_Raiz.append(linha[0])
                        config.socios_pais.append(linha[1])
                        config.socios_repre_legal.append(linha[2])
                        config.socios_nome_repre.append(linha[3])
                        config.socios_quali_repre.append(linha[4])
                        config.socios_socios.append(linha[5])
                break  # Para após o primeiro resultado encontrado
    except Exception as e:
        print(f"Erro durante a consulta de sócios: {e}")
def pesquisar_cnpj_raiz_empresas(cursor, cnpj_r):
    try:# Consulta na tabela de Empresas
        for i in range(10):  # Executa a consulta na tabela de empresas
            tabela_empresas = f"empresas{i}"
            query_empresas = f"SELECT * FROM \"public\".\"{tabela_empresas}\" WHERE \"Cnpj Raiz\" = %s"
            cursor.execute(query_empresas, (cnpj_r,))
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:
                    if config.pesquisa_por_cnpj:  # Verificar duplicação ou replicar conforme o tipo de pesquisa Replicar os dados para todas as linhas se a pesquisa for por CNPJ
                        config.empresas_Cnpj_Raiz.append(linha[0])
                        config.empresas_nome.append(linha[1])
                        config.empresas_capital_social.append(linha[2])
                        config.empresas_ente_federativo.append(linha[3])
                        config.empresas_quali_responsavel.append(linha[4])
                        config.empresas_natureza_juridica.append(linha[5])
                        config.empresas_porte.append(linha[6])
                    elif linha[0] not in config.empresas_Cnpj_Raiz:  # Evitar duplicação em outras pesquisas
                        config.empresas_Cnpj_Raiz.append(linha[0])
                        config.empresas_nome.append(linha[1])
                        config.empresas_capital_social.append(linha[2])
                        config.empresas_ente_federativo.append(linha[3])
                        config.empresas_quali_responsavel.append(linha[4])
                        config.empresas_natureza_juridica.append(linha[5])
                        config.empresas_porte.append(linha[6])
                break  # Para após o primeiro resultado encontrado
    except Exception as e:
        print(f"Erro durante a consulta de empresas: {e}")
def pesquisar_cnpj_raiz_estabelecimentos(cursor, cnpj_r):# Consulta na tabela de Estabelecimentos 
    try:
        for i in range(10):  # Executa a consulta na tabela de estabelecimentos e ordena pelo CNPJ completo
            tabela_estabelecimentos = f"estabelecimentos{i}"
            query_estabelecimentos = f"""
            SELECT * FROM \"public\".\"{tabela_estabelecimentos}\" WHERE \"Cnpj Raiz\" = %s ORDER BY "Cnpj"
            """
            cursor.execute(query_estabelecimentos, (cnpj_r,))
            resultados = cursor.fetchall()
            if resultados:
                for linha in resultados:
                    config.estabelecimentos_Cnpj_Raiz.append(linha[0])
                    config.estabelecimentos_identificador_matriz_filial.append(linha[1])
                    config.estabelecimentos_nome_fantasia.append(linha[2])
                    config.estabelecimentos_situacao_cadastral.append(linha[3])
                    config.estabelecimentos_data_situacao_cadastral.append(linha[4])
                    config.estabelecimentos_motivo_situacao_cadastral.append(linha[5])
                    config.estabelecimentos_cidade_exterior.append(linha[6])
                    config.estabelecimentos_pais.append(linha[7])
                    config.estabelecimentos_data_de_inicio_de_atividade.append(linha[8])
                    config.estabelecimentos_cnae_principal.append(linha[9])
                    config.estabelecimentos_cnae_secundario.append(linha[10])
                    config.estabelecimentos_correio_eletronico.append(linha[11])
                    config.estabelecimentos_situacao_especial.append(linha[12])
                    config.estabelecimentos_data_situacao_especial.append(linha[13])
                    config.estabelecimentos_cnpj.append(linha[14])  # Coluna CNPJ completo
                    config.estabelecimentos_endereco.append(linha[15])
                    config.estabelecimentos_telefones.append(linha[16])
                    if config.pesquisa_por_cnpj:  # Só replicar na pesquisa por CNPJ, Replicar dados de sócios
                        config.socios_Cnpj_Raiz.append(config.socios_Cnpj_Raiz[-1] if config.socios_Cnpj_Raiz else None)
                        config.socios_pais.append(config.socios_pais[-1] if config.socios_pais else None)
                        config.socios_repre_legal.append(config.socios_repre_legal[-1] if config.socios_repre_legal else None)
                        config.socios_nome_repre.append(config.socios_nome_repre[-1] if config.socios_nome_repre else None)
                        config.socios_quali_repre.append(config.socios_quali_repre[-1] if config.socios_quali_repre else None)
                        config.socios_socios.append(config.socios_socios[-1] if config.socios_socios else None)
                        # Replicar dados de empresas
                        config.empresas_Cnpj_Raiz.append(config.empresas_Cnpj_Raiz[-1] if config.empresas_Cnpj_Raiz else None)
                        config.empresas_nome.append(config.empresas_nome[-1] if config.empresas_nome else None)
                        config.empresas_capital_social.append(config.empresas_capital_social[-1] if config.empresas_capital_social else None)
                        config.empresas_ente_federativo.append(config.empresas_ente_federativo[-1] if config.empresas_ente_federativo else None)
                        config.empresas_quali_responsavel.append(config.empresas_quali_responsavel[-1] if config.empresas_quali_responsavel else None)
                        config.empresas_natureza_juridica.append(config.empresas_natureza_juridica[-1] if config.empresas_natureza_juridica else None)
                        config.empresas_porte.append(config.empresas_porte[-1] if config.empresas_porte else None)    
        ordenar_dados() # Ordenar os dados pela lista de CNPJ
    except Exception as e:
        print(f"Erro durante a consulta de estabelecimentos: {e}")
def ordenar_dados():
    dados_completos = list(zip(# Função para ordenar todas as listas com base na lista de estabelecimentos_cnpj
        config.estabelecimentos_cnpj, config.estabelecimentos_Cnpj_Raiz, config.estabelecimentos_identificador_matriz_filial,
        config.estabelecimentos_nome_fantasia, config.estabelecimentos_situacao_cadastral, config.estabelecimentos_data_situacao_cadastral,
        config.estabelecimentos_motivo_situacao_cadastral, config.estabelecimentos_cidade_exterior, config.estabelecimentos_pais,
        config.estabelecimentos_data_de_inicio_de_atividade, config.estabelecimentos_cnae_principal, config.estabelecimentos_cnae_secundario,
        config.estabelecimentos_correio_eletronico, config.estabelecimentos_situacao_especial, config.estabelecimentos_data_situacao_especial,
        config.estabelecimentos_endereco, config.estabelecimentos_telefones
    ))
    dados_completos.sort(key=lambda x: x[0])# Ordenar os dados com base no CNPJ (primeiro campo)
    (   config.estabelecimentos_cnpj, config.estabelecimentos_Cnpj_Raiz, config.estabelecimentos_identificador_matriz_filial, # Desempacotar os dados de volta para as listas globais, agora ordenados
        config.estabelecimentos_nome_fantasia, config.estabelecimentos_situacao_cadastral, config.estabelecimentos_data_situacao_cadastral,
        config.estabelecimentos_motivo_situacao_cadastral, config.estabelecimentos_cidade_exterior, config.estabelecimentos_pais,
        config.estabelecimentos_data_de_inicio_de_atividade, config.estabelecimentos_cnae_principal, config.estabelecimentos_cnae_secundario,
        config.estabelecimentos_correio_eletronico, config.estabelecimentos_situacao_especial, config.estabelecimentos_data_situacao_especial,
        config.estabelecimentos_endereco, config.estabelecimentos_telefones
    ) = map(list, zip(*dados_completos))
