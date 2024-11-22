from flask import send_file
import io
import xlsxwriter
import config

from flask import send_file
import io
import xlsxwriter
import config

def export_excel():
    """
    Exportação de Resultados para Excel
    ---
    tags:
      - Exportação
    responses:
      200:
        description: "Arquivo Excel com os resultados exportados"
        content:
          application/vnd.openxmlformats-officedocument.spreadsheetml.sheet:
            schema:
              type: string
              format: binary
      400:
        description: "Erro: Nenhum dado disponível para exportar"
    """
    # Verificar se os dados estão disponíveis
    if not config.estabelecimentos_Cnpj_Raiz:
        return {"error": "Nenhum dado disponível para exportar. Realize uma pesquisa primeiro."}, 400

    # Criar arquivo Excel na memória
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet()

    # Cabeçalhos
    headers = [
        'Identificador Matriz/Filial', 'Nome da Empresa', 'CNPJ Completo', 'Endereço', 'Sócios',
        'Data de Início de Atividade', 'Situação Cadastral', 'Data Situação Cadastral', 'Nome Fantasia',
        'Telefones', 'Correio Eletrônico', 'CNAE Principal', 'CNAE Secundário', 'Natureza Jurídica',
        'Capital Social', 'Porte', 'CNPJ Raiz', 'País dos Sócios', 'Representante Legal',
        'Nome do Representante', 'Qualificação do Representante', 'Motivo Situação Cadastral',
        'Cidade no Exterior', 'País dos Estabelecimentos', 'Situação Especial', 'Data Situação Especial',
        'Ente Federativo', 'Qualificação do Responsável'
    ]

    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)

    # Preencher os dados
    row = 1  # Começar na segunda linha, já que a primeira é dos cabeçalhos
    # Garantir que todos os dados estejam preenchidos corretamente antes de escrever no Excel
    for i in range(len(config.estabelecimentos_Cnpj_Raiz)):  # Iterar sobre os estabelecimentos
        worksheet.write(row, 0, config.estabelecimentos_identificador_matriz_filial[i])  # Identificador Matriz/Filial
        worksheet.write(row, 1, config.empresas_nome[i] if i < len(config.empresas_nome) else '')  # Nome da Empresa
        # Corrigir o formato do CNPJ
        cnpj = config.estabelecimentos_cnpj[i]
        cnpj_formatado = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}" if len(cnpj) == 14 else cnpj
        worksheet.write(row, 2, cnpj_formatado)  # CNPJ Completo
        worksheet.write(row, 3, config.estabelecimentos_endereco[i])  # Endereço
        worksheet.write(row, 4, config.socios_socios[i] if i < len(config.socios_socios) else '')  # Sócios
        worksheet.write(row, 5, config.estabelecimentos_data_de_inicio_de_atividade[i])  # Data de Início de Atividade
        worksheet.write(row, 6, config.estabelecimentos_situacao_cadastral[i])  # Situação Cadastral
        worksheet.write(row, 7, config.estabelecimentos_data_situacao_cadastral[i])  # Data Situação Cadastral
        worksheet.write(row, 8, config.estabelecimentos_nome_fantasia[i])  # Nome Fantasia
        worksheet.write(row, 9, config.estabelecimentos_telefones[i])  # Telefones
        worksheet.write(row, 10, config.estabelecimentos_correio_eletronico[i])  # Correio Eletrônico
        worksheet.write(row, 11, config.estabelecimentos_cnae_principal[i])  # CNAE Principal
        worksheet.write(row, 12, config.estabelecimentos_cnae_secundario[i])  # CNAE Secundário
        worksheet.write(row, 13, config.empresas_natureza_juridica[i] if i < len(config.empresas_natureza_juridica) else '')  # Natureza Jurídica
        worksheet.write(row, 14, config.empresas_capital_social[i] if i < len(config.empresas_capital_social) else '')  # Capital Social
        worksheet.write(row, 15, config.empresas_porte[i] if i < len(config.empresas_porte) else '')  # Porte
        worksheet.write(row, 16, config.estabelecimentos_Cnpj_Raiz[i])  # CNPJ Raiz
        worksheet.write(row, 17, config.socios_pais[i] if i < len(config.socios_pais) else '')  # País dos Sócios
        worksheet.write(row, 18, config.socios_repre_legal[i] if i < len(config.socios_repre_legal) else '')  # Representante Legal
        worksheet.write(row, 19, config.socios_nome_repre[i] if i < len(config.socios_nome_repre) else '')  # Nome do Representante
        worksheet.write(row, 20, config.socios_quali_repre[i] if i < len(config.socios_quali_repre) else '')  # Qualificação do Representante
        worksheet.write(row, 21, config.estabelecimentos_motivo_situacao_cadastral[i])  # Motivo Situação Cadastral
        worksheet.write(row, 22, config.estabelecimentos_cidade_exterior[i])  # Cidade no Exterior
        worksheet.write(row, 23, config.estabelecimentos_pais[i])  # País dos Estabelecimentos
        worksheet.write(row, 24, config.estabelecimentos_situacao_especial[i])  # Situação Especial
        worksheet.write(row, 25, config.estabelecimentos_data_situacao_especial[i])  # Data Situação Especial
        worksheet.write(row, 26, config.empresas_ente_federativo[i] if i < len(config.empresas_ente_federativo) else '')  # Ente Federativo
        worksheet.write(row, 27, config.empresas_quali_responsavel[i] if i < len(config.empresas_quali_responsavel) else '')  # Qualificação do Responsável
        row += 1  # Passa para a próxima linha após cada estabelecimento
    workbook.close()
    output.seek(0)
    return send_file(output, download_name='resultado.xlsx', as_attachment=True)