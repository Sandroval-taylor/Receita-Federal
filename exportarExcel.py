from flask import send_file
import io
import xlsxwriter
import config
from config import formatar_cnpj 

def export_excel():
    """
    Exportação de Resultados para Excel
    ---
    tags:
      - Exportação
    responses:
      200:
        description: "Arquivo Excel com os resultados exportados"
    """
    if not config.estabelecimentos_Cnpj_Raiz:
        return {"error": "Nenhum dado disponível para exportar. Realize uma pesquisa primeiro."}, 400

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet()

    headers = [
        "Pesquisado", "PF/PJ", "Nome (PF)/Razão Social (PJ)", "CPF/CNPJ", "Relação com o caso",	"Endereços", "QSA: Sócios e administradores",	
        "Data de Nascimento (PF)/Data de Abertura (PJ)", "Idade", "Status Receita", "Data do Status receita", "Nome fantasia (PJ)", "Óbito (PF)", "Nome da Mãe",	
        "Telefones", "E-mails", "CNAE Fiscal (PJ)", "CNAE Secundário (PJ)", "Natureza Jurídica (PJ)", "Capital Social (PJ)"
    ]

    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)

    row = 1
    max_len = len(config.estabelecimentos_Cnpj_Raiz)

    for i in range(max_len):
        formatted_cnpj = formatar_cnpj(config.estabelecimentos_cnpj[i])  # Format the CNPJ
        worksheet.write(row, 11, config.estabelecimentos_nome_fantasia[i])
        worksheet.write(row, 6, config.socios_socios[i])  
        worksheet.write(row, 2, config.empresas_nome[i])
        worksheet.write(row, 19, config.empresas_capital_social[i])    
        worksheet.write(row, 18, config.empresas_natureza_juridica[i])      
        worksheet.write(row, 9, config.estabelecimentos_situacao_cadastral[i])
        worksheet.write(row, 10, config.estabelecimentos_data_situacao_cadastral[i])
        worksheet.write(row, 5, config.estabelecimentos_endereco[i])
        worksheet.write(row, 3, formatted_cnpj)  # Write the formatted CNPJ
        worksheet.write(row, 14, config.estabelecimentos_telefones[i])
        worksheet.write(row, 7, config.estabelecimentos_data_de_inicio_de_atividade[i])
        worksheet.write(row, 16, config.estabelecimentos_cnae_principal[i])
        worksheet.write(row, 17, config.estabelecimentos_cnae_secundario[i])
        worksheet.write(row, 15, config.estabelecimentos_correio_eletronico[i])
        row += 1

    workbook.close()
    output.seek(0)
    return send_file(output, download_name='resultado.xlsx', as_attachment=True)
