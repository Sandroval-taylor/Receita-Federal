from flask import request, send_file, jsonify
import pandas as pd
import io
from werkzeug.utils import secure_filename

def process_excel():
    """
    Processamento e Formatação de Arquivo Excel
    ---
    tags:
      - Processamento
    consumes:
      - multipart/form-data
    parameters:
      - name: excel_files
        in: formData
        type: array
        items:
          type: file
        required: true
        description: "Um ou mais arquivos Excel (.xlsx ou .xls) para processar e formatar"
    responses:
      200:
        description: "Arquivo Excel formatado e unificado"
        content:
          application/vnd.openxmlformats-officedocument.spreadsheetml.sheet:
            schema:
              type: string
              format: binary
      400:
        description: "Erro: Nenhum arquivo válido encontrado ou estrutura inválida"
    """
    # Implementação da função process_excel
    if 'excel_files' not in request.files:
        return jsonify({"message": "Nenhum arquivo foi selecionado."}), 400
    df_list = []
    for file in request.files.getlist('excel_files'):
        filename = secure_filename(file.filename)
        if filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, header=None)
            df = df.iloc[6:]  # Exclui as 6 primeiras linhas
            df_list.append(df)
    if not df_list:
        return jsonify({"message": "Nenhum arquivo válido encontrado."}), 400
    
    df = pd.concat(df_list, ignore_index=True)
    if df.shape[1] > 8:# Unifica as colunas 6, 7 e 8, separando os valores com "-"
        df[6] = df.apply(lambda row: ' - '.join([str(row.iloc[6]), str(row.iloc[7]), str(row.iloc[8])]), axis=1)
        df.drop(columns=[5, 7, 8], inplace=True)
    else:
        return jsonify({"message": "Estrutura do arquivo inválida."}), 400
    def unificar_linhas(group):# Define a função para unificar as linhas agrupadas
        return ' \n '.join(group.astype(str))
    condicao = df[3].duplicated(keep=False) & df[4].duplicated(keep=False)# Condição para detectar duplicidade nas colunas 3 e 4
    df[6] = df.groupby([3, 4])[6].transform(lambda x: unificar_linhas(x) if condicao.any() else '')
    df.drop_duplicates(subset=[3, 4], keep='first', inplace=True)# Remove as linhas duplicadas com base nas colunas 3 e 4
    coluna_6 = df.pop(6)
    df.insert(0, 6, coluna_6)# Move a coluna 6 para a primeira posição
    df[6] = df[6].apply(lambda x: ' \n '.join(pd.Series(str(x).split(' \n ')).unique()) if pd.notnull(x) else '')# Remove valores duplicados em cada célula da primeira coluna
    df[3] = pd.to_datetime(df[3], errors='coerce')# Converte a coluna 3 para o tipo de dados datetime
    df = df.sort_values(by=[3], ascending=False)# Ordena o DataFrame com base na coluna 3 em ordem decrescente
    output = io.BytesIO()# Salva o DataFrame resultante em um arquivo temporário para download
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, header=False)
    output.seek(0)
    return send_file(output, download_name="Unificado.xlsx", as_attachment=True)