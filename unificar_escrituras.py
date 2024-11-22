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
    # Verifica se arquivos foram carregados
    if 'excel_files' not in request.files:
        return jsonify({"message": "Nenhum arquivo foi selecionado."}), 400
    df_list = []
    for file in request.files.getlist('excel_files'):
        filename = secure_filename(file.filename)
        if filename.endswith(('.xlsx', '.xls')):
            # Lê o arquivo e ignora as 6 primeiras linhas; a linha 6 será o cabeçalho
            df = pd.read_excel(file, header=6)
            # Remove espaços extras nos nomes das colunas
            df.columns = df.columns.str.strip()
            df_list.append(df)
    if not df_list:
        return jsonify({"message": "Nenhum arquivo válido encontrado."}), 400
    # Concatena os arquivos em um único DataFrame
    df = pd.concat(df_list, ignore_index=True)
    print("Colunas após concatenação:", df.columns.tolist())
    # Define o nome das colunas desejadas para garantir que elas estejam presentes
    colunas_desejadas = [
        'Partes', 'Cpf/Cnpj', 'Qualidade', 'Ato', 'Natureza do Ato', 
        'Data do Ato', 'Livro', 'Folha', 'Cartório', 'Comarca', 'UF'
    ]
    colunas_faltantes = [coluna for coluna in colunas_desejadas if coluna not in df.columns]
    if colunas_faltantes:
        print(f"Colunas faltantes: {colunas_faltantes}")
        return jsonify({"message": "Estrutura de dados inválida."}), 400
    else:
        print("Todas as colunas esperadas estão presentes.")
    # Unificação das colunas Partes, Cpf/Cnpj e Qualidade em uma única coluna "Partes - Cpf/Cnpj - Qualidade"
    df['Partes - Cpf/Cnpj - Qualidade'] = df.apply(lambda row: f"{row.get('Partes', '')} - {row.get('Cpf/Cnpj', '')} / {row.get('Qualidade', '')}", axis=1)
    print("Colunas após unificação (nova coluna 'Partes - Cpf/Cnpj - Qualidade' adicionada):", df.columns.tolist())
    # Remove as colunas antigas que foram unificadas
    df.drop(columns=['Partes', 'Cpf/Cnpj', 'Qualidade'], inplace=True, errors='ignore')
    print("Colunas após exclusão de 'Partes', 'Cpf/Cnpj' e 'Qualidade':", df.columns.tolist())
    # Reordena as colunas conforme a especificação final, garantindo que todas as colunas estejam presentes
    colunas_final = [
        'Partes - Cpf/Cnpj - Qualidade', 'Ato', 'Natureza do Ato', 'Data do Ato',
        'Livro', 'Folha', 'Cartório', 'Comarca', 'UF'
    ]
    for coluna in colunas_final:
        if coluna not in df.columns:
            df[coluna] = ''  # Adiciona a coluna vazia se estiver ausente
    df = df[colunas_final]
    print("Colunas após reordenação final:", df.columns.tolist())
    # Unificação de linhas duplicadas com base nas colunas 'Livro' e 'Folha' usando concatenação de valores únicos
    def unificar_linhas(series):
        return ' \n '.join(series.dropna().astype(str).unique())
    for coluna in ['Partes - Cpf/Cnpj - Qualidade', 'Ato', 'Natureza do Ato', 'Data do Ato', 'Cartório', 'Comarca']:
        if coluna in df.columns:
            df[coluna] = df.groupby(['Livro', 'Folha'])[coluna].transform(unificar_linhas)
    print("Colunas após unificação de linhas duplicadas:", df.columns.tolist())
    # Remoção de duplicatas com base nas colunas 'Livro' e 'Folha'
    df.drop_duplicates(subset=['Livro', 'Folha'], inplace=True)
    print("Colunas após remoção de duplicatas:", df.columns.tolist())
    # Salva o DataFrame resultante em um arquivo temporário para download
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, header=True)
    output.seek(0)
    return send_file(output, download_name="Unificado.xlsx", as_attachment=True)
