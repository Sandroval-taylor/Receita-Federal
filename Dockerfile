# Use uma imagem base oficial do Python
FROM python:3.9

# Defina o diretório de trabalho
WORKDIR /app

# Copie o arquivo requirements.txt (que vamos criar em seguida)
COPY requirements.txt .

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copie todo o conteúdo do diretório atual para o contêiner
COPY . .

# Exponha a porta 5000, que o Flask usará
EXPOSE 5000

# Defina o comando para rodar a aplicação Flask
CMD ["python", "app.py"]
