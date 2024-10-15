# Usar uma imagem base do Python
FROM python:3.10-slim


# Definir o diretório de trabalho no container
WORKDIR /app

# Copiar as dependências
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copiar o código da aplicação Flask
COPY . .

# Expor a porta 5000 (ou outra porta que você está usando)
EXPOSE 5000

# Comando para rodar a aplicação
CMD ["python", "app.py"]
