from flask import Flask, render_template
from flasgger import Swagger
from flask_cors import CORS
from API import api_pesquisa
from cnpj import pesquisar_cnpj
from nome_cpf import pesquisar_nome_cpf
from coringa import pesquisar_coringa
from export_excel import export_excel
from unificar_escrituras import process_excel
import config

# Inicialização da aplicação Flask
app = Flask(__name__)

# Configuração de CORS e Swagger
CORS(app)  # Ativa o CORS para todas as rotas
swagger = Swagger(app)

# Rota principal para carregar a página HTML
@app.route("/")
def index():
    # Limpa variáveis globais ao carregar a página principal
    config.limpar_variaveis_globais()
    # Renderiza a página com variáveis zeradas
    return render_template("index.html", **config.template())

# Rotas da API
app.add_url_rule("/API", "API", api_pesquisa, methods=["POST"])

# Rotas específicas de pesquisa
app.add_url_rule("/cnpj", "cnpj", pesquisar_cnpj, methods=["POST"])
app.add_url_rule("/nome_cpf", "nome_cpf", pesquisar_nome_cpf, methods=["POST"])
app.add_url_rule("/coringa", "coringa", pesquisar_coringa, methods=["POST"])

# Rotas de exportação e processamento
app.add_url_rule("/export_excel", "export_excel", export_excel, methods=["GET"])
app.add_url_rule("/process_excel", "process_excel", process_excel, methods=["POST"])

# Inicialização do servidor
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
