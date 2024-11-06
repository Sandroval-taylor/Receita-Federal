from flask import Flask, render_template
from flasgger import Swagger
from API import api_pesquisa  # Importe a função de pesquisa consolidada
from cnpj import pesquisar_cnpj
from nome_cpf import pesquisar_nome_cpf
from coringa import pesquisar_coringa
from export_excel import export_excel
from unificar_escrituras import process_excel
import config

app = Flask(__name__)
swagger = Swagger(app)

# Rota principal para carregar a página HTML
@app.route("/")
def index():
    config.limpar_variaveis_globais()  # Limpa as variáveis globais sempre que a página principal é carregada
    return render_template("index.html", **config.template())  # Renderiza a página com variáveis zeradas


app.add_url_rule("/API", "API", api_pesquisa, methods=["POST"])

# Rotas originais de renderização
app.add_url_rule("/cnpj", "cnpj", pesquisar_cnpj, methods=["POST"])
app.add_url_rule("/nome_cpf", "nome_cpf", pesquisar_nome_cpf, methods=["POST"])
app.add_url_rule("/coringa", "coringa", pesquisar_coringa, methods=["POST"])

# Rotas de exportação e processamento de Excel
app.add_url_rule("/export_excel", "export_excel", export_excel, methods=["GET"])

# Rotas de unificar escrituras e procurações
app.add_url_rule("/process_excel", "process_excel", process_excel, methods=["POST"])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
