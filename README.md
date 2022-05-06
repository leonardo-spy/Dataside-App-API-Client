# Dataside App API Client
## Desafio: Consumo de Dados para Previsão do Tempo das Cidades do Vale do Paraíba
Aplicação em Python integrado ao Apache Spark para realizar acessos em API's para consulta e formatação da previsão do tempo do Vale do Paraíba. A aplicação <b>mantem a base</b> do algoritmo pedido no notebook do Jupyter.
## Proposta
<b>Link da Proposta: [Proposta.md](./proposta.md#avaliação-técnica---python-sql-e-spark)</b>

## O que o projeto contém
- Python integrado ao [Apache Spark](https://spark.apache.org/)
- [Jupyter](https://jupyter.org/) (Jupyter Lab)
- Integraçào com a API da [M3o Weather](https://m3o.com/weather/api#Forecast)
- 
## Instalação
Para rodar o projeto faça essas configurações:
- Clone o projeto (utilizando comando git ou baixando em zip)
- Instale o Python (recomendado versão 3.8)
- Instale a biblioteca que se encontra em requirements
```
python -m pip install -U pip setuptools
python -m pip install --upgrade pip
pip install -r requirements.txt
```
- Configure o main.py substituindo o M3O_API_TOKEN para a sua chave do [M3o](https://m3o.com/account/keys/)
```
M3O_API_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxx"
```

## Resultados & O que é esperado
Arquivos Tabela1 e Tabela2 CSV gerados em `./resultados` seguindo as solicitações da proposta, você pode ver o arquivo do notebook final dentro de resultados como [spark_test.ipynb](./resultados/spark_test.ipynb)