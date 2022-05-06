import findspark
findspark.init()

import requests
import json
import unidecode
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# Buscar cidades do Vale do Paraíba
cities = {}
IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes/3513/municipios"

response = requests.get("{}".format(IBGE_URL))
json_response = response.json()
for city in json_response:
      city['nome'] = unidecode.unidecode(city['nome'])

# Criar data frame com as cidades
cities_raw = spark.sparkContext.parallelize([json_response])
cities = spark.read.json(cities_raw)

# Criar view com as cidades
cities.createOrReplaceTempView("cities")

# Buscar previsão do tempo para as cidades
M3O_API_TOKEN="xxxxxxxxxxxxxxxxxxxxxxx"
SEARCH_CITY = "https://api.m3o.com/v1/weather/Forecast"

session = requests.Session()
session.headers = {"Content-Type":"application/json","Authorization": f"Bearer {M3O_API_TOKEN}"}
cidades =  []
for cidade in cities.collect():
      while True:            
            response = session.post(SEARCH_CITY,json={"days": 5, "location" : cidade[2]})
            if response.status_code == 200:
                  probabily_cities = response.json() 
                  for x in probabily_cities['forecast']:
                        if x['will_it_rain'] == True:
                              x['will_it_rain'] = 'Sim'
                        else:
                              x['will_it_rain'] = 'Não'
                  cidades.append(probabily_cities)
                  break
            elif (response.status_code == 500):
                  break # cidade não existe no M3o
            else:
                  continue


# Criar data frame com as previsões
forecast_raw = spark.sparkContext.parallelize(cidades)
forecast = spark.read.json(forecast_raw)
forecast = forecast.filter(forecast['region'] == "Sao Paulo")

# Criar view com as previsões
forecast.createOrReplaceTempView("forecast")

# Criar DF da Tabela 1
SQL_TABLE_1 = "SELECT f.location AS Cidade,cities.id AS CodigoDaCidade,f.date AS Data,f.region AS Regiao,'Brasil' AS Pais,f.latitude AS Latitude,f.longitude AS Longigute,f.max_temp_c AS TemperaturaMaxima,f.min_temp_c AS TemperaturaMinima,f.avg_temp_c AS TemperaturaMedia,f.will_it_rain AS VaiChover,CONCAT(f.chance_of_rain,'%') AS ChanceDeChuva,f.condition AS CondicaoDoTempo,f.sunrise AS NascerDoSol,f.sunset AS PorDoSol,f.max_wind_kph AS VelocidadeMaximaDoVento FROM (SELECT f.location,f.local_time,f.region,f.latitude,f.longitude,inline(f.forecast) FROM forecast AS f) AS f INNER JOIN cities on cities.nome = f.location"
tabela_1 = spark.sql(SQL_TABLE_1)# Longigute é o solicitado no notebook

# Criar DF da Tabela 2
SQL_TABLE_2 = "SELECT f.location AS Cidade, SUM(if(f.will_it_rain = 'Sim', 1, 0)) AS QtdDiasVaiChover, SUM(if(f.will_it_rain = 'Não', 1, 0)) AS QtdDiasNaoVaiChover, COUNT(f.location) AS TotalDiasMapeados FROM (SELECT f.location,inline(f.forecast) FROM forecast AS f) AS f GROUP BY f.location"
tabela_2 = spark.sql(SQL_TABLE_2)

# Exportar CSVs
tabela_1.coalesce(1).write.csv("/resultados/tabela1", mode="append", header="true",sep=';',encoding='ISO-8859-1')#coalesce(1) para deixar em um unico csv final
tabela_2.coalesce(1).write.csv("/resultados/tabela2", mode="append", header="true",sep=';',encoding='ISO-8859-1')
