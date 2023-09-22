
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import requests
import json
import openai


# Configuração do Spark
spark = SparkSession.builder.appName("Pipeline IA GEN").getOrCreate()

# Conexões e chaves de API (mova isso para variáveis de ambiente ou configuração externa)
openai_api_key = "your_openai_api_key"
sdw2023_api_url = "https://sdw-2023-prd.up.railway.app"

# Esquema do DataFrame
arqSchema = "UserID INT"
df_sdw_desafio = spark.read.csv(
    "C:/Users/marlo/Downloads/santander_dev_week_2023.csv",
    header=False,
    schema=arqSchema,
)
user_ids = df_sdw_desafio.select("UserID")

# Função para buscar usuário na API
def get_user(id):
    try:
        response = requests.get(f"{sdw2023_api_url}/users/{id}")
        response.raise_for_status()  # Lida com erros HTTP
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar usuário {id}: {e}")
        return None

# Busca os usuários de forma paralela
users = [user for id in user_ids if (user := get_user(id)) is not None]

# Função para gerar notícias com base em AI
def generate_ai_news(user):
    try:
        completion = openai.Completion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Você é um especialista em marketing bancário!"},
                {"role": "user", "content": f"Crie uma mensagem para {user['name']}"},
            ],
            api_key=openai_api_key,
        )
        return completion.choices[0].message.content.strip('"')
    except Exception as e:
        print(f"Erro ao gerar notícia para {user['name']}: {e}")
        return ""

# Atualiza os usuários
def user_update(user):
    try:
        response = requests.put(f"{sdw2023_api_url}/users/{user['id']}", json=user)
        response.raise_for_status()  # Lida com erros HTTP
        return True
    except requests.exceptions.RequestException as e:
        print(f"Erro ao atualizar usuário {user['name']}: {e}")
        return False

# Gera notícias e atualiza os usuários
for user in users:
    news = generate_ai_news(user)
    print(f"Notícia gerada para {user['name']}: {news}")

    if news:
        user["news"].append({
            "icon": "https://digitalinnovationone.github.io/santander-dev-week-2023-api/icons/credit.svg",
            "description": news,
        })

    success = user_update(user)
    print(f"Usuário {user['name']} atualizado? {'Sim' if success else 'Não'}")

# Encerra a sessão do Spark
spark.stop()
