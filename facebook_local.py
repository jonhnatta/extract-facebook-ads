from dotenv import load_dotenv
import json
import os
import requests
from datetime import datetime, time
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    DateType,
    TimestampType,
)

load_dotenv()

api_url = os.getenv("API_URL")
account_id = os.getenv("ACCOUNT_ID")
token = os.getenv("TOKEN")
db_url = os.getenv("DB_URL")
db_table = os.getenv("DB_TABLE")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

schema = StructType(
    [
        StructField("campaign_id", StringType(), False),
        StructField("campaign_name", StringType(), True),
        StructField("source", StringType(), False),
        StructField("faculdade", StringType(), False),
        StructField("impressions", FloatType(), True),
        StructField("reach", FloatType(), True),
        StructField("frequency", FloatType(), True),
        StructField("clicks", FloatType(), True),
        StructField("ctr", FloatType(), True),
        StructField("cpm", FloatType(), True),
        StructField("spend", FloatType(), True),
        StructField("date_start", DateType(), True),
        StructField("date_stop", DateType(), True),
        StructField("offsite_conversion_pos_lead", FloatType(), True),
        StructField("offsite_conversion_pos_contato", FloatType(), True),
        StructField("offsite_conversion_grad_lead", FloatType(), True),
        StructField("offsite_conversion_di_lead", FloatType(), True),
        StructField("offsite_conversion_sg_lead", FloatType(), True),
        StructField("offsite_conversion_tec_lead", FloatType(), True),
        StructField("offsite_conversion_free_lead", FloatType(), True),
        StructField("offsite_conversion_evento_lead", FloatType(), True),
        StructField("offsite_conversion_lead_form", FloatType(), True),
        StructField("offsite_conversion_presencial_lead", FloatType(), True),
        StructField("offsite_conversion_indicacao_lead", FloatType(), True),
        StructField("offsite_conversion_pos_purchase", FloatType(), True),
        StructField("offsite_conversion_pos_initiate_checkout", FloatType(), True),
        StructField("offsite_conversion_grad_purchase", FloatType(), True),
        StructField("offsite_conversion_grad_initiate_checkout", FloatType(), True),
        StructField("offsite_conversion_di_purchase", FloatType(), True),
        StructField("offsite_conversion_di_initiate_checkout", FloatType(), True),
        StructField("offsite_conversion_sg_purchase", FloatType(), True),
        StructField("offsite_conversion_sg_initiate_checkout", FloatType(), True),
        StructField("offsite_conversion_tec_purchase", FloatType(), True),
        StructField("offsite_conversion_tec_initiate_checkout", FloatType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


class Campaign:
    """
    Classe para extrair e processar dados de campanhas do Facebook Ads.
    """

    def __init__(self, account_id, token, api_url):
        """
        Inicializa a classe com as credenciais necessárias.

        Args:
            account_id (str): ID da conta no Facebook Ads.
            token (str): Token de autenticação da API do Facebook.
            api_url (str): URL base da API do Facebook.
        """
        self.account_id = account_id
        self.token = token
        self.api_url = api_url

    def get_active_campaigns(self):
        """
        Obtém todas as campanhas ativas na conta.

        Realiza paginação automática para buscar todas as campanhas disponíveis.

        Returns:
            list: Lista de dicionários com os dados das campanhas ativas.
        """
        campaigns = []

        # URL e cabeçalhos
        url = f'{self.api_url}/{self.account_id}/campaigns?effective_status=["ACTIVE"]'
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        # Parâmetros
        params = {
            "fields": "name,status,objective",
        }

        # Loop para recuperar todas as campanhas
        while True:
            # Requisição
            response = requests.get(url, headers=headers, params=params)

            # Tratar resposta
            if response.status_code == 200:
                data = response.json()
                campaigns.extend(data["data"])

                # Verificar se há mais campanhas
                if "paging" in data and "next" in data["paging"]:
                    # Atualizar parâmetros para próxima página
                    url = data["paging"]["next"]
                else:
                    break
            else:
                print("Erro na requisição:", response.status_code)

        return campaigns

    def get_insights_campaign(self, campaign_id):
        """
        Processa dados de todas as campanhas ativas.

        Este método busca todas as campanhas ativas, coleta seus insights
        e processa os dados em um formato adequado para armazenamento.
        Também extrai e processa dados de conversão para diferentes tipos
        de eventos configurados no pixel do Facebook.

        Returns:
            list: Lista de dicionários com os dados processados de todas as campanhas.
        """
        url = f"{self.api_url}/{campaign_id}/insights"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        time_range = {"since": "2025-04-20", "until": "2025-04-21"}

        # Parâmetros
        params = {
            "time_increment": "1",
            "fields": "impressions,reach,frequency,clicks,ctr,cpm,conversions,spend",
            "time_range": json.dumps(time_range),
        }

        # Requisição e resposta
        response = requests.get(url, headers=headers, params=params)

        # Tratar resposta
        if response.status_code == 200:
            data = response.json()
            return data["data"]
        else:
            print("Erro na requisição:", response)

    def get_insights_active_campaigns(self):
        """
        Processa dados de todas as campanhas ativas.

        Este método busca todas as campanhas ativas, coleta seus insights
        e processa os dados em um formato adequado para armazenamento.
        Também extrai e processa dados de conversão para diferentes tipos
        de eventos configurados no pixel do Facebook.

        Returns:
            list: Lista de dicionários com os dados processados de todas as campanhas.
        """
        data = []

        # Obter IDs das campanhas ativas
        active_campaigns = self.get_active_campaigns()

        # Iterar pelas IDs das campanhas ativas
        for campaign in active_campaigns:
            # Obter insights da campanha
            insights = self.get_insights_campaign(campaign["id"])

            if insights:
                for i in range(len(insights)):
                    # Inicializa um dicionário para armazenar os dados processados
                    processed_data = {
                        "campaign_id": campaign["id"],
                        "campaign_name": campaign["name"],
                        "source": "facebook",
                        "faculdade": "Faculdade",
                        "impressions": float(insights[i].get("impressions", 0.0)),
                        "reach": float(insights[i].get("reach", 0.0)),
                        "frequency": float(insights[i].get("frequency", 0.0)),
                        "clicks": float(insights[i].get("clicks", 0.0)),
                        "ctr": float(insights[i].get("ctr", 0.0)),
                        "cpm": float(insights[i].get("cpm", 0.0)),
                        "spend": float(insights[i].get("spend", 0.0)),
                        "date_start": datetime.strptime(
                            insights[i].get("date_start", "1999-01-01"), "%Y-%m-%d"
                        ).date(),
                        "date_stop": datetime.strptime(
                            insights[i].get("date_stop", "1999-01-01"), "%Y-%m-%d"
                        ).date(),
                    }

                    # Lista com os tipos de conversão que o código irá buscar nos dados da API
                    # Cada item representa um evento de conversão configurado no pixel do Facebook
                    conversion_types = [
                        "pos_lead",
                        "pos_purchase",
                        "pos_initiate_checkout",
                        "grad_purchase",
                        "grad_initiate_checkout",
                        "di_purchase",
                        "di_initiate_checkout",
                        "sg_purchase",
                        "sg_initiate_checkout",
                        "tec_purchase",
                        "tec_initiate_checkout",
                        "lead_form",
                    ]

                    # Verifica se a resposta da API contém dados de conversão para esta campanha
                    # A API do Facebook só retorna o campo "conversions" se houver pelo menos uma conversão
                    if "conversions" in insights[i]:
                        for conversion_type in conversion_types:
                            # Formata o nome do tipo de ação conforme nomenclatura usada pelo Facebook API
                            # Ex: "pos_lead" vira "offsite_conversion.fb_pixel_custom.pos_lead"
                            action_type = (
                                f"offsite_conversion.fb_pixel_custom.{conversion_type}"
                            )
                            # Procura por conversões que correspondam ao tipo atual e soma seus valores
                            processed_data[f"offsite_conversion_{conversion_type}"] = (
                                float(
                                    sum(
                                        float(item["value"])
                                        for item in insights[i]["conversions"]
                                        if item["action_type"] == action_type
                                    )
                                )
                            )
                    else:
                        # Se não houver conversões, define todos os valores de conversão como 0
                        for conversion_type in conversion_types:
                            processed_data[f"offsite_conversion_{conversion_type}"] = (
                                0.0
                            )

                    processed_data["created_at"] = datetime.combine(
                        processed_data["date_start"], time()
                    )
                    data.append(processed_data)

        return data


def main():
    """
    Função principal do script de extração de dados.
    """
    # Inicialize SparkSession
    spark = SparkSession.builder.appName("ExtractCampaignFacebook").getOrCreate()

    # Instancia a classe Campaign
    campaign = Campaign(account_id, token, api_url)

    # Use o metodo da classe para obter dados
    active_campaigns_data = campaign.get_insights_active_campaigns()

    # Converte os dados para DataFrame do Spark
    df = spark.createDataFrame(active_campaigns_data, schema)

    # Configura a conexão JDBC e salva os dados no PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .mode("append") \
        .save()

    print("Finalizado com sucesso!!!")

    # Finalize a sessão Spark
    spark.stop()


if __name__ == "__main__":
    main()
