import os
from pathlib import Path

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from dotenv import load_dotenv
from openai import AzureOpenAI

from tools.openai_helper import OpenAIHelper

backend_dir = Path(__file__).parent.parent


class AzureEnv:
    def __init__(self, stage="dev", brand=""):
        self.stage = stage
        self.brand = brand

        if stage == "prod":
            load_dotenv(os.path.join(backend_dir, ".env.prod"))
        else:
            load_dotenv(os.path.join(backend_dir, ".env.dev"))

        self.AZURE_SEARCH_SERVICE = os.environ.get("AZURE_SEARCH_SERVICE")
        self.INDEX_NAME = os.environ.get(f"{brand.upper()}_AZURE_SEARCH_INDEX", "clo3d-index-english")

        self.SEARCH_CLIENT_ENDPOINT = f"https://{self.AZURE_SEARCH_SERVICE}.search.windows.net"
        self.AZURE_KEY_CREDENTIAL = AzureKeyCredential(os.environ.get("AZURE_SEARCH_KEY"))

        self.search_client = SearchClient(
            endpoint=f"https://{self.AZURE_SEARCH_SERVICE}.search.windows.net",
            index_name=self.INDEX_NAME,
            credential=self.AZURE_KEY_CREDENTIAL,
        )

        self.search_index_client = SearchIndexClient(
            endpoint=f"https://{self.AZURE_SEARCH_SERVICE}.search.windows.net", credential=self.AZURE_KEY_CREDENTIAL
        )

        self.AZURE_OPENAI_SERVICE = os.environ.get("AZURE_OPENAI_SERVICE")
        self.AZURE_OPENAI_CHATGPT_DEPLOYMENT = os.environ.get("AZURE_OPENAI_CHATGPT_DEPLOYMENT")
        self.AZURE_OPENAI_EMB_DEPLOYMENT = os.environ.get("AZURE_OPENAI_EMB_DEPLOYMENT")
        self.openai_client = AzureOpenAI(
            api_version="2023-07-01-preview",
            azure_endpoint=f"https://{self.AZURE_OPENAI_SERVICE}.openai.azure.com",
            api_key=os.environ.get("AZURE_OPENAI_KEY"),
        )

        self.URI = os.environ.get("MONGO_URI")
        self.DB_NAME = os.environ.get(f"{self.brand.upper()}_MONGO_DB_NAME", "clo3d-dev")
        self.COLLECTION_NAME = os.environ.get("MONGO_COLLECTION_CHATHISTORY")
        self.COLLECTION_USERS = os.environ.get("MONGO_COLLECTION_USERS")
        self.COLLECTION_ARTICLE = os.environ.get("MONGO_COLLECTION_ARTICLES")
        self.COLLECTION_FEEDBACK = os.environ.get("MONGO_COLLECTION_FEEDBACK")

        self.openai_helper = OpenAIHelper(
            self.openai_client,
            self.AZURE_OPENAI_CHATGPT_DEPLOYMENT,
            self.AZURE_OPENAI_EMB_DEPLOYMENT,
        )
