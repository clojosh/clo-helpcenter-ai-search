import os
from pathlib import Path

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from dotenv import load_dotenv
from openai import AzureOpenAI

from tools.openai_helper import OpenAIHelper


class AzureEnv:
    backend_dir = Path(__file__).parent.parent
    ai_search_dir = str(Path(__file__).parent.parent)
    zendesk_article_api_endpoint = "https://{0}.zendesk.com/api/v2/help_center/en-us/articles/{1}"
    zendesk_articles_api_endpoint = (
        "https://{0}.zendesk.com/api/v2/help_center/en-us/articles.json?page={1}&per_page=30&sort_by=updated_at&sort_order=desc"
    )
    zendesk_article_section_api_endpoint = "https://{0}.zendesk.com/api/v2/help_center/en-us/sections/{1}.json"
    zendesk_article_category_api_endpoint = "https://{0}.zendesk.com/api/v2/help_center/en-us/categories/{1}.json"
    zendesk_article_attachment_api_endpoint = "https://support.{0}.com/api/v2/help_center/{1}/articles/{2}/attachments"

    def __init__(self, stage="dev", brand="", language="English"):
        self.stage = stage
        self.brand = brand
        self.language = language
        self.locale = self.get_locale(language)

        if self.stage == "prod":
            load_dotenv(os.path.join(self.backend_dir, ".env.prod"))
        else:
            load_dotenv(os.path.join(self.backend_dir, ".env.dev"))

        self.AZURE_SEARCH_SERVICE = os.environ.get("AZURE_SEARCH_SERVICE")
        self.INDEX_NAME = os.environ.get(f"{self.brand.upper()}_AZURE_SEARCH_INDEX", "clo3d-index-english")

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
            language=language,
        )

    def get_locale(self, language="English"):
        locale = {"English": "en-us", "Espanol": "es", "Japanese": "ja", "Korean": "ko", "Portuguese": "pt-br", "Chinese": "zh-cn", "Taiwanese": "tw"}
        return locale[language]

    def get_locale_path(self, type="articles") -> str:
        document_path = {
            "English": os.path.join(self.ai_search_dir, self.brand, type, "en-us"),
            "Espanol": os.path.join(self.ai_search_dir, self.brand, type, "es"),
            "Japanese": os.path.join(self.ai_search_dir, self.brand, type, "ja"),
            "Korean": os.path.join(self.ai_search_dir, self.brand, type, "ko"),
            "Portuguese": os.path.join(self.ai_search_dir, self.brand, type, "pt-br"),
            "Chinese": os.path.join(self.ai_search_dir, self.brand, type, "zh-cn"),
            "Taiwanese": os.path.join(self.ai_search_dir, self.brand, type, "tw"),
        }

        os.makedirs(document_path[self.language], exist_ok=True)

        return document_path[self.language]

    def get_zendesk_article_api_endpoint(self, article_id):
        """Returns a formatted zendesk API endpoint for one article"""

        if self.brand == "closet":
            return AzureEnv.zendesk_article_api_endpoint.format("clo-set", article_id)
        elif self.brand == "md":
            return AzureEnv.zendesk_article_api_endpoint.format("marvelousdesigner", article_id)
        elif self.brand == "clovf":
            return AzureEnv.zendesk_article_api_endpoint.format("clovf", article_id)

        return AzureEnv.zendesk_article_api_endpoint.format(self.brand, article_id)

    def get_zendesk_articles_api_endpoint(self, page):
        """Returns a formatted zendesk API endpoint for getting articles"""

        if self.brand == "closet":
            return AzureEnv.zendesk_articles_api_endpoint.format("clo-set", page)
        elif self.brand == "md":
            return AzureEnv.zendesk_articles_api_endpoint.format("marvelousdesigner", page)
        elif self.brand == "clovf":
            return AzureEnv.zendesk_articles_api_endpoint.format("clovf", page)

        return AzureEnv.zendesk_articles_api_endpoint.format(self.brand, page)

    def get_zendesk_article_attachment_api_endpoint(self, article_id):
        if self.brand == "closet":
            return AzureEnv.zendesk_article_attachment_api_endpoint.format("clo-set", self.locale, article_id)
        elif self.brand == "md":
            return AzureEnv.zendesk_article_attachment_api_endpoint.format("marvelousdesigner", self.locale, article_id)

        return AzureEnv.zendesk_article_attachment_api_endpoint.format(self.brand, self.locale, article_id)

    def get_zendesk_article_section_api_endpoint(self, section_id):
        if self.brand == "closet":
            return AzureEnv.zendesk_article_section_api_endpoint.format("clo-set", section_id)
        elif self.brand == "md":
            return AzureEnv.zendesk_article_section_api_endpoint.format("marvelousdesigner", section_id)

        return AzureEnv.zendesk_article_section_api_endpoint.format(self.brand, section_id)

    def get_zendesk_article_category_api_endpoint(self, category_id):
        if self.brand == "closet":
            return AzureEnv.zendesk_article_category_api_endpoint.format("clo-set", category_id)
        elif self.brand == "md":
            return AzureEnv.zendesk_article_category_api_endpoint.format("marvelousdesigner", category_id)

        return AzureEnv.zendesk_article_category_api_endpoint.format(self.brand, category_id)
