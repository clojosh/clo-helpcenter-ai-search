import csv
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path

import questionary
import requests
from azure.search.documents.indexes.models import (
    ExhaustiveKnnAlgorithmConfiguration,
    ExhaustiveKnnParameters,
    HnswAlgorithmConfiguration,
    HnswParameters,
    SearchableField,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SearchSuggester,
    SemanticConfiguration,
    SemanticField,
    SemanticPrioritizedFields,
    SemanticSearch,
    SimpleField,
    VectorSearch,
    VectorSearchAlgorithmKind,
    VectorSearchAlgorithmMetric,
    VectorSearchProfile,
)
from tqdm import tqdm

from tools.azure_env import AzureEnv

backend_dir = Path(__file__).parent


class AISearch:
    def __init__(self, azure_env: AzureEnv):
        self.azure_env = azure_env
        self.search_client = azure_env.search_client
        self.search_index_client = azure_env.search_index_client
        self.openai_helper = azure_env.openai_helper

    def text_search(self, text):
        results = self.search_client.search(search_text=text)

        results_list = list(results)

        for i, result in enumerate(results_list):
            print(f"\nTitle:\n{result['Title']}")
            print(f"Source:\n{result['Source']}\n")

    def vector_search(self, query, k=1, print_results=False):
        results = self.search_client.search(
            search_text=None,
            vector=self.openai_helper.generate_embeddings(query),
            vector_fields="TitleVector,ContentVector",
            select=["Title", "Content", "Source"],
            top=k,
        )

        results_list = list(results)

        for i, result in enumerate(results_list):
            results_list[i]["@search.score"] = result["@search.score"] * 100

            if print_results:
                print(f"\nTitle:\n{result['Title']}")
                print(f"Score:\n{result['@search.score']}")
                print(f"Source:\n{result['Source']}\n")

        # print('Results:\n',results_list)

        return results_list

    def hybrid_search(self, query, k=3, print_results=False):
        results = self.search_client.search(
            search_text=query,
            vector=self.openai_helper.generate_embeddings(query),
            vector_fields="TitleVector,ContentVector",
            top=k,
        )

        results_list = list(results)

        for i, result in enumerate(results_list):
            print(f"\nTitle: {result['Title']}")
            print(f"Source:\n{result['Source']}")
            print(f"Labels:\n{result['Labels']}")

        return results_list

    def semantic_vector_search(self, query, k=1, print_results=False):
        results = self.search_client.search(
            search_text=query,
            vector=VectorizedQuery(
                value=self.openai_helper.generate_embeddings(query),
                k=k,
                fields="titleVector,contentVector",
            ),
            select=["Title", "Content", "Source"],
            query_type="semantic",
            query_language="en-us",
            semantic_configuration_name="vector-semantic-config",
            query_caption="extractive",
            query_answer="extractive",
            top=k,
        )

        results_list = list(results)

        # semantic_answers = results.get_answers()
        # for answer in semantic_answers:
        #     if answer.highlights:
        #         print(f"Semantic Answer: {answer.highlights}")
        #     else:
        #         print(f"Semantic Answer: {answer.text}")
        #     print(f"Semantic Answer Score: {answer.score}\n")

        for i, result in enumerate(results_list):
            results_list[i]["@search.score"] = result["@search.score"] * 1000

            if print_results:
                print(f"\nTitle:\n{result['Title']}")
                print(f"Score:\n{result['@search.score']}")
                print(f"Source:\n{result['Source']}\n")

            # captions = result["@search.captions"]
            # if captions:
            #     caption = captions[0]
            #     if caption.highlights:
            #         print(f"Caption:\n{caption.highlights}\n")
            #     else:
            #         print(f"Caption:\n{caption.text}\n")

        # print(results_list)

        return results_list

    def create_search_index(self, index_name=None):
        # Create a search index
        fields = [
            SimpleField(name="ArticleId", type=SearchFieldDataType.String, key=True),
            SearchableField(
                name="Title",
                type=SearchFieldDataType.String,
                searchable=True,
                retrievable=True,
            ),
            SearchableField(
                name="Content",
                type=SearchFieldDataType.String,
                searchable=True,
                retrievable=True,
            ),
            SearchableField(name="Source", type=SearchFieldDataType.String, retrievable=True),
            # SearchableField(name="Labels", type=SearchFieldDataType.String, retrievable=True, searchable=True),
            SearchableField(
                name="YoutubeLinks",
                collection=True,
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                searchable=True,
                retrievable=True,
            ),
            SearchField(
                name="TitleVector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                searchable=True,
                vector_search_dimensions=1536,
                vector_search_profile_name="HnswProfile",
            ),
            SearchField(
                name="ContentVector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                searchable=True,
                vector_search_dimensions=1536,
                vector_search_profile_name="HnswProfile",
            ),
        ]

        vector_search = VectorSearch(
            algorithms=[
                HnswAlgorithmConfiguration(
                    name="Hnsw",
                    kind=VectorSearchAlgorithmKind.HNSW,
                    parameters=HnswParameters(
                        m=4,
                        ef_construction=400,
                        ef_search=500,
                        metric=VectorSearchAlgorithmMetric.COSINE,
                    ),
                ),
                ExhaustiveKnnAlgorithmConfiguration(
                    name="ExhaustiveKnn",
                    kind=VectorSearchAlgorithmKind.EXHAUSTIVE_KNN,
                    parameters=ExhaustiveKnnParameters(metric=VectorSearchAlgorithmMetric.COSINE),
                ),
            ],
            profiles=[
                VectorSearchProfile(
                    name="HnswProfile",
                    algorithm_configuration_name="Hnsw",
                ),
                VectorSearchProfile(
                    name="ExhaustiveKnnProfile",
                    algorithm_configuration_name="ExhaustiveKnn",
                ),
            ],
        )

        semantic_config = SemanticConfiguration(
            name="semantic-config",
            prioritized_fields=SemanticPrioritizedFields(
                title_field=SemanticField(field_name="Title"),
                content_fields=[SemanticField(field_name="Content")],
            ),
        )

        # Create the semantic settings with the configuration
        semantic_search = SemanticSearch(configurations=[semantic_config])

        suggester = SearchSuggester(name="TitleContentSG", source_fields=["Title", "Content"])

        # Create the search index with the semantic settings
        index = SearchIndex(
            name=index_name,
            fields=fields,
            suggesters=[suggester],
            vector_search=vector_search,
            semantic_search=semantic_search,
        )
        result = self.search_index_client.create_or_update_index(index)
        print(f" {result.name} created")

    def drop_search_index(self):
        self.search_index_client.delete_index(self.azure_env.INDEX_NAME)
        print(f"{self.azure_env.INDEX_NAME} deleted")

    def find_documents(self, search_fields: list = [], search_text: str = "*", select: list = [], log_results: bool = False):
        results = self.azure_env.search_client.search(search_fields=search_fields, search_text=search_text, select=select, search_mode="all")

        documents = []
        for result in results:
            document = {}
            for i, field in enumerate(select):
                document[field] = result[field].strip()

                if log_results:
                    if i == 0:
                        print("\n")

                    print(f"{field}:\n{result[field]}")

            documents.append(document)

        return documents

    def delete_documents(self, search_fields: list = [], search_text: str = "*", select: list = []):
        if "ArticleId" not in select:
            select.append("ArticleId")

        results = self.find_documents(search_fields=search_fields, search_text=search_text, select=select)

        print("Documents Deleted: ", len(results))

        for i, result in enumerate(results):
            print(f"Deleting {result['ArticleId']}")
            self.search_client.upload_documents({"@search.action": "delete", "ArticleId": str(result["ArticleId"])})

    def get_documents(self, search_fields: list = [], search_text: str = "*", select: list = [], file_type: str = "json"):
        results = self.find_documents(search_fields=search_fields, search_text=search_text, select=select, log_results=log_results)

        if file_type == "csv":
            fields = ["ArticleId", "Title", "Content", "Source", "YoutubeLinks"]
            with open(os.path.join(backend_dir, "indexes", f"{brand}-index-english.csv"), "w", encoding="utf-8") as f:
                write = csv.writer(f)
                write.writerow(fields)

                pbar = tqdm(results, position=1, leave=False, colour="red")
                for i, result in enumerate(pbar):
                    write.writerows([[result["ArticleId"], result["Title"], result["Content"], result["Source"], "YoutubeLinks"]])
        else:
            for i, result in enumerate(results):
                del result["@search.score"]
                del result["@search.reranker_score"]
                del result["@search.highlights"]
                del result["@search.captions"]

                results[i] = {
                    "ArticleId": result["ArticleId"],
                    "Title": result["Title"],
                    "Content": result["Content"],
                    "Source": result["Source"],
                    "YoutubeLinks": result["YoutubeLinks"],
                }

            if not os.path.exists(os.path.join(backend_dir, "indexes", self.azure_env.stage)):
                os.makedirs(os.path.join(backend_dir, "indexes", self.azure_env.stage), exist_ok=True)

            with open(os.path.join(backend_dir, "indexes", self.azure_env.stage, f"{brand}-index-english.json"), "w+", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=4)

    def delete_posts(self, index_path: str, age: int = 3):
        """
        Delete posts from the AI Search index older than the age(years).

        Args:
            index_path (str): Path to the json file containing the index.
            age (int, optional): The age in years of posts to delete. Defaults to 3.
        """

        with open(index_path, "r", encoding="utf-8") as f:
            documents = json.load(f)

            # Loop through the documents and check the created_at date
            for i, document in enumerate(documents):
                # Get the post from the Zendesk API
                response = requests.request(
                    "GET",
                    f"https://support.clo3d.com/api/v2/community/posts/{document['ArticleId']}",
                    headers={
                        "Content-Type": "application/json",
                    },
                )

                posts_json = json.loads(response.text)
                # print(posts_json)

                # Convert the created_at date to a datetime object
                created_at = datetime.strptime(posts_json["post"]["created_at"], "%Y-%m-%dT%H:%M:%SZ")

                # Set the cutoff date to 3 years ago
                cutoff_date = datetime.strptime(
                    "{}-01-01T00:00:00Z".format(datetime.today().year - age),
                    "%Y-%m-%dT%H:%M:%SZ",
                )

                # If the created_at date is less than the cutoff date, delete the post
                if created_at < cutoff_date:
                    print(f"Deleting {document['ArticleId']}")
                    self.search_client.upload_documents(
                        {
                            "@search.action": "delete",
                            "ArticleId": str(document["ArticleId"]),
                        }
                    )

    def document_source_breakdown(self):
        with open(os.path.join(backend_dir, "indexes", self.azure_env.stage, "clo3d-index-english.json"), "r", encoding="utf-8") as f:
            documents = json.load(f)

            sources = [document["Source"][: document["Source"].rfind("/")] for document in documents]

            sources_count = Counter(sources)

            for url, count in sources_count.items():
                print(f"{url}: {count}")

    def find_missing_documents_per_source(self):
        with open(os.path.join(backend_dir, "indexes", "prod", "clo3d-index-english.json"), "r", encoding="utf-8") as f:
            prod_documents = json.load(f)

        with open(os.path.join(backend_dir, "indexes", "dev", "clo3d-index-english.json"), "r", encoding="utf-8") as f:
            dev_documents = json.load(f)

            prod_sources = [prod_document["Source"] for prod_document in prod_documents]
            dev_sources_not_in_prod = set([dev_document["Source"] for dev_document in dev_documents if dev_document["Source"] not in prod_sources])

            for source in sorted(dev_sources_not_in_prod):
                print(source)


if __name__ == "__main__":
    env = questionary.select("Which environment?", choices=["prod", "dev"]).ask()
    brand = questionary.select("Which brand?", choices=["clo3d", "closet", "md", "allinone"]).ask()
    task = questionary.select(
        "What task?",
        choices=[
            "Create Search Index",
            "Delete Search Index",
            "Get Documents",
            "Search Documents (Hybrid, Text, or Vector)",
            "Find Documents",
            "Delete Documents",
            "Delete Posts By Age",
            "Get Document Source Breakdown",
        ],
    ).ask()

    cognitive_search = AISearch(AzureEnv(env, brand))

    if task == "Create Search Index":
        index_name = questionary.text("Index Name?").ask()
        cognitive_search.create_search_index(index_name)

    elif task == "Delete Search Index":
        cognitive_search.drop_search_index()

    elif task in ["Delete Documents", "Get Documents", "Find Documents"]:
        search_fields = questionary.checkbox("Search Fields?", choices=["ArticleId", "Title", "Source", "Content"]).ask()
        search_text = questionary.text("Search Text?").ask()
        select = questionary.checkbox("Select?", choices=["ArticleId", "Title", "Source", "Content"]).ask()

        if task == "Delete Documents":
            cognitive_search.delete_documents(search_fields=search_fields, search_text=search_text, select=select)
        elif task == "Get Documents":
            cognitive_search.get_documents(search_fields=search_fields, search_text=search_text, select=select)
        elif task == "Find Documents":
            cognitive_search.find_documents(search_fields=search_fields, search_text=search_text, select=select, log_results=True)

    elif task == "Search Documents":
        search_type = questionary.select("Search Type?", choices=["Hybrid", "Text", "Vector"]).ask()
        search_text = questionary.text("Search Text?", default="*").ask()

        if search_type == "Hybrid":
            cognitive_search.hybrid_search(search_text)
        elif search_type == "Text":
            cognitive_search.text_search(search_text)
        elif search_type == "Vector":
            cognitive_search.vector_search(search_text)

    elif task == "Delete Posts By Age":
        age = questionary.text("Age(in years)?", default="3").ask()
        cognitive_search.delete_posts(os.path.join(backend_dir, "indexes", env, "clo3d-index-english.json"), age=age)

    elif task == "Get Document Source Breakdown":
        cognitive_search.document_source_breakdown()

    elif task == "Find Missing Documents Per Source":
        cognitive_search.find_missing_documents_per_source()
