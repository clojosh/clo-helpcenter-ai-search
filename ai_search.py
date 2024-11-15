import csv
import json
import os
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

from tools.environment import Environment

backend_dir = Path(__file__).parent


class AISearch:
    def __init__(self, environment: Environment):
        self.environment = environment
        self.search_client = environment.search_client
        self.search_index_client = environment.search_index_client
        self.openai_helper = environment.openai_helper

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
        self.search_index_client.delete_index(self.environment.INDEX_NAME)
        print(f"{self.environment.INDEX_NAME} deleted")

    def delete_all_documents(self):
        results = self.search_client.search(
            search_text="*",
            select=["ArticleId"],
        )
        results_list = list(results)

        for i, result in enumerate(results_list):
            print(f"Deleting {result['ArticleId']}")
            self.search_client.upload_documents({"@search.action": "delete", "ArticleId": str(result["ArticleId"])})

    def get_documents(self, brand="clo3d", file_type="json"):
        results = self.search_client.search(
            search_text="*",
            select=["ArticleId", "Title", "Content", "Source", "YoutubeLinks"],
        )
        results_list = list(results)

        if file_type == "csv":
            fields = [
                "ArticleId",
                "Title",
                "Content",
                "Source",
                "titleVector",
                "contentVector",
            ]
            with open(os.path.join(backend_dir, "indexes", f"{brand}-index-english.csv"), "w", encoding="utf-8") as f:
                write = csv.writer(f)
                write.writerow(fields)

                pbar = tqdm(results_list, position=1, leave=False, colour="red")
                for i, result in enumerate(pbar):
                    write.writerows(
                        [
                            [
                                result["ArticleId"],
                                result["Title"],
                                result["Content"],
                                result["Source"],
                                result["titleVector"],
                                result["contentVector"],
                            ]
                        ]
                    )
        else:
            for i, result in enumerate(results_list):
                del result["@search.score"]
                del result["@search.reranker_score"]
                del result["@search.highlights"]
                del result["@search.captions"]

                results_list[i] = {
                    "ArticleId": result["ArticleId"],
                    "Title": result["Title"],
                    "Content": result["Content"],
                    "Source": result["Source"],
                    "YoutubeLinks": result["YoutubeLinks"],
                    # "Labels": result["Label"],
                    # "titleVector": result["titleVector"],
                    # "contentVector": result["contentVector"],
                }

            with open(os.path.join(backend_dir, "indexes", f"{brand}-index-english.json"), "w+", encoding="utf-8") as f:
                json.dump(results_list, f, ensure_ascii=False, indent=4)

    def upload_documents(self):
        with open(os.path.join(backend_dir, "indexes", "clo3d-index-english.json"), "r", encoding="utf-8") as f:
            documents = json.load(f)

            documents_to_upload = []
            for i, document in enumerate(tqdm(documents)):
                if document["Content"] == "":
                    continue

                documents[i]["@search.action"] = "mergeOrUpload"
                documents[i]["TitleVector"] = self.environment.openai_helper.generate_embeddings(text=document["Title"])
                documents[i]["ContentVector"] = self.environment.openai_helper.generate_embeddings(text=document["Content"])

                documents_to_upload.append(documents[i])

                if i != 0 and (i % 100 == 0 or i == len(documents) - 1):
                    self.search_client.upload_documents(documents_to_upload)
                    documents_to_upload = []

    def delete_posts(self):
        with open("clo3d-index-english.json", "r", encoding="utf-8") as f:
            documents = json.load(f)

            for i, document in enumerate(documents):
                response = requests.request(
                    "GET",
                    f"https://support.clo3d.com/api/v2/community/posts/{document['ArticleId']}",
                    headers={
                        "Content-Type": "application/json",
                    },
                )

                posts_json = json.loads(response.text)
                # print(posts_json)

                created_at = datetime.strptime(posts_json["post"]["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                cutoff_date = datetime.strptime(
                    "{}-01-01T00:00:00Z".format(datetime.today().year - 3),
                    "%Y-%m-%dT%H:%M:%SZ",
                )

                if created_at < cutoff_date:
                    print(f"Deleting {document['ArticleId']}")
                    self.search_client.upload_documents(
                        {
                            "@search.action": "delete",
                            "ArticleId": str(document["ArticleId"]),
                        }
                    )


if __name__ == "__main__":
    env = questionary.select("Which environment?", choices=["prod", "dev"]).ask()
    brand = questionary.select("Which brand?", choices=["clo3d", "closet", "md", "allinone"]).ask()
    task = questionary.select(
        "What task?",
        choices=[
            "Create Search Index",
            "Delete Search Index",
            "Delete All Documents",
            "Get Documents",
            "Search Documents",
            "Delete Posts",
        ],
    ).ask()

    cognitive_search = AISearch(Environment(env, brand))

    if task == "Create Search Index":
        index_name = questionary.text("Index Name?").ask()
        cognitive_search.create_search_index(index_name)
    elif task == "Delete Search Index":
        cognitive_search.drop_search_index()
    elif task == "Delete All Documents":
        cognitive_search.delete_all_documents()
    elif task == "Get Documents":
        cognitive_search.get_documents(brand)
    elif task == "Search Documents":
        search_type = questionary.select("Search Type?", choices=["Hybrid", "Text", "Vector"]).ask()

        print("Enter Search Text:")
        search_text = input()
        if search_type == "Hybrid":
            cognitive_search.hybrid_search(search_text)
        elif search_type == "Text":
            cognitive_search.text_search(search_text)
        elif search_type == "Vector":
            cognitive_search.vector_search(search_text)
