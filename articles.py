import json
import multiprocessing
import os
import re
import sys
from pathlib import Path

import questionary
import requests

from tools.azure_env import AzureEnv
from tools.misc import (
    extract_youtube_links,
    get_section_and_category,
    num_tokens_from_string,
    remove_html_tags,
    remove_miscellaneous_text,
    trim_tokens,
)


class Article:
    def __init__(self, azure_env: AzureEnv):
        self.azure_env = azure_env
        self.search_client = azure_env.search_client

    def get_zendesk_document(self, article_id):
        page_url = requests.request(
            "GET",
            self.azure_env.get_zendesk_article_api_endpoint(article_id),
            headers={
                "Content-Type": "application/json",
            },
        )

        return json.loads(page_url.text)

    @staticmethod
    def get_zendesk_documents(stage, brand, language, article_path, page):
        print("Getting Zendesk Articles for page: " + str(page))

        azure_env = AzureEnv(stage, brand, language)

        page_url = requests.request(
            "GET",
            azure_env.get_zendesk_article_api_endpoint(page),
            headers={
                "Content-Type": "application/json",
            },
        )

        json_objects = json.loads(page_url.text)

        documents = []
        for article in json_objects["articles"]:
            if article["draft"] is False and article["user_segment_id"] is None:
                # CLO3D:
                # 115001436607 - Update Article Section
                # 115012589987 - Requested by John to exclude in CLO3D
                # 360005512874 - References Article Section that has PDF attachments
                # 360002306994 - Lessons Section
                # CLOSET:
                # 5026352977423, 6280973212175 - Update Article Section
                # 360001149855, 360001011655, 360000854796 - Joining Connect
                # 7975498603663 - Account Section, CVF Articles Cover this section
                if (brand == "clo3d" and (article["section_id"] in [360005512874, 360002306994] or article["id"] in [115012589987])) or (
                    brand == "closet"
                    and article["section_id"] in [5026352977423, 6280973212175, 360001149855, 360001011655, 360000854796, 7975498603663]
                ):
                    continue

                if brand == "clo3d":
                    article["html_url"] = re.findall(
                        rf"https:\/\/support\.clo3d\.com\/hc\/{azure_env.get_locale()}\/articles\/\d+", article["html_url"]
                    )[0]
                elif brand == "closet":
                    article["html_url"] = re.findall(
                        rf"https:\/\/support\.clo-set\.com\/hc\/{azure_env.get_locale()}\/articles\/\d+", article["html_url"]
                    )[0]
                elif brand == "clovf":
                    article["html_url"] = re.findall(
                        rf"https:\/\/clovf\.zendesk\.com\/hc\/{azure_env.get_locale()}\/articles\/\d+", article["html_url"]
                    )[0]
                elif brand == "md":
                    article["html_url"] = re.findall(
                        rf"https:\/\/support\.marvelousdesigner\.com\/hc\/{azure_env.get_locale()}\/articles\/\d+", article["html_url"]
                    )[0]

                article["youtube_links"] = extract_youtube_links(str(article["body"]))
                article["body"] = remove_html_tags(str(article["body"]))
                article["body"] = remove_miscellaneous_text(article["body"])
                article["body"] = trim_tokens(article["body"])
                article["id"] = str(article["id"])
                article["section_id"], article["section"], article["category_id"], article["category"] = get_section_and_category(
                    azure_env, article["section_id"]
                )

                documents.append(
                    {
                        "ArticleId": article["id"],
                        "Source": article["html_url"],
                        "Title": article["title"],
                        "Content": article["body"],
                        "Tokens": num_tokens_from_string(article["body"], "gpt-3.5-turbo"),
                        "SectionId": article["section_id"],
                        "Section": article["section"],
                        "CategoryId": article["category_id"],
                        "Category": article["category"],
                        "YoutubeLinks": article["youtube_links"],
                    }
                )

        if len(documents) > 0:
            with open(os.path.join(article_path, f"page_{page}.json"), "w+", encoding="utf-8") as f:
                json.dump(documents, f, ensure_ascii=False, indent=4)

    def mp_get_zendesk_documents(self):
        headers = {
            "Content-Type": "application/json",
        }

        response = requests.request("GET", self.azure_env.get_zendesk_article_api_endpoint(1), headers=headers)
        json_objects = json.loads(response.text)
        page_count = json_objects["page_count"]

        with multiprocessing.Pool(5) as p:
            p.starmap_async(
                Article.get_zendesk_documents,
                [
                    (self.azure_env.stage, self.azure_env.brand, self.azure_env.language, self.azure_env.get_article_path(), page)
                    for page in range(1, page_count + 1)
                ],
                error_callback=lambda e: print(e),
            )
            p.close()
            p.join()

    @staticmethod
    def upload_documents(env, brand, language, article_path, file):
        print(f"Uploading {file}")

        azure_env = AzureEnv(env, brand, language)

        with open(os.path.join(article_path, file), "r", encoding="utf-8") as f:
            documents = json.load(f)

            for i, document in enumerate(documents):
                if document["Content"] == "":
                    document["Content"] = document["Title"]

                documents[i]["@search.action"] = "mergeOrUpload"
                documents[i]["TitleVector"] = azure_env.openai_helper.generate_embeddings(text=document["Title"])
                documents[i]["ContentVector"] = azure_env.openai_helper.generate_embeddings(text=document["Content"])
                del documents[i]["Tokens"]
                del documents[i]["SectionId"]
                del documents[i]["Section"]
                del documents[i]["CategoryId"]
                del documents[i]["Category"]

            if brand == "clovf":
                # Upload clovf articles to both clo3d and clo-set
                AzureEnv(env, "clo3d").search_client.upload_documents(documents)
                # AzureEnv(env, "closet").search_client.upload_documents(documents)
            else:
                azure_env.search_client.upload_documents(documents)

    def mp_upload_documents(self):
        file_paths = sorted(os.listdir(self.azure_env.get_article_path()), key=lambda x: int(x.partition("_")[2].partition(".")[0]))

        upload_documents_params = []
        for file in file_paths:
            upload_documents_params.append(
                (self.azure_env.stage, self.azure_env.brand, self.azure_env.language, self.azure_env.get_article_path(), file)
            )

        with multiprocessing.Pool(5) as p:
            p.starmap_async(Article.upload_documents, upload_documents_params, error_callback=lambda e: print(e))
            p.close()
            p.join()

    def delete_document(self, article_id: str | list):
        print(f"Deleting {article_id}")

        if isinstance(article_id, list):
            for id in article_id:
                result = self.search_client.upload_documents({"@search.action": "delete", "ArticleId": id})
        else:
            result = self.search_client.upload_documents({"@search.action": "delete", "ArticleId": article_id})

    def delete_excluded_documents(self, brand):
        headers = {
            "Content-Type": "application/json",
        }

        response = requests.request("GET", self.zendesk_article_api_endpoint, headers=headers)
        json_objects = json.loads(response.text)
        page_count = json_objects["page_count"]

        for page in range(1, 1 + page_count):
            response = requests.request("GET", self.azure_env.get_zendesk_articles_api_endpoint(page), headers=headers)
            json_objects = json.loads(response.text)
            articles = json_objects["articles"]

            for article in articles:
                if brand == "closet" and article["section_id"] in [
                    5026352977423,
                    6280973212175,
                    360001149855,
                    360001011655,
                    360000854796,
                    7975498603663,
                ]:
                    print(article["id"])
                    self.search_client.upload_documents({"@search.action": "delete", "ArticleId": str(article["id"])})


if __name__ == "__main__":
    stage = questionary.select("Which stage?", choices=["prod", "dev"]).ask()
    brand = questionary.select("Which brand?", choices=["clo3d", "closet", "clovf", "md"]).ask()
    language = questionary.select("Which language?", choices=["English", "Korean"]).ask()
    task = questionary.select("What task?", choices=["Get Zendesk Article", "Get All Zendesk Articles", "Delete Articles", "Upload Articles"]).ask()
    article = Article(AzureEnv(stage, brand, language))

    if task == "Get Zendesk Article":
        article_id = questionary.text("Article ID").ask()
        article.get_zendesk_document(article_id=article_id)

    elif task == "Get All Zendesk Articles":
        article.mp_get_zendesk_documents()

    elif task == "Upload Articles":
        article.mp_upload_documents()

    elif task == "Delete Articles":
        article.delete_document(["9068601237007", "360002216776", "7975553659023", "7975557113615", "360002199276"])
