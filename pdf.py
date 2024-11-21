import glob
import io
import json
import multiprocessing
import os
import re

import fitz
import questionary
import requests
import shortuuid
from tqdm import tqdm

from tools.azure_env import AzureEnv
from tools.misc import trim_tokens


class PDF:
    def __init__(self, environment: AzureEnv):
        self.env = env
        self.brand = environment.brand
        self.language = environment.language
        self.pdf_path = environment.get_locale_path("pdf")
        self.udemy_path = os.path.join(environment.ai_search_dir, "clo3d", "udemy")
        self.zendesk_article_api_endpoint = environment.get_zendesk_articles_api_endpoint(1)
        self.openai_helper = environment.openai_helper
        self.search_client = environment.search_client

    def remove_miscellaneous_text(pdf: str) -> str:
        """Removes miscellaneous text from the PDF"""

        misc_text = [
            "Copyright 2022",
            "Copyright 2021",
            "Copyright 2020",
            "Copyright 2019",
            "Copyright 2018",
            "CLO Virtual Fashion Inc.",
            "All Rights Reserved.",
            "C L O V I R T U A L F A S H I O N",
            "STRICTLY CONFIDENTIAL",
            "CLO VIRTUAL FASHION",
        ]

        for text in misc_text:
            pdf = pdf.replace(text, "")

        return pdf

    def extract_pdf_url(zendesk_article_attachment_api_endpoint) -> list:
        """Extracts the url location for every PDF in the article"""

        headers = {
            "Content-Type": "application/json",
        }

        response = requests.request(
            "GET", zendesk_article_attachment_api_endpoint, auth=("share_admin@foxxing.com", "CLOzendeskshare12#$"), headers=headers
        )

        json_objects = json.loads(response.text)

        pdf = []
        for attachment in json_objects["article_attachments"]:
            if attachment["content_type"] == "application/pdf":
                pdf.append(attachment["content_url"])

        return pdf

    def extract_pdf_text(pdf_url="") -> str:
        """Extracts the text from the PDF"""

        if pdf_url != "":
            r = requests.get(pdf_url)
            f = io.BytesIO(r.content)
            documents = fitz.open(stream=f, filetype="pdf")

        content_chunks = []
        for page in documents:
            content_chunks.append(page.get_text())

        content = (" ").join(content_chunks)

        return trim_tokens(PDF.remove_miscellaneous_text(trim_tokens(content)))

    def add_labels(env, brand, language, pdf_path, file):
        print(f"Adding labels to: {file}")

        environment = AzureEnv(env, brand, language)

        with open(os.path.join(pdf_path, file), "r", encoding="utf-8") as f:
            documents = json.load(f)

        for i, document in enumerate(documents):
            labels = environment.openai_helper.generate_labels(document["PDF_Text"])
            documents[i]["Labels"] = labels

        with open(os.path.join(pdf_path, file), "w", encoding="utf-8") as f:
            json.dump(documents, f, ensure_ascii=False, indent=4)

    def mp_add_labels(self):
        file_paths = sorted(os.listdir(self.pdf_path), key=lambda x: int(x.partition("_")[2].partition(".")[0]))

        add_labels_params = []
        for file in file_paths:
            add_labels_params.append((self.env, self.brand, self.language, self.pdf_path, file))

        with multiprocessing.Pool(5) as p:
            p.starmap_async(PDF.add_labels, add_labels_params, error_callback=lambda e: print(e))
            p.close()
            p.join()

    def get_zendesk_articles_with_pdf(env, brand, language, pdf_path, page):
        """Retrieves all articles with PDF attachments and stores them in a JSON"""
        print(f"Getting Zendesk Articles for page: {page}")

        environment = AzureEnv(env, brand, language)

        page_url = requests.request(
            "GET",
            environment.get_zendesk_articles_api_endpoint(page),
            headers={
                "Content-Type": "application/json",
            },
        )

        json_objects = json.loads(page_url.text)

        documents = []
        for article in json_objects["articles"]:
            if article["draft"] == False and article["user_segment_id"] == None:
                article["id"] = str(article["id"])

                article["pdf"] = ""
                pdf_url = PDF.extract_pdf_url(environment.get_zendesk_article_attachment_api_endpoint(article["id"]))
                for url in pdf_url:
                    article["pdf"] += PDF.extract_pdf_text(url)

                if "pdf" in article:
                    if len(article["pdf"]) > 0:
                        if brand == "closet":
                            article["html_url"] = re.findall(r"https:\/\/support\.clo-set\.com\/hc\/en-us\/articles\/\d+", article["html_url"])[0]
                        else:
                            article["html_url"] = re.findall(r"https:\/\/support\.clo3d\.com\/hc\/en-us\/articles\/\d+", article["html_url"])[0]

                        documents.append(
                            {
                                "ArticleId": article["id"],
                                "Title": article["title"],
                                "Source": article["html_url"],
                                "PDF_URL": pdf_url,
                                "PDF_Text": article["pdf"],
                            }
                        )

        if len(documents) > 0:
            with open(os.path.join(pdf_path, f"page_{page}.json"), "w+", encoding="utf-8") as f:
                json.dump(documents, f, ensure_ascii=False, indent=4)
            documents = []

    def mp_get_zendesk_articles_with_pdf(self):
        response = requests.request(
            "GET",
            self.zendesk_article_api_endpoint,
            headers={
                "Content-Type": "application/json",
            },
        )

        json_objects = json.loads(response.text)
        page_count = json_objects["page_count"]

        with multiprocessing.Pool(5) as p:
            p.starmap_async(
                PDF.get_zendesk_articles_with_pdf, [(self.env, self.brand, self.language, self.pdf_path, page) for page in range(1, page_count + 1)]
            )
            p.close()
            p.join()

    def get_udemy_pdfs(self):
        """Retrieves all articles with PDF attachments and stores them in a JSON"""

        print("Getting Udemy PDFs")

        documents = []
        for file in glob.glob(os.path.join(self.udemy_path, "**/*.pdf"), recursive=True):
            pdf = fitz.open(file)

            file_name = os.path.basename(file)
            file_name = file_name.split(".")
            file_name = file_name[0]

            content_chunks = []
            for page in pdf:
                content_chunks.append(page.get_text())

            content = trim_tokens(PDF.remove_miscellaneous_text(trim_tokens((" ").join(content_chunks))))

            documents.append(
                {
                    "ArticleId": shortuuid.uuid(),
                    "Title": file_name,
                    "Source": "https://www.udemy.com/user/clo3d-virtual-fashion/",
                    "PDF_Text": content,
                    # "PDF_Summary": self.openai_helper.generate_pdf_summary(content),
                    "Labels": self.openai_helper.generate_labels(content),
                }
            )

            with open(os.path.join(self.udemy_path, "udemy_pdf.json"), "w", encoding="utf-8") as f:
                json.dump(documents, f, ensure_ascii=False, indent=4)

    def summarize_pdf(env, brand, language, pdf_path, file):
        print(f"Summarizing {file}")

        environment = AzureEnv(env, brand, language)

        with open(f"{os.path.join(pdf_path, file)}", "r", encoding="utf-8") as f:
            documents = json.load(f)

        for i, document in enumerate(documents):
            pdf_summary = environment.openai_helper.generate_pdf_summary(document["PDF_Text"])
            documents[i]["PDF_Summary"] = pdf_summary

        with open(os.path.join(pdf_path, file), "w", encoding="utf-8") as f:
            json.dump(documents, f, ensure_ascii=False, indent=4)

    def mp_summarize_pdf(self):
        file_paths = os.listdir(self.pdf_path)
        file_paths = sorted(file_paths, key=lambda x: int(x.partition("_")[2].partition(".")[0]))

        with multiprocessing.Pool(5) as p:
            p.starmap_async(
                PDF.summarize_pdf,
                [(self.env, self.brand, self.language, self.pdf_path, file) for file in file_paths],
                error_callback=lambda e: print(e),
            )
            p.close()
            p.join()

    def upload_pdfs(self):
        pdf_path = sorted(os.listdir(self.pdf_path), key=lambda x: int(x.partition("_")[2].partition(".")[0]))

        for file in tqdm(pdf_path, colour="green", position=0, leave=True):
            with open(os.path.join(self.pdf_path, file), "r", encoding="utf-8") as f:
                documents = json.load(f)

            for i, document in enumerate(documents):
                document["@search.action"] = "mergeOrUpload"
                document["Content"] = document["PDF_Summary"]
                document["TitleVector"] = self.openai_helper.generate_embeddings(text=document["Title"])
                document["ContentVector"] = self.openai_helper.generate_embeddings(text=document["PDF_Summary"])
                document["YoutubeLinks"] = []
                del document["PDF_URL"]
                del document["PDF_Text"]
                del document["PDF_Summary"]

            self.search_client.upload_documents(documents)

    def upload_udemy_pdfs(self):
        with open(os.path.join(self.udemy_path, "udemy_pdf.json"), "r", encoding="utf-8") as f:
            documents = json.load(f)

        for i, document in enumerate(documents):
            document["@search.action"] = "mergeOrUpload"
            document["Content"] = document["PDF_Summary"]
            document["Labels"] = self.openai_helper.generate_labels(document["PDF_Summary"])
            document["TitleVector"] = self.openai_helper.generate_embeddings(text=document["Title"])
            document["ContentVector"] = self.openai_helper.generate_embeddings(text=document["PDF_Summary"])
            document["YoutubeLinks"] = []

            del document["PDF_Text"]
            del document["PDF_Summary"]

        self.search_client.upload_documents(documents)


if __name__ == "__main__":
    env = questionary.select("Which environment?", choices=["prod", "dev"]).ask()
    brand = questionary.select("Which environment?", choices=["clo3d", "closet"]).ask()
    task = questionary.select(
        "What task?", choices=["Get Zendesk Articles With PDFs", "Get Udemy PDFs", "Summarize PDF", "Add Labels", "Upload PDFs", "Upload Udemy PDFs"]
    ).ask()
    # language = questionary.select("What language?", choices=["English", "Korean"]).ask()

    pdf = PDF(AzureEnv(env, brand))

    if task == "Get Zendesk Articles With PDFs":
        pdf.mp_get_zendesk_articles_with_pdf()

    elif task == "Get Udemy PDFs":
        pdf.get_udemy_pdfs()

    elif task == "Summarize PDF":
        pdf.mp_summarize_pdf()

    elif task == "Add Labels":
        pdf.mp_add_labels()

    elif task == "Upload PDFs":
        pdf.upload_pdfs()

    elif task == "Upload Udemy PDFs":
        pdf.upload_udemy_pdfs()
