import json
import os
import re
import sys
from pathlib import Path

import questionary
import requests
import shortuuid

from tools.environment import Environment
from tools.misc import trim_tokens


class CLOAPI:
    def __init__(self, environment: Environment):
        self.environment = environment
        self.search_client = environment.search_client
        self.clo_api_path = os.path.join(environment.ai_search_dir, "clo_api")
        self.language = environment.language

    def parse_api_docs(self):
        """
        Parse the API documentation files and save them as JSON files.
        """

        # Iterate over the API documentation files in the directory
        for api_doc in os.listdir(os.path.join(self.clo_api_path, "api_list", "original_docs")):
            docs = []

            # Open the API documentation file
            with open(os.path.join(self.clo_api_path, "api_list", "original_docs", api_doc), "r", encoding="utf-8") as f:
                func_objects = json.load(f)

                # Iterate over the functions in the API documentation
                for i, func in enumerate(func_objects):
                    # Trim the function documentation
                    content = trim_tokens(func_objects[func]["Doc"])

                    # Format the function documentation to include the arguments and return type
                    regex = r"{0}\(.*?\)".format(func)
                    content = re.sub(regex, func + "(" + func_objects[func]["Args"] + ") -> " + func_objects[func]["ReturnType"], content)

                    # Create a dictionary representing the parsed API documentation
                    docs.append(
                        {
                            "ArticleId": shortuuid.uuid(),  # Generate a unique ID for the document
                            "Source": "https://developer.clo3d.com/list.html",  # Specify the source URL
                            "Title": func + " API",  # Format the title as the function name followed by "API"
                            "Content": content,  # Use the trimmed function documentation
                            "Labels": [],  # Initialize an empty list for labels
                            "YoutubeLinks": [],  # Initialize an empty list for YouTube links
                        }
                    )

                # Save the parsed API documentation as a JSON file
                with open(os.path.join(self.clo_api_path, "api_list", api_doc), "w+", encoding="utf-8") as f:
                    json.dump(docs, f, indent=4)

    def parse_environment_setup_build_page(self):
        """
        Parse the content of the "Environment Setup & Build" page and save it as a JSON file.
        """

        # Retrieve the content of the "Environment Setup & Build" page
        url = "https://developer.clo3d.com/_sources/environment.rst.txt"
        response = requests.get(url)

        # Create a dictionary with the article information
        env_setup_build = [
            {
                "ArticleId": shortuuid.uuid(),  # Generate a unique identifier
                "Source": "https://developer.clo3d.com/environment.html",  # URL of the original source
                "Title": "Environment Setup & Build",  # Title of the article
                "Content": response.text,  # Content of the article
                "Labels": [],  # List of labels associated with the article
                "YoutubeLinks": [],  # List of YouTube links associated with the article
            }
        ]

        # Create the directory if it doesn't exist
        env_setup_build_path = os.path.join(self.clo_api_path, "env_setup_build")
        if not os.path.exists(env_setup_build_path):
            os.mkdir(os.path.dirname(env_setup_build_path), exist_ok=True)

        # Save the article information as a JSON file
        with open(os.path.join(env_setup_build_path, "env_setup_build.json"), "w+", encoding="utf-8") as f:
            json.dump(env_setup_build, f, indent=4)

    def parse_api_scenario_page(self):
        """
        Parse the content of the "API Scenario" page and save it as a JSON file.
        """

        # Retrieve the content of the "Environment Setup & Build" page
        url = "https://developer.clo3d.com/_sources/scenario.rst.txt"
        response = requests.get(url)

        code_block = response.text.split("\n|\n|\n|\n\n\n")

        code_block_dump = []
        for code in code_block:
            title = (
                code.split("code-block")[0]
                .replace("API Scenario", "")
                .replace("\n", "")
                .replace(".", "")
                .replace("-", "")
                .replace("=", "")
                .replace("*", "")
                .strip()
                + " Python Script"
            )

            code_block_dump.append(
                {
                    "ArticleId": shortuuid.uuid(),  # Generate a unique identifier
                    "Source": "https://developer.clo3d.com/scenario.html",  # URL of the original source
                    "Title": title,  # Title of the article
                    "Content": code.replace("API Scenario", "").replace("=======================", "").replace("****", ""),  # Content of the article
                    "Labels": [],  # List of labels associated with the article
                    "YoutubeLinks": [],  # List of YouTube links associated with the article
                }
            )

        # Create the directory if it doesn't exist
        env_setup_build_path = os.path.join(self.clo_api_path, "api_scenario")
        if not os.path.exists(env_setup_build_path):
            os.makedirs(env_setup_build_path, exist_ok=True)

        # Save the article information as a JSON file
        with open(os.path.join(env_setup_build_path, "api_scenario.json"), "w+", encoding="utf-8") as f:
            json.dump(code_block_dump, f, indent=4)

    def parse_api_option_type_page(self):
        """
        Parse the content of the "API Option & Type" page and save it as a JSON file.
        """

        # Retrieve the content of the "Environment Setup & Build" page
        url = "https://developer.clo3d.com/_sources/optiontype.rst.txt"
        response = requests.get(url)

        # Create a dictionary with the article information
        env_setup_build = [
            {
                "ArticleId": shortuuid.uuid(),  # Generate a unique identifier
                "Source": "https://developer.clo3d.com/optiontype.html",  # URL of the original source
                "Title": "API Option & Type",  # Title of the article
                "Content": response.text,  # Content of the article
                "Labels": [],  # List of labels associated with the article
                "YoutubeLinks": [],  # List of YouTube links associated with the article
            }
        ]

        # Create the directory if it doesn't exist
        env_setup_build_path = os.path.join(self.clo_api_path, "api_option_type")
        if not os.path.exists(env_setup_build_path):
            os.makedirs(env_setup_build_path, exist_ok=True)

        # Save the article information as a JSON file
        with open(os.path.join(env_setup_build_path, "api_option_type.json"), "w+", encoding="utf-8") as f:
            json.dump(env_setup_build, f, indent=4)

    def upload_documents(self):
        for dir in os.listdir(os.path.join(self.clo_api_path)):
            for files in os.listdir(os.path.join(self.clo_api_path, dir)):
                if files.endswith(".json"):
                    print("Uploading: " + os.path.join(self.clo_api_path, dir, files))
                    with open(os.path.join(self.clo_api_path, dir, files), "r", encoding="utf-8") as f:
                        documents = json.load(f)

                        for i, document in enumerate(documents):
                            documents[i]["@search.action"] = "mergeOrUpload"
                            documents[i]["TitleVector"] = self.environment.openai_helper.generate_embeddings(text=document["Title"])
                            documents[i]["ContentVector"] = self.environment.openai_helper.generate_embeddings(text=document["Content"])

                        self.environment.search_client.upload_documents(documents)

    def delete_documents(self):
        for dir in os.listdir(os.path.join(self.clo_api_path)):
            for files in os.listdir(os.path.join(self.clo_api_path, dir)):
                if files.endswith(".json"):
                    with open(os.path.join(self.clo_api_path, dir, files), "r", encoding="utf-8") as f:
                        documents = json.load(f)

                        for i, document in enumerate(documents):
                            documents[i]["@search.action"] = "delete"

                        self.environment.search_client.upload_documents(documents)


if __name__ == "__main__":
    env = questionary.select("Which environment?", choices=["prod", "dev"]).ask()
    # brand = questionary.select("Which brand?", choices=["clo3d", "closet", "clovf", "md"]).ask()
    task = questionary.select(
        "What task?",
        choices=[
            "Parse API Docs",
            "Parse Environment Setup & Build",
            "Parse API Scenario",
            "Parse API Option & Type",
            "Upload Documents",
            "Delete Documents",
        ],
    ).ask()

    clo_api = CLOAPI(Environment(env, "clo3d"))

    if task == "Parse API Docs":
        delete_previous_documents = questionary.select("Did you delete previous documents?", choices=["Yes", "No"]).ask()
        if delete_previous_documents == "Yes":
            clo_api.parse_api_docs()
    elif task == "Parse Environment Setup & Build":
        clo_api.parse_environment_setup_build_page()
    elif task == "Parse API Scenario":
        clo_api.parse_api_scenario_page()
    elif task == "Parse API Option & Type":
        clo_api.parse_api_option_type_page()
    elif task == "Upload Documents":
        clo_api.upload_documents()
    elif task == "Delete Documents":
        clo_api.delete_documents()
