import os

import tqdm
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient

from tools.azure_env import AzureEnv


class BackupAISearch:
    def __init__(self, source_endpoint, source_credential, source_index_name, target_endpoint, target_credential, target_index_name):
        self.source_endpoint = source_endpoint
        self.source_credential = source_credential
        self.source_index_name = source_index_name
        self.target_endpoint = target_endpoint
        self.target_credential = target_credential
        self.target_index_name = target_index_name

    def create_clients(self, endpoint, credential, index_name):
        search_client = SearchClient(endpoint=endpoint, index_name=index_name, credential=credential)
        index_client = SearchIndexClient(endpoint=endpoint, credential=credential)
        return search_client, index_client

    def total_count(self, search_client):
        response = search_client.search(include_total_count=True, search_text="*", top=0)
        return response.get_count()

    def search_results_with_filter(self, search_client, key_field_name):
        last_item = None
        response = search_client.search(search_text="*", top=100000, order_by=key_field_name).by_page()
        while True:
            for page in response:
                page = list(page)
                if len(page) > 0:
                    last_item = page[-1]
                    yield page
                else:
                    last_item = None

            if last_item:
                response = search_client.search(
                    search_text="*", top=100000, order_by=key_field_name, filter=f"{key_field_name} gt '{last_item[key_field_name]}'"
                ).by_page()
            else:
                break

    def search_results_without_filter(self, search_client):
        response = search_client.search(search_text="*", top=100000).by_page()
        for page in response:
            page = list(page)
            yield page

    def backup_and_restore_index(self, source_endpoint, source_key, source_index_name, target_endpoint, target_key, target_index_name):
        # Create search and index clients
        source_search_client, source_index_client = self.create_clients(source_endpoint, source_key, source_index_name)
        target_search_client, target_index_client = self.create_clients(target_endpoint, target_key, target_index_name)

        # Get the source index definition
        source_index = source_index_client.get_index(name=source_index_name)
        non_retrievable_fields = []
        for field in source_index.fields:
            if field.hidden == True:
                non_retrievable_fields.append(field)
            if field.key == True:
                key_field = field

        if not key_field:
            raise Exception("Key Field Not Found")

        if len(non_retrievable_fields) > 0:
            print(
                f"WARNING: The following fields are not marked as retrievable and cannot be backed up and restored: {', '.join(f.name for f in non_retrievable_fields)}"
            )

        # Create target index with the same definition
        source_index.name = target_index_name
        target_index_client.create_or_update_index(source_index)

        document_count = self.total_count(source_search_client)
        can_use_filter = key_field.sortable and key_field.filterable
        if not can_use_filter:
            print("WARNING: The key field is not filterable or not sortable. A maximum of 100,000 records can be backed up and restored.")
        # Backup and restore documents
        all_documents = (
            self.search_results_with_filter(source_search_client, key_field.name)
            if can_use_filter
            else self.search_results_without_filter(source_search_client)
        )

        print("Backing up and restoring documents:")
        failed_documents = 0
        failed_keys = []
        with tqdm.tqdm(total=document_count) as progress_bar:
            for page in all_documents:
                result = target_search_client.upload_documents(documents=page)
                progress_bar.update(len(result))

                for item in result:
                    if item.succeeded is not True:
                        failed_documents += 1
                        failed_keys.append(page[result.index_of(item)].id)
                        print(f"Document upload error: {item.error.message}")

        if failed_documents > 0:
            print(f"Failed documents: {failed_documents}")
            print(f"Failed document keys: {failed_keys}")
        else:
            print("All documents uploaded successfully.")

        print(f"Successfully backed up '{source_index_name}' and restored to '{target_index_name}'")
        return source_search_client, target_search_client, all_documents


if __name__ == "__main__":
    environment = AzureEnv("dev", "md")

    # Variables not used here do not need to be updated in your .env file
    source_endpoint = environment.SEARCH_CLIENT_ENDPOINT
    source_credential = environment.AZURE_KEY_CREDENTIAL
    source_index_name = "md-index-english"

    # Default to same service for copying index
    target_endpoint = environment.SEARCH_CLIENT_ENDPOINT
    target_credential = environment.AZURE_KEY_CREDENTIAL
    target_index_name = "clo3d-closet-index"

    backup = BackupAISearch(source_endpoint, source_credential, source_index_name, target_endpoint, target_credential, target_index_name)

    source_search_client, target_search_client, all_documents = backup.backup_and_restore_index(
        source_endpoint, source_credential, source_index_name, target_endpoint, target_credential, target_index_name
    )
