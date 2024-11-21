import json
import multiprocessing
import os
import re
from datetime import datetime

import questionary
import requests

from tools.azure_env import AzureEnv
from tools.misc import remove_html_tags, trim_tokens


class Posts:
    def __init__(self, environment: AzureEnv):
        self.env = environment.env
        self.brand = environment.brand
        self.language = environment.language
        self.posts_path = environment.get_locale_path("posts")
        self.search_client = environment.search_client
        self.openai_helper = environment.openai_helper
        self.zendesk_posts_endpoint = "https://support.{brand}.com/api/v2/help_center/community/posts.json?page={page}&per_page=60"
        self.zendesk_comments_endpoint = "https://support.{brand}.com/api/v2/community/posts/{post_id}/comments?sort_by=created_at&sort_order=asc"

    def get_comments(brand, comments_endpoint, post_id) -> list:
        """Returns list of comments up to the admin/agent last comment for each post"""

        comments_response = requests.request(
            "GET",
            comments_endpoint.format(brand=brand, post_id=post_id),
            auth=(
                "share_admin@foxxing.com" if brand == "clo3d" else "joshua.lee@clo3d.com",
                "CLOzendeskshare12#$",
            ),
            headers={
                "Accept": "application/json",
            },
        )

        comment_objects = json.loads(comments_response.text)
        page_count = comment_objects["page_count"]

        comments = []
        for page in range(page_count):
            for comment in comment_objects["comments"]:
                if brand == "clo3d":
                    comment["html_url"] = re.findall(
                        r"https:\/\/support\.clo3d\.com\/hc\/en-us\/community\/posts\/\d+",
                        comment["html_url"],
                    )[0]
                elif brand == "md":
                    comment["html_url"] = re.findall(
                        r"https:\/\/support\.marvelousdesigner\.com\/hc\/en-us\/community\/posts\/\d+",
                        post["html_url"],
                    )[0]

                comments.append(
                    {
                        "author_id": comment["author_id"],
                        "comment_id": comment["id"],
                        "official": comment["official"],  # true or false
                        "comment_url": comment["html_url"],
                        "comment_body": trim_tokens(remove_html_tags(comment["body"])),
                    }
                )

            if comment_objects["next_page"]:
                next_page = requests.request(
                    "GET",
                    comment_objects["next_page"],
                    auth=("share_admin@foxxing.com", "CLOzendeskshare12#$"),
                    headers={
                        "Content-Type": "application/json",
                    },
                )

                comment_objects = json.loads(next_page.text)

        filter_official_comments = [comment for comment in comments if comment["official"] == True]
        if len(filter_official_comments) > 0:
            return filter_official_comments

        return comments

    def get_posts(brand, post_endpoint, comments_endpoint, page_count, post_path):
        """Returns a post"""

        print(f"Getting posts for page {page_count}")

        posts_response = requests.request(
            "GET",
            post_endpoint,
            headers={
                "Content-Type": "application/json",
            },
        )

        posts_objects = json.loads(posts_response.text)

        posts = []
        cutoff_date = datetime.strptime("{}-01-01T00:00:00Z".format(datetime.today().year - 3), "%Y-%m-%dT%H:%M:%SZ")
        for post in posts_objects["posts"]:
            created_at = datetime.strptime(post["created_at"], "%Y-%m-%dT%H:%M:%SZ")

            if brand == "clo3d":
                post_url = re.findall(
                    r"https:\/\/support\.clo3d\.com\/hc\/en-us\/community\/posts\/\d+",
                    post["html_url"],
                )[0]
            elif brand == "marvelousdesigner":
                post_url = re.findall(
                    r"https:\/\/support\.marvelousdesigner\.com\/hc\/en-us\/community\/posts\/\d+",
                    post["html_url"],
                )[0]

            if created_at >= cutoff_date:
                posts.append(
                    {
                        "post_id": post["id"],
                        "post_title": post["title"],
                        "post_url": post_url,
                        "post_details": trim_tokens(remove_html_tags(post["details"])),
                        "created_at": post["created_at"],
                        "comments": Posts.get_comments(brand, comments_endpoint, post["id"]),
                    }
                )

        if len(posts) > 0:
            with open(
                os.path.join(post_path, f"page_{page_count}.json"),
                "w+",
                encoding="utf-8",
            ) as f:
                json.dump(posts, f, ensure_ascii=False, indent=4)

        return posts

    def mp_get_posts(self):
        """Returns list of posts"""
        brand = self.brand if self.brand != "md" else "marvelousdesigner"

        posts_response = requests.request(
            "GET",
            self.zendesk_posts_endpoint.format(brand=brand, page=1),
            headers={
                "Content-Type": "application/json",
            },
        )

        posts_objects = json.loads(posts_response.text)
        page_count = posts_objects["page_count"]

        with multiprocessing.Pool(7) as p:
            p.starmap_async(
                Posts.get_posts,
                [
                    (
                        brand,
                        self.zendesk_posts_endpoint.format(brand=brand, page=page),
                        self.zendesk_comments_endpoint,
                        page,
                        self.posts_path,
                    )
                    for page in range(1, page_count + 1)
                ],
                error_callback=lambda e: print(e),
            )
            p.close()
            p.join()

    def upload_posts(env, posts_path, file, brand, language):
        """Upload posts to Cognitive Search"""

        print(f"Uploading {file}")

        environment = AzureEnv(env, brand, language)
        search_client = environment.search_client
        openai_helper = environment.openai_helper

        with open(os.path.join(posts_path, file), "r", encoding="utf-8") as f:
            documents = json.load(f)

            upload_documents = []
            for i, document in enumerate(documents):
                content = document["post_details"]
                for comments in document["comments"]:
                    content += " " + comments["comment_body"]

                try:
                    upload_documents.append(
                        {
                            "@search.action": "mergeOrUpload",
                            "ArticleId": str(document["post_id"]),
                            "Source": document["post_url"],
                            "Title": document["post_title"],
                            "Content": content,
                            "Labels": [],
                            "YoutubeLinks": [],
                            "titleVector": openai_helper.generate_embeddings(text=document["post_title"]),
                            "contentVector": openai_helper.generate_embeddings(text=content if content != "" else document["post_title"]),
                        }
                    )
                except Exception:
                    print(f"Failed to upload: {document['post_id']}")

            search_client.upload_documents(upload_documents)
            print(f"Uploaded {file}")

    def mp_upload_posts(self):
        file_paths = sorted(
            os.listdir(self.posts_path),
            key=lambda x: int(x.partition("_")[2].partition(".")[0]),
        )

        upload_posts_params = []
        for file in file_paths:
            upload_posts_params.append((self.env, self.posts_path, file, self.brand, self.language))

        with multiprocessing.Pool(5) as p:
            p.starmap_async(
                Posts.upload_posts,
                upload_posts_params[1:],
                error_callback=lambda e: print(e),
            )
            p.close()
            p.join()


if __name__ == "__main__":
    env = questionary.select("Which environment?", choices=["prod", "dev"]).ask()
    brand = questionary.select("Which brand?", choices=["clo3d", "closet", "md"]).ask()
    task = questionary.select("What task?", choices=["Get Posts", "Add Labels", "Upload Posts"]).ask()
    # language = questionary.select("What language?", choices=["English", "Korean"]).ask()

    post = Posts(AzureEnv(env, brand))

    if task == "Get Posts":
        post.mp_get_posts()
    elif task == "Add Labels":
        post.mp_add_labels()
    elif task == "Upload Posts":
        post.mp_upload_posts()
