import json
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_random_exponential

from .misc import trim_tokens

backend_dir = Path(__file__).parent.parent.parent

sys.path.append(str(backend_dir))
from tools.misc import num_tokens_from_string


class OpenAIHelper:
    def __init__(
        self,
        openai_client,
        AZURE_OPENAI_CHATGPT_DEPLOYMENT,
        AZURE_OPENAI_EMB_DEPLOYMENT,
        language="English",
    ):
        self.openai_client = openai_client
        self.AZURE_OPENAI_CHATGPT_DEPLOYMENT = AZURE_OPENAI_CHATGPT_DEPLOYMENT
        self.AZURE_OPENAI_EMB_DEPLOYMENT = AZURE_OPENAI_EMB_DEPLOYMENT
        self.language = language

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_embeddings(self, text: str):
        tokens = num_tokens_from_string(text, "text-embedding-ada-002")

        if tokens >= 8000:
            text = text[:8000]

        return self.openai_client.embeddings.create(input=[text], model=self.AZURE_OPENAI_EMB_DEPLOYMENT).data[0].embedding

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_questions(self, text):
        tokens = num_tokens_from_string(text, "gpt-4")

        if tokens >= 4096 - 200:
            text = text[: 4096 - 200]

        messages = [
            {"role": "user", "content": f"Generate 10 brief and concise questions a customer would ask about this in {self.language}: {text}"}
        ]

        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0.7, max_tokens=200, n=1
        )

        questions = chat_completion.choices[0].message.content
        questions = re.sub("^[0-9]+\.\s", "", questions, flags=re.MULTILINE)
        questions = re.sub("\n+", " ", questions, flags=re.MULTILINE)

        return questions

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_labels(self, text) -> list[str]:
        tokens = num_tokens_from_string(text, "gpt-4")
        keywords_num = 10

        if tokens <= 50:
            keywords_num = 5

        if tokens >= 4096 - 200:
            text = text[: 4096 - 200]

        messages = [{"role": "user", "content": f"Generate {keywords_num} keywords from the this in {self.language}: {text}"}]

        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0, max_tokens=200, n=1
        )

        labels = chat_completion.choices[0].message.content.splitlines()
        for i, l in enumerate(labels):
            labels[i] = re.sub("[0-9]+\.*\)*\s*", "", l, flags=re.MULTILINE).strip()

        return labels

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_transcript_summary(self, transcript):
        """Summarize a transcript"""

        tokens = num_tokens_from_string(transcript, "gpt-4")

        if tokens >= 4096 - 750:
            transcript = transcript[: 4096 - 750]

        messages = [
            {
                "role": "user",
                "content": f"Provide a comprehensive guide of the given text. Include all step-by-step instructions, definitions, and tips and tricks. {transcript}",
            }
        ]

        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0.7, max_tokens=1000, n=1
        )

        summary = chat_completion.choices[0].message.content
        summary = re.sub(r"\n+", " ", summary)
        summary = re.sub(r"\s+", " ", summary)

        return summary

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_pdf_summary(self, pdf):
        """Summarize a pdf"""

        tokens = num_tokens_from_string(pdf, "gpt-4")

        if tokens >= 4096 - 1000:
            pdf = pdf[: 4096 - 1000]

        messages = [
            {
                "role": "user",
                "content": f"Provide a comprehensive guide of the given text. Include all step-by-step instructions, definitions, and warranties. {pdf}",
            }
        ]

        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0.7, max_tokens=1000, n=1
        )

        summary = chat_completion.choices[0].message.content
        summary = re.sub(r"\n+", " ", summary)
        summary = re.sub(r"\s+", " ", summary)

        return summary

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def outline_webpage(self, content, website_url):
        """Outline a Webpage"""

        try:
            tokens = num_tokens_from_string(content, "gpt-4")

            if tokens >= 32000 - 1500:
                raise ValueError(f"Content too long, tokens found {tokens}")

            messages = [
                {
                    "role": "user",
                    "content": f"Provide a detailed outline of a website based on the provided HTML code. Outline must include all text and web links whereever possible, for example: [Start Free Trial](https://clo3d.com). The outline must exclude any html tags. The url of the website is {website_url}. ###HTML Code###: {content}",
                }
            ]

            chat_completion = self.openai_client.chat.completions.create(
                model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0, max_tokens=1500, n=1
            )

            outline = chat_completion.choices[0].message.content
            outline = re.sub(r"\[https.*\]", "", outline)
            outline = outline.replace("[", "").replace("]", "").replace("(", "[").replace(")", "]")

            return outline
        except Exception as e:
            print("OpenAI Outline Webpage Error: ", e)
            return ""

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def scrape_webpage(self, content, website_url):
        """Scrape a Webpage"""

        try:
            tokens = num_tokens_from_string(content, "gpt-4")

            if tokens >= 32000 - 1500:
                raise ValueError(f"Content too long for {website_url}, tokens found {tokens}")

            # messages = [{
            #     "role": "user",
            #     "content": f"Provide detailed instructions and web links to effectively navigate and utilize the features of a website based on the provided HTML code: {content}"
            # }]

            messages = [
                {
                    "role": "user",
                    "content": f"Provide detailed instructions to effectively navigate and utilize the features of a website based on the provided HTML code. Instructions must include web links from the provided HTML code, for example: [Start Free Trial](https://clo3d.com). Instructions must exclude any html tags. The url of the website is {website_url}. ###HTML Code###: {content}",
                }
            ]

            chat_completion = self.openai_client.chat.completions.create(
                model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0, max_tokens=1500, n=1
            )

            scraped_content = chat_completion.choices[0].message.content
            scraped_content = re.sub(r"\[https.*\]", "", scraped_content)
            scraped_content = scraped_content.replace("[", "").replace("]", "").replace("(", "[").replace(")", "]")

            return scraped_content
        except Exception as e:
            print("OpenAI Scrape Webpage Error: ", e)
            return ""

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def create_webpage_title(self, content):
        """Scrape a Webpage"""

        tokens = num_tokens_from_string(content, "gpt-4")

        if tokens >= 32000 - 1500:
            raise ValueError(f"Content too long, tokens found {tokens}")

        messages = [{"role": "user", "content": f"Generate a title for a website based on the following content: {content}"}]

        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0.7, max_tokens=50, n=1
        )

        outline = chat_completion.choices[0].message.content
        # outline = re.sub(r"\n+", " ", outline)
        # outline = re.sub(r"\s+", " ", outline)

        return outline
