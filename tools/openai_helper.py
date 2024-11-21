import json
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from openai import AzureOpenAI
from tenacity import retry, stop_after_attempt, wait_random_exponential

from .misc import trim_tokens

backend_dir = Path(__file__).parent.parent.parent

sys.path.append(str(backend_dir))
from tools.misc import num_tokens_from_string

# https://learn.microsoft.com/en-us/azure/ai-services/openai/concepts/models?tabs=python-secure%2Cglobal-standard%2Cstandard-chat-completions#gpt-4o-and-gpt-4-turbo
GPT_4_MINI_MAX_INPUT_TOKENS = 128000
GPT_4_MINI_MAX_OUTPUT_TOKENS = 16000
EMBEDDING_ADA_002_MAX_INPUT_TOKENS = 8191


class OpenAIHelper:
    def __init__(
        self,
        openai_client: AzureOpenAI,
        AZURE_OPENAI_CHATGPT_DEPLOYMENT,
        AZURE_OPENAI_EMB_DEPLOYMENT,
        language="English",
    ):
        self.openai_client = openai_client
        self.AZURE_OPENAI_CHATGPT_DEPLOYMENT = AZURE_OPENAI_CHATGPT_DEPLOYMENT
        self.AZURE_OPENAI_EMB_DEPLOYMENT = AZURE_OPENAI_EMB_DEPLOYMENT
        self.language = language

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_embeddings(self, text: str) -> list[float]:
        """
        Generate embeddings for a given text.

        Args:
            text (str): The text to generate embeddings for.

        Returns:
            list[float]: The generated embeddings.

        Raises:
            openai.error.OpenAIError: If the request to the OpenAI API fails.
        """
        tokens = num_tokens_from_string(text, "text-embedding-ada-002")

        if tokens >= EMBEDDING_ADA_002_MAX_INPUT_TOKENS:
            text = text[:EMBEDDING_ADA_002_MAX_INPUT_TOKENS]

        return (
            self.openai_client.embeddings.create(
                input=[text],
                model=self.AZURE_OPENAI_EMB_DEPLOYMENT,
            )
            .data[0]
            .embedding
        )

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_questions(self, text: str) -> str:
        """
        Generate questions from a given text.

        Args:
            text (str): The text to generate questions from.

        Returns:
            str: The generated questions.

        Raises:
            openai.error.OpenAIError: If the request to the OpenAI API fails.
        """
        # Check if the text length exceeds the maximum allowed input length
        tokens = num_tokens_from_string(text, "gpt-4o-mini")

        if tokens >= GPT_4_MINI_MAX_INPUT_TOKENS:
            # Trim the text to the maximum allowed length
            text = text[:GPT_4_MINI_MAX_INPUT_TOKENS]

        # Create a list of messages to send to the OpenAI API
        messages = [
            # The first message is the text to generate questions from
            {"role": "user", "content": f"Generate 10 brief and concise questions a customer would ask about this in {self.language}: {text}"},
        ]

        # Use the OpenAI API to generate the questions
        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT,
            messages=messages,
            temperature=0.7,
            max_tokens=200,
            n=1,
        )

        # Extract the generated questions from the response
        questions = chat_completion.choices[0].message.content

        # Remove any numbers at the start of each line
        questions = re.sub("^[0-9]+\.\s", "", questions, flags=re.MULTILINE)

        # Replace any newlines with spaces
        questions = re.sub("\n+", " ", questions, flags=re.MULTILINE)

        return questions

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_labels(self, text: str) -> list[str]:
        """
        Generate keywords from a given text.

        Args:
            text (str): The text to generate keywords from.

        Returns:
            list[str]: A list of keywords.
        """
        # 50 tokens or less, generate 5 keywords
        # otherwise, generate 10 keywords
        tokens = num_tokens_from_string(text, "gpt-4o-mini")
        keywords_num = 10

        if tokens <= 50:
            keywords_num = 5

        if tokens >= GPT_4_MINI_MAX_INPUT_TOKENS:
            # truncate the text if it exceeds the maximum token limit
            text = text[:GPT_4_MINI_MAX_INPUT_TOKENS]

        messages = [
            {
                "role": "user",
                "content": f"Generate {keywords_num} keywords from the this in {self.language}: {text}",
            }
        ]

        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT,
            messages=messages,
            temperature=0,
            max_tokens=200,
            n=1,
        )

        # split the response into individual lines, strip whitespace, and remove numbers
        labels = chat_completion.choices[0].message.content.splitlines()
        for i, l in enumerate(labels):
            labels[i] = re.sub("[0-9]+\.*\)*\s*", "", l, flags=re.MULTILINE).strip()

        return labels

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_transcript_summary(self, transcript: str) -> str:
        """
        Summarize a transcript

        Args:
            transcript (str): The text of the transcript to summarize

        Returns:
            str: A summary of the transcript
        """

        # If the transcript is too long, trim it to a length that OpenAI can handle
        tokens = num_tokens_from_string(transcript, "gpt-4o-mini")

        if tokens >= GPT_4_MINI_MAX_INPUT_TOKENS:
            transcript = transcript[:GPT_4_MINI_MAX_INPUT_TOKENS]

        # Create the prompt for the AI
        messages = [
            {
                "role": "user",
                "content": f"Provide a comprehensive guide of the given transcript. Include all step-by-step instructions, definitions, and tips and tricks. {transcript}",
            }
        ]

        # Ask the AI to generate a summary
        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0.7, max_tokens=2000, n=1
        )

        # Extract the summary from the response
        summary = chat_completion.choices[0].message.content
        summary = re.sub(r"\n+", " ", summary)
        summary = re.sub(r"\s+", " ", summary)

        return summary

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def generate_pdf_summary(self, pdf):
        """
        Summarize a PDF

        Args:
            pdf (str): The text of the PDF to summarize

        Returns:
            str: A summary of the PDF
        """
        # If the PDF is too long, trim it to a length that OpenAI can handle
        tokens = num_tokens_from_string(pdf, "gpt-4o-mini")

        if tokens >= GPT_4_MINI_MAX_INPUT_TOKENS:
            pdf = pdf[:GPT_4_MINI_MAX_INPUT_TOKENS]

        # Create the prompt for the AI
        messages = [
            {
                "role": "user",
                "content": f"Provide a comprehensive guide of the given text. Include all step-by-step instructions, definitions, and warranties. {pdf}",
            }
        ]

        # Ask the AI to generate a summary
        chat_completion = self.openai_client.chat.completions.create(
            model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0.7, max_tokens=1000, n=1
        )

        # Extract the summary from the response
        summary = chat_completion.choices[0].message.content
        summary = re.sub(r"\n+", " ", summary)
        summary = re.sub(r"\s+", " ", summary)

        return summary

    @retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
    def outline_webpage(self, content, website_url):
        """
        Outlines a webpage based on the content of the webpage.

        Args:
            content (str): The HTML content of the webpage
            website_url (str): The URL of the webpage

        Returns:
            str: A detailed outline of the webpage
        """
        try:
            # The maximum amount of tokens that can be processed by the AI is 32,000 - 1,500
            # If the content is longer than this, trim it to this length
            tokens = num_tokens_from_string(content, "gpt-4o-mini")

            if tokens >= 32000 - 1500:
                raise ValueError(f"Content too long, tokens found {tokens}")

            # Create a prompt for the AI
            messages = [
                {
                    "role": "user",
                    "content": f"""
                        Provide a detailed outline of a website based on the provided HTML code.
                        Outline must include all text and web links whereever possible, for example:
                        [Start Free Trial](https://clo3d.com).
                        The outline must exclude any html tags.
                        The url of the website is {website_url}.
                        ###HTML Code###: {content}
                    """,
                }
            ]

            # Ask the AI to generate an outline
            chat_completion = self.openai_client.chat.completions.create(
                model=self.AZURE_OPENAI_CHATGPT_DEPLOYMENT, messages=messages, temperature=0, max_tokens=1500, n=1
            )

            # Extract the outline from the response
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
            tokens = num_tokens_from_string(content, "gpt-4o-mini")

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

        tokens = num_tokens_from_string(content, "gpt-4o-mini")

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
