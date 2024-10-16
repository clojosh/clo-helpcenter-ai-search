import json
import multiprocessing
import os
import ssl
from datetime import datetime
from io import BytesIO

import questionary
import requests
import shortuuid
from pytube import Channel, Playlist, extract
from tqdm import tqdm
from youtube_transcript_api import YouTubeTranscriptApi

from tools.environment import Environment
from tools.misc import check_create_directory, sanitize_directory_file_name, trim_tokens

ssl._create_default_https_context = ssl._create_stdlib_context


class YouTube:
    def __init__(self, environment: Environment):
        self.env = environment.env
        self.brand = environment.brand
        self.language = environment.language
        self.youtube_path = os.path.join(environment.ai_search_dir, environment.brand, "youtube")
        self.youtube_channel_path = os.path.join(environment.ai_search_dir, environment.brand, "youtube", "channel")
        self.youtube_playlist_path = os.path.join(environment.ai_search_dir, environment.brand, "youtube", "playlist")
        self.zendesk_youtube_path = os.path.join(
            environment.ai_search_dir, environment.brand, "youtube", "zendesk", environment.get_locale(environment.language)
        )
        self.zendesk_article_api_endpoint = environment.zendesk_article_api_endpoint
        self.openai_helper = environment.openai_helper
        self.search_client = environment.search_client

    def extract_video_transcript(self, video_id):
        """Returns transcript of a youtube video"""
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id=video_id, languages=["en"])
            with open(f"{os.path.join(self.youtube_path, f'{video_id}.json')}", "w+", encoding="utf-8") as f:
                json.dump(transcript, f, ensure_ascii=False, indent=4)
        except:
            pass

    def extract_video_transcript_text(video_id):
        """Returns only the text of a transcript from a youtube video"""

        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id=video_id, languages=["en"])
            combined_transcript_text = ""
            for t in transcript:
                combined_transcript_text += t["text"].strip().replace("[Music]", " ").replace("foreign", " ") + " "

            return combined_transcript_text

        except:
            return ""

    def extract_youtube_channel_transcripts(resp_objects, youtube_channel_path, page):
        """Extracts transcripts from a youtube channel"""

        videos = []

        for item in resp_objects["items"]:
            print("Getting transcript for video: " + item["snippet"]["title"])

            if "videoId" not in item["id"]:
                continue

            video = {
                "ArticleId": shortuuid.uuid(),
                "VideoId": item["id"]["videoId"],
                "Source": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                "Title": item["snippet"]["title"].replace("&#39;", "'").replace("&quot;", '"').replace("&amp;", "&"),
                "publishedAt": item["snippet"]["publishedAt"],
            }

            video["Transcript"] = YouTube.extract_video_transcript_text(item["id"]["videoId"])
            videos.append(video)

        with open(f"{os.path.join(youtube_channel_path, f'page_{page}')}.json", "w+", encoding="utf-8") as f:
            json.dump(videos, f, ensure_ascii=False, indent=4)

    def mp_extract_youtube_channel_transcripts(self):
        """Use multiprocessing to extract transcripts from a youtube channel"""

        published_after = "{}-01-01T00:00:00Z".format(datetime.today().year - 2)

        response = requests.request(
            "GET",
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "id, snippet",
                "channelId": "UCApF8J_2QeJ8QPXIAZ25uhw",
                "key": "AIzaSyC2LupTSVApfy90Bfzq8L5AAkAawOmT0gY",
                # "publishedAfter": published_after,
                "order": "date",
                "maxResults": 30,
            },
            headers={
                "Content-Type": "application/json",
            },
        )

        resp_objects = json.loads(response.text)

        extract_youtube_channel_transcripts_params = []
        page = 0
        while "nextPageToken" in resp_objects:
            extract_youtube_channel_transcripts_params.append((resp_objects, self.youtube_channel_path, page))

            response = requests.request(
                "GET",
                "https://www.googleapis.com/youtube/v3/search",
                params={
                    "part": "id, snippet",
                    "channelId": "UCApF8J_2QeJ8QPXIAZ25uhw",
                    "key": "AIzaSyC2LupTSVApfy90Bfzq8L5AAkAawOmT0gY",
                    # "publishedAfter": published_after,
                    "order": "date",
                    "maxResults": 30,
                    "pageToken": resp_objects["nextPageToken"],
                },
                headers={
                    "Content-Type": "application/json",
                },
            )

            resp_objects = json.loads(response.text)

            page += 1

        with multiprocessing.Pool(5) as p:
            p.starmap_async(
                YouTube.extract_youtube_channel_transcripts, extract_youtube_channel_transcripts_params, error_callback=lambda e: print(e)
            )
            p.close()
            p.join()

    def extract_youtube_playlist_transcripts(self, playlist_url):
        """Extract the transcripts of every video in a playlist and save them into a JSON file"""
        playlist_title, videos = self.extract_youtube_playlist_videos(playlist_url)
        playlist_title = sanitize_directory_file_name(playlist_title)

        transcripts = []
        for video in tqdm(videos, desc="Extractng YouTube Transcripts", colour="blue", leave=False):
            transcript = {
                "Title": video["title"],
                "Source": video["url"],
                "Transcript": self.extract_video_transcript_text(video["url"]),
                "Labels": [],
                "YoutubeLinks": [video["url"]],
            }
            transcripts.append(transcript)

        for transcript in transcripts:
            buffer = []
            playlist_dir_path = os.path.join(self.youtube_playlist_path, playlist_title)
            check_create_directory(playlist_dir_path)

            buffer.append(transcript)
            with open(f"{os.path.join(playlist_dir_path,sanitize_directory_file_name(transcript['Title']))}.json", "w+", encoding="utf-8") as f:
                json.dump(buffer, f, ensure_ascii=False, indent=4)

        return playlist_title

    def summarize_transcripts(env, brand, language, file_path):
        """Summarize every transcript inside a folder"""
        print("Summarizing: " + os.path.split(file_path)[1])

        environment = Environment(env, brand, language)

        with open(file_path, "r", encoding="utf-8") as file:
            transcripts = json.load(file)

            for index, trans in enumerate(transcripts):
                if len(trans["Transcript"]) > 450:
                    summary = environment.openai_helper.generate_transcript_summary(trans["Transcript"])
                else:
                    summary = ""
                transcripts[index]["Summary"] = summary

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(transcripts, file, ensure_ascii=False, indent=4)

    def mp_summarize_transcripts(self):
        files = os.listdir(self.youtube_channel_path)

        summarize_transcripts_params = []
        for file in files:
            summarize_transcripts_params.append((self.env, self.brand, self.language, os.path.join(self.youtube_channel_path, file)))

        with multiprocessing.Pool(5) as p:
            p.starmap_async(YouTube.summarize_transcripts, summarize_transcripts_params, error_callback=lambda e: print(e))
            p.close()
            p.join()

    def upload_transcripts(self):
        files = os.listdir(self.youtube_channel_path)

        for file in tqdm(files, desc="Uploading YouTube Transcripts", colour="green", position=0, leave=True):
            with open(os.path.join(self.youtube_channel_path, file), "r", encoding="utf-8") as f:
                transcripts = json.load(f)

                upload_transcripts = []
                for transcript in transcripts:
                    if "sorry" in transcript["Summary"] or transcript["Summary"] == "":
                        continue

                    upload_transcripts.append(
                        {
                            "@search.action": "mergeOrUpload",
                            "ArticleId": transcript["ArticleId"],
                            "Source": transcript["Source"],
                            "Title": transcript["Title"],
                            "Content": transcript["Summary"],
                            # "Labels": self.openai_helper.generate_labels(transcript["Summary"]),
                            "YoutubeLinks": [transcript["Source"]],
                            "titleVector": self.openai_helper.generate_embeddings(text=transcript["Title"]),
                            "contentVector": self.openai_helper.generate_embeddings(text=transcript["Summary"]),
                        }
                    )

                self.search_client.upload_documents(upload_transcripts)


if __name__ == "__main__":
    env = questionary.select("Which environment?", choices=["prod", "dev"]).ask()
    brand = questionary.select("Which brand?", choices=["clo3d", "closet"]).ask()
    task = questionary.select(
        "What task?",
        choices=["Get Youtube Channel Transcripts", "Summarize Youtube Channel Transcripts", "Upload Transcripts"],
    ).ask()
    # language = questionary.select("What language?", choices=["English", "Korean"]).ask()

    yt = YouTube(Environment(env, brand))

    if task == "Get Youtube Channel Transcripts":
        yt.mp_extract_youtube_channel_transcripts()
    elif task == "Summarize Youtube Channel Transcripts":
        yt.mp_summarize_transcripts()
    elif task == "Upload Transcripts":
        yt.upload_transcripts()
    yt.extract_video_transcript("A_fkc7n4fzU")
