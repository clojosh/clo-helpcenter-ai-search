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

from tools.azure_env import AzureEnv
from tools.misc import check_create_directory, logger, sanitize_directory_file_name

ssl._create_default_https_context = ssl._create_stdlib_context


class YouTube:
    def __init__(self, azure_env: AzureEnv):
        self.azure_env = azure_env

        if not os.path.exists(os.path.join(azure_env.brand, "youtube")):
            os.makedirs(os.path.join(azure_env.brand, "youtube"))
        self.youtube_dir_path = os.path.join(azure_env.brand, "youtube")

        if not os.path.exists(os.path.join(azure_env.brand, "youtube", "channel")):
            os.makedirs(os.path.join(azure_env.brand, "youtube", "channel"))
        self.youtube_channel_dir_path = os.path.join(azure_env.brand, "youtube", "channel")

        if not os.path.exists(os.path.join(azure_env.brand, "youtube", "playlist")):
            os.makedirs(os.path.join(azure_env.brand, "youtube", "playlist"))
        self.youtube_playlist_dir_path = os.path.join(azure_env.brand, "youtube", "playlist")

    @staticmethod
    def extract_video_transcript(youtube_dir_path: str, video_id: str) -> None:
        """
        Saves a transcript of a youtube video to a json file.

        Args:
            youtube_dir_path (str): The path to the directory where the transcript will be saved.
            video_id (str): The id of the video to extract the transcript for.

        Returns:
            None
        """
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id=video_id, languages=["en"])
            with open(f"{os.path.join(youtube_dir_path, f'{video_id}.json')}", "w+", encoding="utf-8") as f:
                json.dump(transcript, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(e)
            pass

    @staticmethod
    def extract_video_transcript_text(video_id: str) -> str:
        """
        Returns only the text of a transcript from a youtube video

        Args:
            video_id (str): The id of the video to extract the transcript for.

        Returns:
            str: The text from the transcript.
        """
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id=video_id, languages=["en"])

            combined_transcript_text = ""
            for t in transcript:
                combined_transcript_text += t["text"].strip().replace("[Music]", " ").replace("foreign", " ") + " "

            return combined_transcript_text

        except Exception:
            logger("Error", "No Transcripts found for " + "https://www.youtube.com/watch?v=" + video_id)

            return ""

    @staticmethod
    def extract_youtube_channel_transcripts(resp_objects: dict[str, str], youtube_channel_dir_path: str, page: int) -> None:
        """
        Extracts transcripts from a youtube channel

        Args:
            resp_objects (dict): The response from the youtube api
            youtube_channel_path (str): The path to save the transcripts
            page (int): The page number to be processed

        Returns:
            None
        """
        videos = []

        for item in resp_objects["items"]:
            # Print the title of the video being processed
            print("Retrieving transcript for:\n" + item["snippet"]["title"] + "\n")

            # Check if the item is a video and has a video id
            if "videoId" not in item["id"]:
                continue

            # Create a dictionary to store the video data
            video = {
                "ArticleId": shortuuid.uuid(),  # Generate a unique id for the video
                "VideoId": item["id"]["videoId"],  # Get the video id
                "Source": f"https://www.youtube.com/watch?v={item['id']['videoId']}",  # Get the video url
                "Title": item["snippet"]["title"]
                .replace("&#39;", "'")
                .replace("&quot;", '"')
                .replace("&amp;", "&"),  # Get the video title and replace special characters
                "publishedAt": item["snippet"]["publishedAt"],  # Get the video publish date
            }

            # Add the transcript to the video data
            video["Transcript"] = YouTube.extract_video_transcript_text(item["id"]["videoId"])
            videos.append(video)

        # Save the transcripts to a json file
        with open(f"{os.path.join(youtube_channel_dir_path, f'page_{page}')}.json", "w+", encoding="utf-8") as f:
            json.dump(videos, f, ensure_ascii=False, indent=4)

    def mp_extract_youtube_channel_transcripts(self, video_age: int = 3) -> None:
        """Use multiprocessing to extract transcripts from a youtube channel"""

        published_after = "{}-01-01T00:00:00Z".format(datetime.today().year - video_age)

        page = 0
        resp_objects = {}
        extract_youtube_channel_transcripts_params = []
        while True:
            response = requests.request(
                "GET",
                "https://www.googleapis.com/youtube/v3/search",
                params={
                    "part": "id, snippet",
                    "channelId": "UCApF8J_2QeJ8QPXIAZ25uhw",
                    "key": "AIzaSyC2LupTSVApfy90Bfzq8L5AAkAawOmT0gY",
                    "publishedAfter": published_after,
                    "order": "date",
                    "maxResults": 30,
                    "pageToken": resp_objects["nextPageToken"] if "nextPageToken" in resp_objects else "",
                },
                headers={
                    "Content-Type": "application/json",
                },
            )

            resp_objects = json.loads(response.text)
            extract_youtube_channel_transcripts_params.append((resp_objects, self.youtube_channel_dir_path, page))
            page += 1

            if "nextPageToken" not in resp_objects:
                break

        # print(len(extract_youtube_channel_transcripts_params))

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
                "Transcript": YouTube.extract_video_transcript_text(video["url"]),
                "Labels": [],
                "YoutubeLinks": [video["url"]],
            }
            transcripts.append(transcript)

        for transcript in transcripts:
            buffer = []
            playlist_dir_path = os.path.join(self.youtube_playlist_dir_path, playlist_title)
            check_create_directory(playlist_dir_path)

            buffer.append(transcript)
            with open(f"{os.path.join(playlist_dir_path,sanitize_directory_file_name(transcript['Title']))}.json", "w+", encoding="utf-8") as f:
                json.dump(buffer, f, ensure_ascii=False, indent=4)

        return playlist_title

    @staticmethod
    def summarize_transcripts(env: str, brand: str, youtube_channel_dir_path: str):
        """
        Summarize all the transcripts in a file

        Args:
            env (str): The environment to use
            brand (str): The brand to use
            youtube_channel_dir_path (str): The path to the directory containing the transcripts of the youtube channel

        Returns:
            None
        """
        print("Summarizing ", os.path.split(youtube_channel_dir_path)[1].strip() + "\n")

        environment = AzureEnv(env, brand)

        # Read the transcripts from the file
        with open(youtube_channel_dir_path, "r", encoding="utf-8") as file:
            transcripts = json.load(file)

        # Summarize each transcript
        for index, trans in enumerate(transcripts):
            if len(trans["Transcript"]) > 450:
                summary = environment.openai_helper.generate_transcript_summary(trans["Transcript"])
            else:
                summary = ""
            transcripts[index]["Summary"] = summary

        with open(youtube_channel_dir_path, "w", encoding="utf-8") as file:
            json.dump(transcripts, file, ensure_ascii=False, indent=4)

    def mp_summarize_transcripts(self):
        """
        Summarize transcripts of a youtube channel in parallel

        This function will call `summarize_transcripts` on all files in `youtube_channel_dir_path`
        in parallel using 5 processes.

        See `summarize_transcripts` for more details on what is done.
        """
        # Get the list of files to process
        files = os.listdir(self.youtube_channel_dir_path)

        # Create a list to store the parameters to pass to `summarize_transcripts`
        summarize_transcripts_params = []

        # Iterate over the files and create the parameters
        for file in files:
            summarize_transcripts_params.append((self.azure_env.stage, self.azure_env.brand, os.path.join(self.youtube_channel_dir_path, file)))

        # Create a multiprocessing pool and process the files in parallel
        with multiprocessing.Pool(3) as p:
            p.starmap_async(YouTube.summarize_transcripts, summarize_transcripts_params, error_callback=lambda e: print(e))
            p.close()
            p.join()

    def upload_transcripts(self):
        files = os.listdir(self.youtube_channel_dir_path)

        for file in tqdm(files, desc="Uploading Transcripts", colour="green", position=0, leave=True):
            with open(os.path.join(self.youtube_channel_dir_path, file), "r", encoding="utf-8") as f:
                transcripts = json.load(f)

            upload_transcripts = []
            for transcript in transcripts:
                if transcript["Summary"] == "":
                    continue

                upload_transcripts.append(
                    {
                        "@search.action": "mergeOrUpload",
                        "ArticleId": transcript["ArticleId"],
                        "Source": transcript["Source"],
                        "Title": transcript["Title"],
                        "Content": transcript["Summary"],
                        "YoutubeLinks": [transcript["Source"]],
                        "titleVector": self.azure_env.openai_helper.generate_embeddings(text=transcript["Title"]),
                        "contentVector": self.azure_env.openai_helper.generate_embeddings(text=transcript["Summary"]),
                    }
                )

            self.azure_env.search_client.upload_documents(upload_transcripts)


if __name__ == "__main__":
    task = questionary.select(
        "What task?",
        choices=[
            "Get Transcripts",
            "Summarize Transcript",
            "Summarize All Transcripts",
            "Upload All Transcripts",
        ],
    ).ask()

    if task == "Get Transcripts":
        video_age = questionary.text("What video age(in years)?").ask()
        yt = YouTube(AzureEnv("dev", "clo3d"))
        yt.mp_extract_youtube_channel_transcripts(video_age=int(video_age))

    else:
        env = questionary.select("Which environment?", choices=["prod", "dev"]).ask()
        brand = questionary.select("Which brand?", choices=["allinone", "clo3d", "closet"]).ask()
        yt = YouTube(AzureEnv(env, brand))

        if task == "Summarize Transcript":
            youtube_channel_pages = sorted(os.listdir(yt.youtube_channel_dir_path), key=lambda x: int(x.split("_")[1].split(".")[0]))
            page = questionary.select("Which page?", choices=youtube_channel_pages).ask()
            YouTube.summarize_transcripts(env, brand, os.path.join(yt.youtube_channel_dir_path, page))

        elif task == "Summarize All Transcripts":
            yt.mp_summarize_transcripts()

        elif task == "Upload All Transcripts":
            yt.upload_transcripts()
