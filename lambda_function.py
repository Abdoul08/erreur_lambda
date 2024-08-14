#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import json
import yt_dlp
import whisper
import warnings
from googleapiclient.discovery import build
import boto3

# Désactiver les avertissements pour FP16
warnings.filterwarnings("ignore", message="FP16 is not supported on CPU; using FP32 instead")

# Définir les variables d'environnement directement dans le code
API_KEY = ''  # Remplacez par votre clé API
BUCKET_NAME = 'youtube-audio-transcriptions-2024'  # Remplacez par le nom de votre bucket S3

# Configurer l'API YouTube
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Configurer le client S3
s3 = boto3.client('s3')

def get_channel_ids(channel_names):
    """
    Obtient les IDs des chaînes YouTube en fonction de leurs noms.

    :param channel_names: Liste des noms de chaînes YouTube
    :return: Dictionnaire des noms de chaînes avec leurs IDs respectifs
    """
    channel_ids = {}
    for name in channel_names:
        search_response = youtube.search().list(
            q=name,
            type='channel',
            part='id,snippet',
            maxResults=1
        ).execute()

        if search_response['items']:
            channel_id = search_response['items'][0]['id']['channelId']
            channel_ids[name] = channel_id
        else:
            print(f"Aucune chaîne trouvée pour le nom: {name}")

    return channel_ids

def search_videos(keywords, channel_ids, max_results_per_channel=3):
    """
    Recherche des vidéos en fonction des mots clés et des chaînes spécifiés via l'API YouTube.

    :param keywords: Liste de mots clés à rechercher dans les titres des vidéos
    :param channel_ids: Dictionnaire des noms de chaînes avec leurs IDs respectifs
    :param max_results_per_channel: Nombre maximum de vidéos à rechercher par mot-clé et par chaîne
    :return: Liste des vidéos trouvées sous forme de tuples (URL, titre, ID, chaîne)
    """
    search_results = []

    for keyword in keywords:
        for channel_id in channel_ids.values():
            search_response = youtube.search().list(
                q=keyword,
                type='video',
                part='id,snippet',
                maxResults=max_results_per_channel,
                channelId=channel_id,
            ).execute()

            for item in search_response['items']:
                video_title = item['snippet']['title']
                video_id = item['id']['videoId']
                video_url = f'https://www.youtube.com/watch?v={video_id}'
                video_channel = item['snippet']['channelTitle']
                published_date = item['snippet']['publishedAt']
                search_results.append((video_url, video_title, video_id, video_channel, published_date))

    return search_results

def transcribe_audio(temp_file_path, model):
    """
    Transcrit l'audio du fichier temporaire en texte en utilisant Whisper.

    :param temp_file_path: Chemin vers le fichier audio temporaire
    :param model: Modèle Whisper préchargé
    :return: Texte transcrit
    """
    try:
        result = model.transcribe(temp_file_path)
        return result['text']
    except Exception as e:
        print(f"Erreur lors de la transcription de {temp_file_path}: {e}")
        return ""

def download_and_transcribe_videos(channel_ids, keywords, bucket_name, max_videos=5):
    """
    Télécharge les audios des vidéos YouTube en fonction des chaînes et des mots clés spécifiés,
    transcrit les audios et enregistre les transcriptions sur S3, avec une vérification des doublons.

    :param channel_ids: Dictionnaire des noms de chaînes avec leurs IDs respectifs
    :param keywords: Liste de mots clés à rechercher dans les titres des vidéos
    :param bucket_name: Nom du bucket S3 où les transcriptions seront enregistrées
    :param max_videos: Nombre maximum de vidéos à traiter par exécution
    """
    model = whisper.load_model("base")

    videos = search_videos(keywords, channel_ids, max_results_per_channel=max_videos)

    for video_url, video_title, video_id, video_channel, published_date in videos:
        print(f"Téléchargement de: {video_title} de la chaîne {video_channel}")

        transcription_file_path = f"transcriptions/video_{video_id}.json"

        # Vérifier si le fichier existe déjà dans le bucket S3
        try:
            s3.head_object(Bucket=bucket_name, Key=transcription_file_path)
            print(f"Transcription existe déjà pour la vidéo {video_id}. Passer au suivant.")
            continue
        except:
            pass

        try:
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': f'temp_{video_id}.%(ext)s',
                'quiet': True
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(video_url, download=True)
                temp_file_path = f"temp_{info_dict['id']}.webm"  # Assurez-vous du format du fichier

            transcription_text = transcribe_audio(temp_file_path, model)
            print(f"Transcription pour {video_title}: {transcription_text[:100]}...")

            transcription_data = {
                "channel_name": video_channel,
                "video_id": video_id,
                "title": video_title,
                "published_date": published_date,
                "video_link": video_url,
                "transcription": transcription_text
            }
            json_data = json.dumps(transcription_data, ensure_ascii=False, indent=4)  # ensure_ascii=False pour éviter l'encodage des caractères non-ASCII

            # Télécharger la transcription sur S3
            s3.put_object(Bucket=bucket_name, Key=transcription_file_path, Body=json_data, ContentType='application/json')
            print(f"Transcription stockée sur S3: s3://{bucket_name}/{transcription_file_path}")

            # Supprimer le fichier temporaire
            os.remove(temp_file_path)

        except Exception as e:
            print(f"Erreur lors du traitement de {video_url}: {e}")

def lambda_handler(event, context):
    channel_names = ["FRANCE 24"]  # Liste des noms de chaînes
    channel_ids = get_channel_ids(channel_names)  # Obtenir les IDs des chaînes

    keywords = ["JO 2024"]  # Liste de mots-clés

    download_and_transcribe_videos(channel_ids, keywords, BUCKET_NAME, max_videos=3)
    return {
        'statusCode': 200,
        'body': json.dumps('Transcription terminée')
    }

