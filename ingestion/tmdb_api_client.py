import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3/movie"


def fetch_movie(movie_id):
    try:
        url = f"{BASE_URL}/{movie_id}?api_key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching movie {movie_id}: {e}")
        return None


def fetch_credits(movie_id):
    try:
        url = f"{BASE_URL}/{movie_id}/credits?api_key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching credits {movie_id}: {e}")
        return None
