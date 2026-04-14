from ingestion.tmdb_api_client import fetch_movie

data = fetch_movie(299534)

if data:
    print("Movie:", data.get("title"))
else:
    print("API failed")
