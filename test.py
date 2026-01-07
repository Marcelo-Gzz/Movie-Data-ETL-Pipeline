import requests, os
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("TMDB_BEARER_TOKEN")

headers = {"Authorization": f"Bearer {token}"}
r = requests.get(
    "https://api.themoviedb.org/3/genre/movie/list",
    headers=headers
)

print(r.status_code)
print(r.json())
