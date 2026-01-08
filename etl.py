import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

TMDB_TOKEN = os.getenv("TMDB_BEARER_TOKEN")
TMDB_BASE_URL = "https://api.themoviedb.org/3"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def tmdb_get(path: str, params: dict | None = None) -> dict:
    url = f"{TMDB_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {TMDB_TOKEN}"}
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# GENRES

def upsert_genres(conn, genres: list[dict]):
    rows = [(g["id"], g["name"]) for g in genres]

    sql = """
        INSERT INTO genres (tmdb_genre_id, name)
        VALUES %s
        ON CONFLICT (tmdb_genre_id)
        DO UPDATE SET name = EXCLUDED.name
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

# MOVIES

def fetch_popular_movies(pages: int = 1) -> list[dict]:
    """
    Pull N pages from /movie/popular.
    Each page is usually ~20 movies.
    """
    all_movies = []
    for page in range(1, pages + 1):
        data = tmdb_get("/movie/popular", params={"page": page})
        results = data.get("results", [])
        all_movies.extend(results)
        print(f"Fetched page {page}: {len(results)} movies")

    return all_movies

def upsert_movies(conn, movies: list[dict]):
    """
    Upsert movies into movies table.
    NOTE: /movie/popular does NOT include runtime, so we set runtime_minutes to NULL for now.
    """
    rows = []
    for m in movies:
        rows.append((
            m["id"],                        
            m.get("title"),
            m.get("original_title"),
            m.get("overview"),
            m.get("release_date") or None,    
            m.get("original_language"),
            m.get("popularity"),
            m.get("vote_average"),
            m.get("vote_count"),
            None                              
        ))

    sql = """
        INSERT INTO movies (
          tmdb_movie_id, title, original_title, overview,
          release_date, language, popularity, vote_average, vote_count,
          runtime_minutes
        )
        VALUES %s
        ON CONFLICT (tmdb_movie_id)
        DO UPDATE SET
          title = EXCLUDED.title,
          original_title = EXCLUDED.original_title,
          overview = EXCLUDED.overview,
          release_date = EXCLUDED.release_date,
          language = EXCLUDED.language,
          popularity = EXCLUDED.popularity,
          vote_average = EXCLUDED.vote_average,
          vote_count = EXCLUDED.vote_count
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def load_movie_genres(conn, movies: list[dict]):
    """
    Populate movie_genre junction table from /movie/popular data.
    Uses INSERT ... ON CONFLICT DO NOTHING so it’s safe to rerun.
    """
    rows = []

    for m in movies:
        tmdb_movie_id = m["id"]
        for tmdb_genre_id in m.get("genre_ids", []):
            rows.append((tmdb_movie_id, tmdb_genre_id))

    if not rows:
        print("No movie_genre rows to insert")
        return

    sql = """
        INSERT INTO movie_genre (tmdb_movie_id, tmdb_genre_id)
        VALUES %s
        ON CONFLICT (tmdb_movie_id, tmdb_genre_id)
        DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)

    conn.commit()
    print(f"Inserted {len(rows)} movie_genre links ")

def dedupe_by_tmdb_id(movies: list[dict]) -> list[dict]:
    """
    Remove duplicates by TMDB movie id while preserving the latest occurrence.
    """
    by_id = {}
    for m in movies:
        by_id[m["id"]] = m
    return list(by_id.values())

def print_duplicate_movie_ids(movies: list[dict]):
    seen = set()
    dups = set()
    for m in movies:
        mid = m["id"]
        if mid in seen:
            dups.add(mid)
        seen.add(mid)
    if dups:
        print("Duplicate TMDB movie IDs found:", sorted(list(dups))[:20])
    else:
        print("No duplicate TMDB movie IDs found")



def main():
    conn = get_conn()
    try:
        genre_data = tmdb_get("/genre/movie/list")
        genres = genre_data.get("genres", [])
        upsert_genres(conn, genres)
        print(f"Upserted {len(genres)} genres ")

        
        popular_movies = fetch_popular_movies(pages=2)
        print_duplicate_movie_ids(popular_movies)

        popular_movies = dedupe_by_tmdb_id(popular_movies)
        print(f"After dedupe: {len(popular_movies)} unique movies")

        upsert_movies(conn, popular_movies)
        print(f"Upserted {len(popular_movies)} movies ")

        load_movie_genres(conn, popular_movies)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
