# Movie-Data-ETL-Pipeline

Movie Data ETL Pipeline (TMDB API → PostgreSQL)
Overview

This project is an end-to-end ETL pipeline that extracts real movie data from the TMDB (The Movie Database) API, transforms it into a normalized relational model, and loads it into a PostgreSQL database for analytics.

The pipeline demonstrates:

API integration

relational database design

many-to-many relationships

idempotent ETL patterns

analytical SQL views

Architecture
TMDB API
   ↓
Python ETL (02_etl.py)
   ↓
PostgreSQL (movie_db)
   ↓
Analytics Views (SQL)

Database Schema

The database is fully normalized and includes the following tables:

movies — core movie metadata (title, release date, ratings, popularity)

genres — official TMDB genres

actors — cast members (people)

movie_genre — many-to-many relationship between movies and genres

movie_actor — many-to-many relationship between movies and actors (includes character name and billing order)

Schema is defined in:

sql/01_schema.sql

ETL Pipeline

The ETL process is implemented in Python using:

requests (API calls)

psycopg2 (PostgreSQL access)

python-dotenv (environment variables)

ETL Steps

Extract

Fetch genre list from TMDB

Fetch popular movies (paginated)

Fetch movie credits (cast) per movie

Transform

Deduplicate movies by TMDB ID

Normalize nested API responses

Limit cast size per movie for performance

Load

Upsert data into PostgreSQL tables

Populate junction tables safely (ON CONFLICT)

Pipeline is fully idempotent (safe to rerun)

ETL script:

02_etl.py

Analytics Views

The project includes SQL views for analytics and reporting:

Top Actors (by number of movies)

Top Genres (by movie count)

Genre Ratings (average rating, popularity, vote totals)

Top Actors by Average Movie Rating

Views are defined in:

sql/03_views.sql


Example:

SELECT * FROM v_top_actors LIMIT 20;

Setup Instructions
Prerequisites

Python 3.10+

PostgreSQL (Docker or local)

TMDB API account

1. Clone the repository
git clone https://github.com/yourusername/Movie-Data-ETL-Pipeline.git
cd Movie-Data-ETL-Pipeline

2. Install dependencies
pip install -r requirements.txt

3. Configure environment variables

Copy the example file:

cp .env.example .env


Fill in:

TMDB_BEARER_TOKEN=your_tmdb_v4_token
DB_HOST=localhost
DB_PORT=5433
DB_NAME=movie_db
DB_USER=admin
DB_PASSWORD=admin123


⚠️ .env is ignored by Git for security.

4. Create the database schema
psql -h localhost -p 5433 -U admin -d movie_db -f sql/01_schema.sql

5. Run the ETL pipeline
python 02_etl.py

6. Create analytics views
psql -h localhost -p 5433 -U admin -d movie_db -f sql/03_views.sql