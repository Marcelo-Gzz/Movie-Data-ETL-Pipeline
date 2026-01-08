
DROP TABLE IF EXISTS movie_actor;
DROP TABLE IF EXISTS movie_genre;
DROP TABLE IF EXISTS actors;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS movies;

-- 1) Movies
CREATE TABLE movies (
  movie_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_movie_id   INT NOT NULL UNIQUE,        
  title           TEXT NOT NULL,
  original_title  TEXT,
  overview        TEXT,
  release_date    DATE,
  language        VARCHAR(10),
  popularity      NUMERIC(10,3),
  vote_average    NUMERIC(4,2),
  vote_count      INT,
  runtime_minutes INT,
  created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 2) Genres
CREATE TABLE genres (
  genre_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_genre_id INT NOT NULL UNIQUE,          
  name          TEXT NOT NULL
);

-- 3) Actors 
CREATE TABLE actors (
  actor_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tmdb_person_id INT NOT NULL UNIQUE,         
  name           TEXT NOT NULL,
  gender         INT,                         
  popularity     NUMERIC(10,3),
  created_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 4) Movie <-> Genre 
CREATE TABLE movie_genre (
  tmdb_movie_id INT NOT NULL,
  tmdb_genre_id INT NOT NULL,
  PRIMARY KEY (tmdb_movie_id, tmdb_genre_id),
  FOREIGN KEY (tmdb_movie_id) REFERENCES movies (tmdb_movie_id) ON DELETE CASCADE,
  FOREIGN KEY (tmdb_genre_id) REFERENCES genres (tmdb_genre_id) ON DELETE CASCADE
);

-- 5) Movie <-> Actor 
CREATE TABLE movie_actor (
  tmdb_movie_id  INT NOT NULL,
  tmdb_person_id INT NOT NULL,
  cast_order     INT,         -- billing order
  character_name TEXT,
  PRIMARY KEY (tmdb_movie_id, tmdb_person_id),
  FOREIGN KEY (tmdb_movie_id) REFERENCES movies (tmdb_movie_id) ON DELETE CASCADE,
  FOREIGN KEY (tmdb_person_id) REFERENCES actors (tmdb_person_id) ON DELETE CASCADE
);

-- indexes for analytics/joins
CREATE INDEX idx_movies_vote_average ON movies (vote_average DESC);
CREATE INDEX idx_movies_popularity ON movies (popularity DESC);
CREATE INDEX idx_movie_actor_person ON movie_actor (tmdb_person_id);
CREATE INDEX idx_movie_genre_genre ON movie_genre (tmdb_genre_id);
