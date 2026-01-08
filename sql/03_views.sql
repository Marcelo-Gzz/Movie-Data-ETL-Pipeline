

-- top actors view
CREATE OR REPLACE VIEW v_top_actors AS
SELECT
  a.tmdb_person_id,
  a.name,
  COUNT(DISTINCT ma.tmdb_movie_id) AS movie_count
FROM actors a
JOIN movie_actor ma
  ON ma.tmdb_person_id = a.tmdb_person_id
GROUP BY a.tmdb_person_id, a.name
ORDER BY movie_count DESC, a.name;


-- top genre view
CREATE OR REPLACE VIEW v_top_genres AS
SELECT
  g.tmdb_genre_id,
  g.name AS genre_name,
  COUNT(DISTINCT mg.tmdb_movie_id) AS movie_count
FROM genres g
JOIN movie_genre mg
  ON mg.tmdb_genre_id = g.tmdb_genre_id
GROUP BY g.tmdb_genre_id, g.name
ORDER BY movie_count DESC, genre_name;


-- rating analytics view
CREATE OR REPLACE VIEW v_genre_ratings AS
SELECT
  g.name AS genre_name,
  COUNT(DISTINCT m.tmdb_movie_id) AS movies_in_genre,
  ROUND(AVG(m.vote_average)::numeric, 2) AS avg_rating,
  SUM(m.vote_count) AS total_votes,
  ROUND(AVG(m.popularity)::numeric, 2) AS avg_popularity
FROM genres g
JOIN movie_genre mg
  ON mg.tmdb_genre_id = g.tmdb_genre_id
JOIN movies m
  ON m.tmdb_movie_id = mg.tmdb_movie_id
GROUP BY g.name
ORDER BY avg_rating DESC, movies_in_genre DESC;


-- top actors by average movie rating
CREATE OR REPLACE VIEW v_top_actors_by_rating AS
SELECT
  a.tmdb_person_id,
  a.name,
  COUNT(DISTINCT ma.tmdb_movie_id) AS movie_count,
  ROUND(AVG(m.vote_average)::numeric, 2) AS avg_movie_rating,
  SUM(m.vote_count) AS total_votes
FROM actors a
JOIN movie_actor ma
  ON ma.tmdb_person_id = a.tmdb_person_id
JOIN movies m
  ON m.tmdb_movie_id = ma.tmdb_movie_id
GROUP BY a.tmdb_person_id, a.name
HAVING COUNT(DISTINCT ma.tmdb_movie_id) >= 3
ORDER BY avg_movie_rating DESC, movie_count DESC, total_votes DESC;
