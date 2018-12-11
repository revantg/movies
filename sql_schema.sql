CREATE TABLE download_links (
  rarbgprx_link VARCHAR(50) PRIMARY KEY,
  movie_name VARCHAR(100),
  download_name VARCHAR(100),
  release_name VARCHAR(200),
  imdb_id VARCHAR(10),
  torrent_link VARCHAR(200),
  magnet_link VARCHAR(700),
  size VARCHAR(15),
  date_added DATETIME,
  torrent_status VARCHAR(50),
  quality VARCHAR(15),
  res  VARCHAR(15),
  sound VARCHAR(15),
  codec VARCHAR(15),
  seeds VARCHAR(15),
  leeches VARCHAR(15),
  tags VARCHAR(200)
)

file_name VARCHAR(100),
file_size VARCHAR(20),
imdb_id VARCHAR(20),

CREATE TABLE scraped_movie_info (
  imdb_id VARCHAR(20),
  imdb_link VARCHAR(70),
  rarbgprx_rating DECIMAL(5, 3),
  poster_img VARCHAR(200),
  category VARCHAR(20),
  category_link VARCHAR(50),
  description_imgs VARCHAR(1000),
  trailer_link VARCHAR(70),
  pg_rating VARCHAR(20),
  imdb_rating VARCHAR(10),
  metacritic_rating VARCHAR(10),
  rotten_tomatoes_popcorn VARCHAR(10),
  rotten_tomatoes_tomatoes VARCHAR(10),
  genres VARCHAR(100),
  actors VARCHAR(500)
)
