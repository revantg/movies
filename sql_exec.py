import pymysql
from creds import rds_host, master_username, master_password
import requests
import base64


def insert_torrent(data, conn):
    db_name = 'movies'
    # conn = pymysql.connect(rds_host, user=master_username, passwd=master_password, db=db_name, connect_timeout=5)

    with conn.cursor() as cur:
        cur.execute(
            '''INSERT INTO download_links
            (rarbgprx_link, movie_name, download_name, imdb_id, torrent_link, magnet_link, date_added, quality, res, sound, codec, torrent_status, seeds, leeches, size, release_name, tags)
            VALUES
            (
                "{rarbgprx_url}",
                "{movie_name}",
                "{download_name}",
                "{imdb_id}",
                "{torrent_link}",
                "{magnet_link}",
                "{date_added}",
                "{quality}",
                "{res}",
                "{sound}",
                "{codec}",
                "{torrent_status}",
                "{seeds}",
                "{leeches}",
                "{torrent_size}",
                "{release_name}",
                "{tags}"
            )
            '''.format(**data)
        )
        conn.commit()

def insert_movie_info(data, conn):
    with conn.cursor() as cur:
        query = ''' INSERT INTO scraped_movie_info
        (rarbgprx_link, movie_name, release_name, imdb_id, imdb_link, rarbgprx_rating, poster_img, category, category_link, description_imgs, trailer_link, pg_rating, imdb_rating,
        metacritic_rating, rotten_tomatoes_popcorn, rotten_tomatoes_tomatoes, genres, actors)
        VALUES(
            "{rarbgprx_url}",
            "{movie_name}",
            "{release_name}",
            "{imdb_id}",
            "{imdb_link}",
            "{rarbgprx_rating}",
            "{poster_img}",
            "{category}",
            "{category_link}",
            "{description_imgs}",
            "{trailer_link}",
            "{pg_rating}",
            "{imdb_rating}",
            "{metacritic_rating}",
            "{rotten_tomatoes_popcorn}",
            "{rotten_tomatoes_tomatoes}",
            "{genres}",
            "{actors}"
        )'''.format(**data)
        cur.execute(query)
        conn.commit()

def update_other_downloads(data, conn):
    query = "SELECT EXISTS(SELECT 1 FROM download_links from {});"

def insert_images(images, conn):
    with conn.cursor() as cur:
        for img in images:
            b = requests.get(img)
            img_content = b.content
            img_content_b64 = base64.b64encode(img_content)
            query = '''INSERT INTO images
                        (img_url, b64_img)
                        VALUES ("{img_url}", "{image}")
                    '''.format(img_url = img, image = img_content_b64.decode("utf-8") )
            print(query)
            cur.execute(query)
            conn.commit()


# +--------------------------+---------------+------+-----+---------+-------+
# | Field                    | Type          | Null | Key | Default | Extra |
# +--------------------------+---------------+------+-----+---------+-------+
# | imdb_id                  | varchar(20)   | YES  |     | NULL    |       |
# | imdb_link                | varchar(70)   | YES  |     | NULL    |       |
# | rarbgprx_rating          | decimal(5,3)  | YES  |     | NULL    |       |
# | poster_img               | varchar(200)  | YES  |     | NULL    |       |
# | category                 | varchar(20)   | YES  |     | NULL    |       |
# | category_link            | varchar(50)   | YES  |     | NULL    |       |
# | description_imgs         | varchar(1000) | YES  |     | NULL    |       |
# | trailer_link             | varchar(70)   | YES  |     | NULL    |       |
# | pg_rating                | varchar(20)       | YES  |     | NULL    |       |
# | imdb_rating              | varchar(10)   | YES  |     | NULL    |       |
# | metacritic_rating        | varchar(10)   | YES  |     | NULL    |       |
# | rotten_tomatoes_popcorn  | varchar(10)   | YES  |     | NULL    |       |
# | rotten_tomatoes_tomatoes | varchar(10)   | YES  |     | NULL    |       |
# | genres                   | varchar(100)  | YES  |     | NULL    |       |
# | actors                   | varchar(500)  | YES  |     | NULL    |       |
# +--------------------------+---------------+------+-----+---------+-------+
# 15 rows in set (0.27 sec)


# +----------------+--------------+------+-----+---------+-------+
# | Field          | Type         | Null | Key | Default | Extra |
# +----------------+--------------+------+-----+---------+-------+
# | rarbgprx_link  | varchar(50)  | NO   | PRI | NULL    |       |
# | movie_name     | varchar(100) | YES  |     | NULL    |       |
# | download_name  | varchar(100) | YES  |     | NULL    |       |
# | release_name   | varchar(200) | YES  |     | NULL    |       |
# | imdb_id        | varchar(10)  | YES  |     | NULL    |       |
# | torrent_link   | varchar(200) | YES  |     | NULL    |       |
# | magnet_link    | varchar(700) | YES  |     | NULL    |       |
# | size           | varchar(15)  | YES  |     | NULL    |       |
# | date_added     | datetime     | YES  |     | NULL    |       |
# | torrent_status | varchar(50)  | YES  |     | NULL    |       |
# | quality        | varchar(15)  | YES  |     | NULL    |       |
# | res            | varchar(15)  | YES  |     | NULL    |       |
# | sound          | varchar(15)  | YES  |     | NULL    |       |
# | codec          | varchar(15)  | YES  |     | NULL    |       |
# | seeds          | varchar(15)  | YES  |     | NULL    |       |
# | leeches        | varchar(15)  | YES  |     | NULL    |       |
# | tags           | varchar(200) | YES  |     | NULL    |       |
# +----------------+--------------+------+-----+---------+-------+
# 17 rows in set (0.27 sec)
#
