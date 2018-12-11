# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from sql_exec import insert_torrent, insert_movie_info, update_other_downloads, insert_images
from creds import rds_host, master_username, master_password
import pymysql

dict_headers = {
    'Host': 'rarbgprx.org',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://rarbgprx.org/index56.php',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Cookie': 'gaDts48g=q8h5pp9t; aby=1; tcc; skt=6FNXBaguf9; skt=6FNXBaguf9; gaDts48g=q8h5pp9t; expla=3',
    'Upgrade-Insecure-Requests': '1',
}

dict_headers2 = {
    'Host': 'rarbgprx.org',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://rarbgprx.org/torrents.php?category=movies',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Cookie': 'gaDts48g=q8h5pp9t; aby=1; tcc; skt=6FNXBaguf9; skt=6FNXBaguf9; gaDts48g=q8h5pp9t; expla=3',
    'Upgrade-Insecure-Requests': '1',
}

cookies = {
    'aby': '1',
    'gaDts48g': 'q8h5pp9t',
    'skt': '6FNXBaguf9',
}

class ImdbMoviesSpider(scrapy.Spider):
    name = 'imdb_movies'
    # allowed_domains = ['rarbgprx.org/torrents.php?category=movies']
    start_urls = ['http://rarbgprx.org/torrents.php?category=movies/']
    db_name = 'movies'
    conn = pymysql.connect(rds_host, user=master_username, passwd=master_password, db=db_name, connect_timeout=5)

    # def start_requests(self, url = ""):
    #     # request = super(ImdbMoviesSpider, self).make_requests_from_url(url)
    #     # request.cookies = cookies
    #     if url=="" : url = self.farzi_start_urls[0]
    #     req = Request(url, cookies = cookies, headers = dict_headers, dont_filter=True, meta={'dont_redirect': True})
    #     return [req]

    def parse(self, response):
        with open("log.txt", "w") as file:
        	file.write("executed")
        for i in range(1, 100):
            yield scrapy.Request(
                "http://rarbgprx.org/torrents.php?category=movies&page={}".format(i),
                 cookies = cookies,
                 headers = dict_headers,
                 meta = {'dont_redirect' : True},
                 callback = self.parse1
            )

        # yield scrapy.Request(
        #     "https://rarbgprx.org/torrent/r17xlkc",
        #     callback = self.parse_movie_page,
        #     headers = dict_headers,
        #     cookies = cookies,
        # )#, cookies = cookies, headers=dict_headers)



    def parse1(self, response):
        print("hi   ")
        # scrapy.Request("http://rarbgprx.org/torrents.php?category=movies", cookies = cookies, headers = dict_headers, meta = {'dont_redirect' : True})
        movies_table = response.xpath("//*[table/@class = 'lista2t']")
        movies_list = movies_table.xpath(".//*[@class = 'lista2']")
        movies_list = response.xpath("//*[@class = 'lista2']")
        for movie in movies_list:
            name = movie.xpath("./td[2]/a[1]/text()").extract_first()
            imdb_url = movie.xpath("./td[2]/a[1]/@href").extract_first()
            movie_url = response.urljoin(imdb_url)
            yield scrapy.Request(movie_url, callback = self.parse_movie_page, cookies = cookies, headers=dict_headers2)
            print (name, movie_url)
        # yield scrapy.Request("https://rarbgprx.org/torrent/r17xlkc", callback = self.parse_movie_page)#, cookies = cookies, headers=dict_headers)

    def parse_movie_page(self, response):
        movie_name = response.xpath("//*[h1/@class = 'black']/h1/text()").extract_first()
        download_name = response.xpath("//*[tr]//*[td[1]]//*[text()=' Torrent:']//following::td[1]/a[1]/text()").extract_first()
        torrent_link_temp = response.xpath("//*[tr]//*[td[1]]//*[text()=' Torrent:']//following::td[1]/a[1]/@href").extract_first()
        torrent_link = response.urljoin(torrent_link_temp)

        magnet_link = response.xpath("//*[tr]//*[td[1]]//*[text()=' Torrent:']//following::td[1]/a[2]/@href").extract_first()

        poster_img = response.xpath("//*[tr]//*[td[1]]//*[text()=' Torrent:']//following::td[1]/img/@src").extract_first()
        try :insert_images([poster_img], self.conn)
        except : pass

        download_attributes = response.xpath("//*[tr]//*[td[1]]//*[text()=' Others:']//following::table/tbody/tr")
        if download_attributes:
            download_attributes = download_attributes[0].xpath("./td/text()").extract()
            download_attributes = ' || '.join(download_attributes)
        raw_parameters = response.xpath("//*[tr]//*[td[1]]//*[text()=' Others:']//following::table/tbody/tr[1]/td/text()").extract()

        no_of_options = len(response.xpath("//*[tr]//*[td[1]]//*[text()=' Others:']//following::table/tbody/tr")) - 1

        download_options = response.xpath("//*[tr]//*[td[1]]//*[text()=' Others:']//following::table/tbody/tr")[1:]
        all_downloads = []
        for i in download_options:
            additional_downloads = {}
            additional_downloads['quality'] = i.xpath("./td[1]/text()").extract_first()
            additional_downloads['res'] = i.xpath("./td[2]/text()").extract_first()
            additional_downloads['sound'] = i.xpath("./td[3]/text()").extract_first()
            additional_downloads['codec'] = i.xpath("./td[4]/text()").extract_first()
            additional_downloads['name'] = i.xpath("./td[5]/a/text()").extract_first()
            additional_downloads['seeds'] = i.xpath("./td[6]/font/text()").extract_first()
            additional_downloads['leeches'] = i.xpath("./td[7]/text()").extract_first()
            additional_downloads['size'] = i.xpath("./td[8]/text()").extract_first()

            # update_operation()
            all_downloads.append(additional_downloads)

        description_imgs = response.xpath("//*[tr]//*[td[1]]//*[text()=' Description:']//following::td[1]/a/@href").extract()
        try : insert_images(description_imgs, self.conn)
        except : pass

        description_imgs = ' || '.join(description_imgs)

        trailer_link_temp = response.xpath("//*[tr]//*[td[1]]//*[text()='Trailer:']//following::td[1]/a/@href").extract_first()
        trailer_link = response.urljoin(trailer_link_temp)

        imdb_link = response.xpath("//*[contains(@href, 'imdb.com')]/@href").extract_first()
        if imdb_link: imdb_id = imdb_link.replace("https://www.imdb.com/title/", "").replace("/", '')
        else: imdb_id = ''

        rarbgprx_rating = response.xpath("//*[@class = 'unit-rating']//following::strong[1]/text()").extract_first()
        if rarbgprx_rating: rarbgprx_rating = rarbgprx_rating.strip()

        category = response.xpath("//*[tr]//*[td[1]]//*[text()=' Category:']//following::td[1]/a/text()").extract_first()
        category_link_temp = response.xpath("//*[tr]//*[td[1]]//*[text()=' Category:']//following::td[1]/a/@href").extract_first()
        category_link = response.urljoin(category_link_temp)

        torrent_size = response.xpath("//*[tr]//*[td[1]]//*[text()=' Size:']//following::td[1]/text()").extract_first()

        file_details = []
        file_names = []
        file_sizes = []

        files = response.xpath("//*[@id = 'showhidefiles']//following::table[1]/tr")
        for file in files[1:]:
            file_name = file.xpath("./td[1]/text()").extract_first()
            file_size = file.xpath("./td[2]/text()").extract_first()

            if file_name: file_name = file_name.strip()
            if file_size : file_size = file_size.strip()

            file_names.append(file_name)
            file_sizes.append(file_size)

            file_dict = {
                'file_name' : file_name,
                'file_size' : file_size,
            }
            file_details.append(file_dict)

        file_names = ' || '.join(file_names)
        file_sizes = ' || '.join(file_sizes)

        date_added = response.xpath("//*[tr]//*[td[1]]//*[text()=' Added:']//following::td[1]/text()").extract_first()
        if date_added: date_added = date_added.strip()

        movie_name = response.xpath("//*[tr]//*[td[1]]//*[text()='Title:']//following::td[1]/text()").extract_first()
        if movie_name: movie_name = movie_name.strip()

        pg_rating = response.xpath("//*[tr]//*[td[1]]//*[text()=' PG Rating:']//following::td[1]/text()").extract_first()
        if pg_rating: pg_rating = pg_rating.strip()

        imdb_rating = response.xpath("//*[tr]//*[td[1]]//*[text()='IMDB Rating:']//following::td[1]/text()").extract_first()
        if imdb_rating: imdb_rating = imdb_rating.strip()

        metacritic_rating = response.xpath("//*[tr]//*[td[1]]//*[text()='Metacritic:']//following::td[1]/span/text()").extract_first()
        if metacritic_rating: metacritic_rating = metacritic_rating.strip()


        rotten_rating = response.xpath("//*[tr]//*[td[1]]//*[text()='Rotten Rating:']//following::td[1]/text()").extract_first()
        if rotten_rating: rotten_rating = rotten_rating.strip()

        rotten_tomatoes = response.xpath("//*[tr]//*[td[1]]//*[text()='RottenTomatoes:']//following::td[1]/text() ").extract()
        rotten_tomatoes_popcorn, rotten_tomatoes_tomatoes = '',''
        if rotten_tomatoes:
            rotten_tomatoes_popcorn = rotten_tomatoes[1].strip()
            rotten_tomatoes_tomatoes = rotten_tomatoes[0].strip()

        torrent_status = response.xpath("//*[tr]//*[td[1]]//*[text()='Peers:']//following::td[1]/text()").extract_first()
        if torrent_status: torrent_status = torrent_status.strip()

        hit_and_run = response.xpath("//*[tr]//*[td[1]]//*[text()='Hit&Run:']//following::td[1]/text()").extract_first()
        if hit_and_run: hit_and_run = hit_and_run.strip()

        release_name = response.xpath("//*[tr]//*[td[1]]//*[text()='Release name:']//following::td[1]/text()").extract_first()
        if release_name: release_name = release_name.strip()

        tags = response.xpath("//*[tr]//*[td[1]]//*[text()='Tags:']//following::td[1]/a/text()").extract()
        tags = ' || '.join(tags)

        genres = response.xpath("//*[tr]//*[td[1]]//*[text()='Genres:']//following::td[1]/a/text()").extract()
        genres = ' || '.join(genres)

        actors = response.xpath("//*[tr]//*[td[1]]//*[text()='Actors:']//following::td[1]/a/text()").extract()
        actors = ' || '.join(actors)

        directors = response.xpath("//*[tr]//*[td[1]]//*[text()='Director:']//following::td[1]/a/text()").extract()
        directors = ' || '.join(directors)

        year = response.xpath("//*[tr]//*[td[1]]//*[text()='Director:']//following::td[1]/text()").extract_first()
        if year: year = year.strip()

        plot = response.xpath("//*[tr]//*[td[1]]//*[text()='Plot:']//following::td[1]/a/text()").extract_first()
        if plot: plot = plot.strip()

        yield_data = {
            'rarbgprx_url' : response.url,
            'movie_name' : movie_name,
            'download_name' : download_name,
            # 'torrent_link_temp' : torrent_link_temp,
            'torrent_link' : torrent_link,
            'magnet_link' : magnet_link,
            'poster_img' : poster_img,
            'download_attributes' : download_attributes,
            'no_of_options' : no_of_options,
            # 'download_options' : download_options,
            'trailer_link' : trailer_link,
            'imdb_link' : imdb_link,
            'imdb_id' : imdb_id,
            'category' : category,
            'category_link' : category_link,
            'torrent_size' : torrent_size,
            # 'file_details' : file_details,
            'file_names' : file_names,
            'file_sizes' : file_sizes,
            # 'all_downloads' : all_downloads,
            'description_imgs' : description_imgs,
            'trailer_link' : trailer_link,
            # 'imdb_link' : imdb_link,
            'imdb_id' : imdb_id,
            'rarbgprx_rating' : rarbgprx_rating,
            'category' : category,
            # 'category_link_temp' : category_link_temp,
            'category_link' : category_link,
            'torrent_size' : torrent_size,
            'date_added' : date_added,
            'movie_name' : movie_name,
            'pg_rating' : pg_rating,
            'imdb_rating' : imdb_rating,
            'metacritic_rating' : metacritic_rating,
            'rotten_rating' : rotten_rating,
            # 'rotten_tomatoes' : rotten_tomatoes,
            'rotten_tomatoes_popcorn' : rotten_tomatoes_popcorn,
            'rotten_tomatoes_tomatoes' : rotten_tomatoes_tomatoes,
            'torrent_status' : torrent_status,
            'release_name' : release_name,
            'tags' : tags,
            'genres' : genres,
            'actors' : actors,
            'directors' : directors,
            'year' : year,
            'plot' : plot,
            'seeds' : "",
            'leeches' : "",
            'quality' : "",
            'res' : "",
            'sound' : "",
            'codec' : "",
        }

        insert_torrent(yield_data, self.conn)
        insert_movie_info(yield_data, self.conn )
        yield yield_data
