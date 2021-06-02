import json
import re
import sqlite3

import scrapy


def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


class DB_Handler:
    def __init__(self):
        self.con = sqlite3.connect('../../data/metallum.db')
        self.cur = self.con.cursor()
        self.cur.execute("DROP TABLE IF EXISTS band")
        self.cur.execute("DROP TABLE IF EXISTS album")
        self.cur.execute("CREATE TABLE band(band_id integer,band_name text,country_of_origin text,location text,status text,formed_in text,genre text,lyrical_themes text,last_label text,years_active text)")
        self.cur.execute("CREATE TABLE album(album_id integer,album_name text,band_id integer,type text,release_date text,catalog_id text,label text,format text,reviews text)")
        self.con.commit()
        self.con.close()

    def add_band(self, values):
        self.con = sqlite3.connect('../../data/metallum.db')
        self.cur = self.con.cursor()
        self.cur.execute(f"INSERT INTO band VALUES ({values})")
        self.con.commit()
        self.con.close()

    def add_album(self, values):
        self.con = sqlite3.connect('../../data/metallum.db')
        self.cur = self.con.cursor()
        self.cur.execute(f"INSERT INTO album VALUES ({values})")
        self.con.commit()
        self.con.close()


class FetchDatasetSpider(scrapy.Spider):
    name = 'fetch_dataset'
    allowed_domains = ['metal-archives.com']
    start_urls = ['https://www.metal-archives.com/browse/letter']

    def parse_album(self, response):
        album_name = response.xpath('//body/div/div[3]/div[2]/div[2]/h1/a')[0].root.text
        album_info = response.xpath('//body/div/div[3]/div[2]/div[2]')

        type_ = album_info.xpath('//dl[1]/dd[1]')[0].root.text
        release_date = album_info.xpath('//dl[1]/dd[2]')[0].root.text
        catalog_id = album_info.xpath('//dl[1]/dd[3]')[0].root.text
        label = album_info.xpath('//dl[2]/dd[1]/a')[0].root.text
        format_ = album_info.xpath('//dl[2]/dd[2]')[0].root.text
        reviews = cleanhtml(album_info.xpath('//dl[2]/dd[3]')[0].extract()).strip('\n').replace('\n', '')

        band_id = response.xpath('//body/div/div[3]/div[2]/div[2]/h2/a')[0].attrib['href'].rsplit('/', 1)[-1]
        album_id = response.url.rsplit('/', 1)[-1]
        db: DB_Handler = response.meta['db']
        db.add_album(
            f"'{album_id}','{album_name}','{band_id}','{type_}','{release_date}','{catalog_id}','{label}','{format_}','{reviews}'")
        yield

    def parse_band_discography(self, response):
        for el in response.xpath('//table/tbody/tr/td[1]'):
            album_link = el.xpath('//a').attrib.get('href')
            yield response.follow(album_link, callback=self.parse_album, meta={'db': response.meta['db']})

    def parse_band(self, response):
        band_name = response.xpath('//body/div/div[3]/div[2]/div[2]/h1/a')[0].root.text
        band_stats = response.xpath('//body/div/div[3]/div[2]/div[2]/div[2]')[0]

        country_of_origin = band_stats.xpath('//dl[1]/dd[1]/a')[0].root.text
        location = band_stats.xpath('//dl[1]/dd[2]')[0].root.text
        status = band_stats.xpath('//dl[1]/dd[3]')[0].root.text
        formed_in = band_stats.xpath('//dl[1]/dd[4]')[0].root.text
        genre = band_stats.xpath('//dl[2]/dd[1]')[0].root.text
        lyrical_themes = band_stats.xpath('//dl[2]/dd[2]')[0].root.text
        last_label = band_stats.xpath('//dl[2]/dd[3]')[0].root.text
        years_active = band_stats.xpath('//dl[3]/dd')[0].root.text.strip('\n').replace('\n', '')

        band_id = response.url.rsplit('/', 1)[-1]
        discography_link = f'https://www.metal-archives.com/band/discography/id/{band_id}/tab/all'
        db: DB_Handler = response.meta['db']
        db.add_band(
            f"'{band_id}','{band_name}','{country_of_origin}','{location}','{status}','{formed_in}','{genre}','{lyrical_themes}','{last_label}','{years_active}'")
        yield response.follow(discography_link, callback=self.parse_band_discography, meta={'db': response.meta['db']})

    def parse_letter_page(self, response):
        result = json.loads(response.text)
        for el in result.get('aaData'):
            band_link = scrapy.Selector(text=el[0]).xpath('//a').attrib.get('href')
            yield response.follow(band_link, callback=self.parse_band, meta={'db': response.meta['db']})

    def parse(self, response):
        db = DB_Handler()
        letter_pages = response.xpath('//body/div/div[3]/div[1]/ul/li/a')
        for letter_page in letter_pages:
            letter_link = letter_page.attrib.get('href')
            letter = letter_link.split('/')[4]
            display_start = 0
            content_link = f"https://www.metal-archives.com/browse/ajax-letter/l/{letter}/json/1?sEcho=1&iColumns=4&sColumns=&iDisplayStart={display_start}&iDisplayLength=500&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&bSortable_0=true&bSortable_1=true&bSortable_2=true&bSortable_3=false&_=1622244976120"
            yield response.follow(content_link, callback=self.parse_letter_page, meta={'db': db})
