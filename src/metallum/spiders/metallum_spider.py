from scrapy import Spider
from metallum.items import MetallumItem


class MetallumSpider(Spider):
    name = 'src'
    start_urls = ['https://www.kabum.com.br/']

    def parse(self, response, **kwargs):
        pages = response.xpath('//div[@class="texto_categoria"]/p[@class="bot-categoria"]/a')

        for page in pages:
            yield response.follow(page, callback=self.parse_sub)

    def parse_sub(self, response):
        pages = response.xpath(
            '//div[@class="listagem-box"]/div[@class="listagem-titulo_descr"]/span[@class="H-titulo"]/a')

        for page in pages:
            yield response.follow(page, callback=self.parse_item)

        next_page_container = response.xpath(
            '//div[@class="listagem-paginacao"]/form/table/tr/td/a[contains(text(),"Proxima")]')
        if len(next_page_container) > 0:
            yield response.follow(next_page_container[0], callback=self.parse_sub)

    def parse_item(self, response):
        self.logger.info('Started item scraping on %s', response.url)

        item = MetallumItem()

        item['url'] = response.url

        # Fetch product name
        item['name'] = response.xpath('//div[@id="titulo_det"]/h1[@class="titulo_det"]/text()').extract_first()

        # Fetch product description
        item['description'] = response.xpath('//p[contains(@itemprop,"description")]/text()').extract_first()

        # Fetch product category
        item['category'] = response.xpath('//h2[@class="h2titcategoria"]/text()').extract_first()

        # Fetch brand name
        try:
            item['brand'] = response.xpath('//p[contains(text(),"Marca:")]/text()').extract_first().split(':')[
                1].strip()
        except AttributeError:
            self.logger.debug('Failed to extract brand on %s', response.url)
            pass

        # Fetch navigation list
        item['navigation'] = [li.split('>')[0].strip() for li in response.xpath(
            '//ol[contains(@itemtype,"http://schema.org/BreadcrumbList")]/li/a/text()').extract()]

        # Fetch product prices
        prices = {
            'old_price': response.xpath('//div[@class="preco_antigo"]/text()').re(r'\d*\.*\d+\,\d+'),
            'current_price': response.xpath('//div[@class="preco_normal"]/text()').re(r'\d*\.*\d+\,\d+'),
            'discount_price': response.xpath('//span[@class="preco_desconto"]/span/span/strong/text()').re(
                r'\d*\.*\d+\,\d+')
        }

        for key in prices.keys():
            if len(prices[key]) > 0:
                item[key] = float(prices[key][0].replace('.', '').replace(',', '.'))
            else:
                self.logger.debug('{} not found on %s'.format(key),
                                  response.url)

                # Fetch images
        images = response.xpath('//ul[@id="imagens-carrossel"]/li/img/@src')
        item['main_image'] = images.extract_first()
        item['secondary_images'] = images.extract()[1:]

        # Fetch features
        features = []
        for feat in response.xpath('//div[@class="content_tab"]/p/text()').re(r'\w+\:[\s\S]*'):
            name = feat.split(':')[0].strip()
            value = feat.split(':')[1].strip()
            features.append({'name': name, 'value': value})

        yield item
