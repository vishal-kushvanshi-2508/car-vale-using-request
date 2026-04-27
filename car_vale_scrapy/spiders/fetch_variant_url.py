import scrapy
import mysql.connector
import json
from urllib.parse import urlencode
import gzip
import os
from lxml import html

# from volkswagen_scrapy import items


class FetchVariantUrlSpider(scrapy.Spider):
    name = "fetch_variant_url"
    # allowed_domains = ["www.carwale.com"]
    # start_urls = ["https://www.carwale.com/"]

    cookies = {
        'CWC': 'w2ooYdKFmgo2XdXQLLC5qy1ci',
        '_cwutmz': 'utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29%7Cutmtrm%3D%7Cutmcnt%3D',
        'CurrentLanguage': 'en',
        '_abtest': '79',
        'languageSelected': 'en',
        '.AspNetCore.Antiforgery.9TtSrW0hzOs': 'CfDJ8JUh_GoNOjhBqUJ2d7OuCzSFiakfiDt9WyfchA_zA-n0CBND7WNAPPKAYbzn830z_LEGCuTQ-s7WHyq-5EMQP43g8mC4-WPF0ba6osrp-O4Q86DUq2erq60fmaFXPTVSJGZWHHqnmmGZNyNuz9Bvofs',
        '_gcl_au': '1.1.1073683189.1776926802',
        '_carSearchType': '1',
        'BHC': 'w2ooYdKFmgo2XdXQLLC5qy1ci',
        '_ga': 'GA1.1.52096775.1776926803',
        '_fbp': 'fb.1.1776926808507.424654618933864771',
        'cebs': '1',
        '_ce.clock_data': '46%2C45.114.65.131%2C1%2Cb87543ecbc0ba610d9f06f9f2c432a46%2CChrome%2CIN',
        'vernacularPopupClose': '1',
        '__gads': 'ID=9cb6bc50c25459ea:T=1776926875:RT=1776926875:S=ALNI_MZU_ylBXabM20P3nfK5o0SK5PxLlg',
        '__gpi': 'UID=000013e12997897f:T=1776926875:RT=1776926875:S=ALNI_MYb89m-42YAon5LCxGhrjOM7L23LQ',
        '_uetsid': '359895203ee011f18389d591c81e3005',
        '_uetvid': '3598af703ee011f1a0a04794f1261607',
        'FCCDCF': '%5Bnull%2Cnull%2Cnull%2C%5B%22CQjHcoAQjHcoAEsACBENCbFgAAAAAEPgABBoAAAWEQD-F2I2EKFEGCuQUYIYBCuACAAxYBgAAwCBgAAGCQgQAgFJIIkCAEAIEAAEAAAQAgCAABQEBAAAIAAAAAqAACAABgAQCAQAIABAAAAgIAAAAAAEQAAIgEAAAAIAIABABAAAAQAkIAAAAAAAAECAAAAAAAAAAAAAAAAAAIAAEABgAAAAAABEAAAAAAAACAQIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAIAA.ILCIB_C7EbCFCiDJ3IKMEMAhXABBAYsAwAAYBAwAADBIQIAQCkkEaBASAFCACCAAAKASBAAAoCAgAAUAAIAAVAABAAAwAIBAIIEAAgAAAQEAIAAAACIAAEQCAAAAEAEAAkAgAAAIASEAAAAAAAACBAAAAAAAAAAAAAAAABAEAASAAwAAAAAAAiAAAAAAAABAIEAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAABAAAAAAAQgAAE.YAAAAAAAAAA%22%2C%222~~dv.61.89.122.161.184.196.230.314.442.445.494.550.576.827.1029.1033.1046.1047.1051.1097.1126.1166.1301.1342.1415.1725.1765.1942.1958.1987.2068.2072.2074.2107.2213.2219.2223.2224.2328.2331.2416.2501.2567.2568.2575.2657.2686.2778.2869.2878.2908.2920.2963.3005.3023.3126.3234.3235.3253.3309.3731.6931.8931.13731.15731.33931%22%2C%2266C94EF8-B31A-43DE-BC0D-DEB8ED4C1BD9%22%5D%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%223003381c-d732-4b25-9797-2ece13f25452%5C%22%2C%5B1776926809%2C100000000%5D%5D%22%5D%5D%5D',
        'quizSlug2': '{%222781%22:%22question%22}',
        '_cwutmzsrc': 'D%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD',
        '_cwutmzmed': 'NN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN',
        '_pageviews_modelid': '-1',
        'cebsp_': '23',
        '_ce.s': 'lcw~1776938343571~v~bd0af91b7db415eda09c18c6e954e6dc9d877a01~vir~returning~lva~1776938346211~vpv~0~as~false~v11ls~b66e0b40-3ef9-11f1-a08d-bb03229ff20b~v11.cs~44156~v11.s~b66e0b40-3ef9-11f1-a08d-bb03229ff20b~v11.vs~bd0af91b7db415eda09c18c6e954e6dc9d877a01~v11.fsvd~eyJub3RNb2RpZmllZFVybCI6Imh0dHBzOi8vd3d3LmNhcndhbGUuY29tL21hcnV0aS1zdXp1a2ktY2Fycy9mcm9ueC8iLCJ1cmwiOiJjYXJ3YWxlLmNvbS9tYXJ1dGktc3V6dWtpLWNhcnMvZnJvbngiLCJyZWYiOiIiLCJ1dG0iOltdfQ%3D%3D~v11.sla~1776937765369~v11.ss~1776937765379~lcw~1776938346212',
        '__eoi': 'ID=68a27d1d8813ec1e:T=1776926875:RT=1776938346:S=AA-AfjaNmMEambtNL0Vlr2tvGnpk',
        '_cwv': 'w2ooYdKFmgo2XdXQLLC5qy1ci.CSdeWnaoln.1776937747.1776938321.1776939233.3',
        '_userModelHistory': '2781~3684~2823~1633~2633~2033',
        'bhs_cw': 'w2ooYdKFmgo2XdXQLLC5qy1ci.fwM9jYT9Yu.1776937752.1776938333.1776939253.3',
        '_ga_Z81QVQY510': 'GS2.1.s1776926803$o1$g1$t1776939257$j26$l0$h0',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
        # 'cookie': 'CWC=w2ooYdKFmgo2XdXQLLC5qy1ci; _cwutmz=utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29%7Cutmtrm%3D%7Cutmcnt%3D; CurrentLanguage=en; _abtest=79; languageSelected=en; .AspNetCore.Antiforgery.9TtSrW0hzOs=CfDJ8JUh_GoNOjhBqUJ2d7OuCzSFiakfiDt9WyfchA_zA-n0CBND7WNAPPKAYbzn830z_LEGCuTQ-s7WHyq-5EMQP43g8mC4-WPF0ba6osrp-O4Q86DUq2erq60fmaFXPTVSJGZWHHqnmmGZNyNuz9Bvofs; _gcl_au=1.1.1073683189.1776926802; _carSearchType=1; BHC=w2ooYdKFmgo2XdXQLLC5qy1ci; _ga=GA1.1.52096775.1776926803; _fbp=fb.1.1776926808507.424654618933864771; cebs=1; _ce.clock_data=46%2C45.114.65.131%2C1%2Cb87543ecbc0ba610d9f06f9f2c432a46%2CChrome%2CIN; vernacularPopupClose=1; __gads=ID=9cb6bc50c25459ea:T=1776926875:RT=1776926875:S=ALNI_MZU_ylBXabM20P3nfK5o0SK5PxLlg; __gpi=UID=000013e12997897f:T=1776926875:RT=1776926875:S=ALNI_MYb89m-42YAon5LCxGhrjOM7L23LQ; _uetsid=359895203ee011f18389d591c81e3005; _uetvid=3598af703ee011f1a0a04794f1261607; FCCDCF=%5Bnull%2Cnull%2Cnull%2C%5B%22CQjHcoAQjHcoAEsACBENCbFgAAAAAEPgABBoAAAWEQD-F2I2EKFEGCuQUYIYBCuACAAxYBgAAwCBgAAGCQgQAgFJIIkCAEAIEAAEAAAQAgCAABQEBAAAIAAAAAqAACAABgAQCAQAIABAAAAgIAAAAAAEQAAIgEAAAAIAIABABAAAAQAkIAAAAAAAAECAAAAAAAAAAAAAAAAAAIAAEABgAAAAAABEAAAAAAAACAQIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAIAA.ILCIB_C7EbCFCiDJ3IKMEMAhXABBAYsAwAAYBAwAADBIQIAQCkkEaBASAFCACCAAAKASBAAAoCAgAAUAAIAAVAABAAAwAIBAIIEAAgAAAQEAIAAAACIAAEQCAAAAEAEAAkAgAAAIASEAAAAAAAACBAAAAAAAAAAAAAAAABAEAASAAwAAAAAAAiAAAAAAAABAIEAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAABAAAAAAAQgAAE.YAAAAAAAAAA%22%2C%222~~dv.61.89.122.161.184.196.230.314.442.445.494.550.576.827.1029.1033.1046.1047.1051.1097.1126.1166.1301.1342.1415.1725.1765.1942.1958.1987.2068.2072.2074.2107.2213.2219.2223.2224.2328.2331.2416.2501.2567.2568.2575.2657.2686.2778.2869.2878.2908.2920.2963.3005.3023.3126.3234.3235.3253.3309.3731.6931.8931.13731.15731.33931%22%2C%2266C94EF8-B31A-43DE-BC0D-DEB8ED4C1BD9%22%5D%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%223003381c-d732-4b25-9797-2ece13f25452%5C%22%2C%5B1776926809%2C100000000%5D%5D%22%5D%5D%5D; quizSlug2={%222781%22:%22question%22}; _cwutmzsrc=D%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD; _cwutmzmed=NN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN; _pageviews_modelid=-1; cebsp_=23; _ce.s=lcw~1776938343571~v~bd0af91b7db415eda09c18c6e954e6dc9d877a01~vir~returning~lva~1776938346211~vpv~0~as~false~v11ls~b66e0b40-3ef9-11f1-a08d-bb03229ff20b~v11.cs~44156~v11.s~b66e0b40-3ef9-11f1-a08d-bb03229ff20b~v11.vs~bd0af91b7db415eda09c18c6e954e6dc9d877a01~v11.fsvd~eyJub3RNb2RpZmllZFVybCI6Imh0dHBzOi8vd3d3LmNhcndhbGUuY29tL21hcnV0aS1zdXp1a2ktY2Fycy9mcm9ueC8iLCJ1cmwiOiJjYXJ3YWxlLmNvbS9tYXJ1dGktc3V6dWtpLWNhcnMvZnJvbngiLCJyZWYiOiIiLCJ1dG0iOltdfQ%3D%3D~v11.sla~1776937765369~v11.ss~1776937765379~lcw~1776938346212; __eoi=ID=68a27d1d8813ec1e:T=1776926875:RT=1776938346:S=AA-AfjaNmMEambtNL0Vlr2tvGnpk; _cwv=w2ooYdKFmgo2XdXQLLC5qy1ci.CSdeWnaoln.1776937747.1776938321.1776939233.3; _userModelHistory=2781~3684~2823~1633~2633~2033; bhs_cw=w2ooYdKFmgo2XdXQLLC5qy1ci.fwM9jYT9Yu.1776937752.1776938333.1776939253.3; _ga_Z81QVQY510=GS2.1.s1776926803$o1$g1$t1776939257$j26$l0$h0',
    }


   # ---------- DB ----------
    def fetch_all_movies(self):
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="car_vale_scrapy_db"
        )

        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM cars_url WHERE status='pending'")
        rows = cursor.fetchall()

        cursor.close()
        connection.close()
        return rows

    def update_status(self, car_id, status):
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="car_vale_scrapy_db"
        )
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE cars_url SET status=%s WHERE id=%s",
            (status, car_id)
        )
        connection.commit()
        cursor.close()
        connection.close()



    def errback_handler(self, failure):
        request = failure.request
        car_id = request.meta.get("car_id")
        retry_count = request.meta.get("retry_count", 0)

        if retry_count < 3:
            self.logger.warning(f"Errback retry {retry_count+1} car_id={car_id}")

            yield scrapy.Request(
                url=request.url,
                headers=request.headers,
                callback=self.parse,
                errback=self.errback_handler,
                meta={
                    "car_id": car_id,
                    "retry_count": retry_count + 1
                },
                dont_filter=True
            )
        else:
            self.logger.error(f"Final FAIL (network) car_id={car_id}")
            self.update_status(car_id, "failed")

    # ---------- START ----------

    def start_requests(self):
        rows = self.fetch_all_movies()

        for row in rows:
            car_id =row["id"]
            car_url = row["car_url"]

            yield scrapy.Request(
                url=car_url,
                headers=self.headers,
                cookies=self.cookies,
                callback=self.parse,
                errback=self.errback_handler,
                meta={
                    "car_id": car_id,
                    "car_url" : car_url
                },
                dont_filter=True
            )
            # break


    def parse(self, response):


        car_id = response.meta["car_id"]
        car_url = response.meta["car_url"]

        folder = r"D:\vishal_kushvanshi\car_vale_scrapy\car_url_pages"
        os.makedirs(folder, exist_ok=True)

        with gzip.open(f"{folder}\\{car_id}.html.gz", "wt", encoding='utf-8') as f:
            f.write(response.text)
    
        # # Parse HTML
        tree = html.fromstring(response.text)

        rows = tree.xpath('//tr[contains(@class, "o-kY") and contains(@class, "o-bK")]')

        for row in rows:
            name = row.xpath('.//a/text()')#.get()
            variant_name = name[0] if name else "" 

            price = row.xpath('.//span[contains(text(),"Rs")]/text()')#.get()
            variant_price = price[0] if price else "" 

            website_url = row.xpath('.//a/@href')#.get()
            variant_website_url = ("https://www.carwale.com" + website_url[0]) if website_url else ""

            yield {
                "type" : "variant_details",
                "car_url" : car_url,
                "variant_name" : variant_name,
                "variant_price" : variant_price,
                "variant_website_url" : variant_website_url
            }
            
        
        self.update_status(car_id, "success")

