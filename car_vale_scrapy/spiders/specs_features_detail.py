import scrapy

import scrapy
import gzip
import os
import json
from lxml import html
import mysql.connector
# from store_data_database import *

class SpecsFeaturesDetailSpider(scrapy.Spider):
    name = "specs_features_detail"
    # allowed_domains = ["www.carwale.com"]
    # start_urls = ["https://www.carwale.com/"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 5,
        "ROBOTSTXT_OBEY": False
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
        cursor.execute("SELECT * FROM variant_details WHERE status='pending'")
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
            "UPDATE variant_details SET status=%s WHERE id=%s",
            (status, car_id)
        )
        connection.commit()
        cursor.close()
        connection.close()

    cookies = {
        'CWC': 'w2ooYdKFmgo2XdXQLLC5qy1ci',
        '_cwutmz': 'utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29%7Cutmtrm%3D%7Cutmcnt%3D',
        'CurrentLanguage': 'en',
        '_abtest': '79',
        'languageSelected': 'en',
        '_gcl_au': '1.1.1073683189.1776926802',
        '_carSearchType': '1',
        'BHC': 'w2ooYdKFmgo2XdXQLLC5qy1ci',
        '_ga': 'GA1.1.52096775.1776926803',
        '_fbp': 'fb.1.1776926808507.424654618933864771',
        '_ce.clock_data': '46%2C45.114.65.131%2C1%2Cb87543ecbc0ba610d9f06f9f2c432a46%2CChrome%2CIN',
        '__gads': 'ID=9cb6bc50c25459ea:T=1776926875:RT=1776926875:S=ALNI_MZU_ylBXabM20P3nfK5o0SK5PxLlg',
        '__gpi': 'UID=000013e12997897f:T=1776926875:RT=1776926875:S=ALNI_MYb89m-42YAon5LCxGhrjOM7L23LQ',
        '_uetsid': '359895203ee011f18389d591c81e3005',
        '_uetvid': '3598af703ee011f1a0a04794f1261607',
        'FCCDCF': '%5Bnull%2Cnull%2Cnull%2C%5B%22CQjHcoAQjHcoAEsACBENCbFgAAAAAEPgABBoAAAWEQD-F2I2EKFEGCuQUYIYBCuACAAxYBgAAwCBgAAGCQgQAgFJIIkCAEAIEAAEAAAQAgCAABQEBAAAIAAAAAqAACAABgAQCAQAIABAAAAgIAAAAAAEQAAIgEAAAAIAIABABAAAAQAkIAAAAAAAAECAAAAAAAAAAAAAAAAAAIAAEABgAAAAAABEAAAAAAAACAQIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAIAA.ILCIB_C7EbCFCiDJ3IKMEMAhXABBAYsAwAAYBAwAADBIQIAQCkkEaBASAFCACCAAAKASBAAAoCAgAAUAAIAAVAABAAAwAIBAIIEAAgAAAQEAIAAAACIAAEQCAAAAEAEAAkAgAAAIASEAAAAAAAACBAAAAAAAAAAAAAAAABAEAASAAwAAAAAAAiAAAAAAAABAIEAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAABAAAAAAAQgAAE.YAAAAAAAAAA%22%2C%222~~dv.61.89.122.161.184.196.230.314.442.445.494.550.576.827.1029.1033.1046.1047.1051.1097.1126.1166.1301.1342.1415.1725.1765.1942.1958.1987.2068.2072.2074.2107.2213.2219.2223.2224.2328.2331.2416.2501.2567.2568.2575.2657.2686.2778.2869.2878.2908.2920.2963.3005.3023.3126.3234.3235.3253.3309.3731.6931.8931.13731.15731.33931%22%2C%2266C94EF8-B31A-43DE-BC0D-DEB8ED4C1BD9%22%5D%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%223003381c-d732-4b25-9797-2ece13f25452%5C%22%2C%5B1776926809%2C100000000%5D%5D%22%5D%5D%5D',
        '_cwutmzsrc': 'D%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD',
        '_cwutmzmed': 'NN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN',
        '_userModelHistory': '2781~3684~2823~1633~2633~2789~2033',
        '.AspNetCore.Antiforgery.9TtSrW0hzOs': 'CfDJ8JUh_GoNOjhBqUJ2d7OuCzTee5QCrpimZejpEm1YmEBSvkHhUO0SMzXjMSKx7GJ241L4Du1Q_PkPyPzt1iSGFRGOVeUEynm3upvGt2MLxBl9lXcj1z_RuH_BMNn3gMqQunlneZayyjcyWDUrdJ-dFdQ',
        '_cwv': 'w2ooYdKFmgo2XdXQLLC5qy1ci.w2ooYdKFmgo2XdXQLLC5qy1ci.1776945187.1776946426.1776946432.4',
        '_pageviews_modelid': '2033',
        'cebs': '1',
        'cebsp_': '2',
        '_ce.s': 'lcw~1776946441485~v~bd0af91b7db415eda09c18c6e954e6dc9d877a01~vir~returning~lva~1776946442918~vpv~1~as~false~v11ls~03cca750-3f0b-11f1-b92b-1fef903190e9~v11slnt~1776941026303~v11.cs~44156~v11.s~03cca750-3f0b-11f1-b92b-1fef903190e9~v11.vs~bd0af91b7db415eda09c18c6e954e6dc9d877a01~v11.fsvd~eyJub3RNb2RpZmllZFVybCI6Imh0dHBzOi8vd3d3LmNhcndhbGUuY29tL21hcnV0aS1zdXp1a2ktY2Fycy9mcm9ueC9zaWdtYS8iLCJ1cmwiOiJjYXJ3YWxlLmNvbS9tYXJ1dGktc3V6dWtpLWNhcnMvZnJvbngvc2lnbWEiLCJyZWYiOiIiLCJ1dG0iOltdfQ%3D%3D~v11.sla~1776945196617~v11.ss~1776945196624~lcw~1776946442919',
        'bhs_cw': 'w2ooYdKFmgo2XdXQLLC5qy1ci.y7b1lvVedp.1776945189.1776946435.1776947054.4',
        '__eoi': 'ID=68a27d1d8813ec1e:T=1776926875:RT=1776947058:S=AA-AfjaNmMEambtNL0Vlr2tvGnpk',
        '_ga_Z81QVQY510': 'GS2.1.s1776945189$o2$g1$t1776947059$j53$l0$h0',
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
        # 'cookie': 'CWC=w2ooYdKFmgo2XdXQLLC5qy1ci; _cwutmz=utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29%7Cutmtrm%3D%7Cutmcnt%3D; CurrentLanguage=en; _abtest=79; languageSelected=en; _gcl_au=1.1.1073683189.1776926802; _carSearchType=1; BHC=w2ooYdKFmgo2XdXQLLC5qy1ci; _ga=GA1.1.52096775.1776926803; _fbp=fb.1.1776926808507.424654618933864771; _ce.clock_data=46%2C45.114.65.131%2C1%2Cb87543ecbc0ba610d9f06f9f2c432a46%2CChrome%2CIN; __gads=ID=9cb6bc50c25459ea:T=1776926875:RT=1776926875:S=ALNI_MZU_ylBXabM20P3nfK5o0SK5PxLlg; __gpi=UID=000013e12997897f:T=1776926875:RT=1776926875:S=ALNI_MYb89m-42YAon5LCxGhrjOM7L23LQ; _uetsid=359895203ee011f18389d591c81e3005; _uetvid=3598af703ee011f1a0a04794f1261607; FCCDCF=%5Bnull%2Cnull%2Cnull%2C%5B%22CQjHcoAQjHcoAEsACBENCbFgAAAAAEPgABBoAAAWEQD-F2I2EKFEGCuQUYIYBCuACAAxYBgAAwCBgAAGCQgQAgFJIIkCAEAIEAAEAAAQAgCAABQEBAAAIAAAAAqAACAABgAQCAQAIABAAAAgIAAAAAAEQAAIgEAAAAIAIABABAAAAQAkIAAAAAAAAECAAAAAAAAAAAAAAAAAAIAAEABgAAAAAABEAAAAAAAACAQIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAIAA.ILCIB_C7EbCFCiDJ3IKMEMAhXABBAYsAwAAYBAwAADBIQIAQCkkEaBASAFCACCAAAKASBAAAoCAgAAUAAIAAVAABAAAwAIBAIIEAAgAAAQEAIAAAACIAAEQCAAAAEAEAAkAgAAAIASEAAAAAAAACBAAAAAAAAAAAAAAAABAEAASAAwAAAAAAAiAAAAAAAABAIEAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAABAAAAAAAQgAAE.YAAAAAAAAAA%22%2C%222~~dv.61.89.122.161.184.196.230.314.442.445.494.550.576.827.1029.1033.1046.1047.1051.1097.1126.1166.1301.1342.1415.1725.1765.1942.1958.1987.2068.2072.2074.2107.2213.2219.2223.2224.2328.2331.2416.2501.2567.2568.2575.2657.2686.2778.2869.2878.2908.2920.2963.3005.3023.3126.3234.3235.3253.3309.3731.6931.8931.13731.15731.33931%22%2C%2266C94EF8-B31A-43DE-BC0D-DEB8ED4C1BD9%22%5D%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%223003381c-d732-4b25-9797-2ece13f25452%5C%22%2C%5B1776926809%2C100000000%5D%5D%22%5D%5D%5D; _cwutmzsrc=D%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD; _cwutmzmed=NN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN; _userModelHistory=2781~3684~2823~1633~2633~2789~2033; .AspNetCore.Antiforgery.9TtSrW0hzOs=CfDJ8JUh_GoNOjhBqUJ2d7OuCzTee5QCrpimZejpEm1YmEBSvkHhUO0SMzXjMSKx7GJ241L4Du1Q_PkPyPzt1iSGFRGOVeUEynm3upvGt2MLxBl9lXcj1z_RuH_BMNn3gMqQunlneZayyjcyWDUrdJ-dFdQ; _cwv=w2ooYdKFmgo2XdXQLLC5qy1ci.w2ooYdKFmgo2XdXQLLC5qy1ci.1776945187.1776946426.1776946432.4; _pageviews_modelid=2033; cebs=1; cebsp_=2; _ce.s=lcw~1776946441485~v~bd0af91b7db415eda09c18c6e954e6dc9d877a01~vir~returning~lva~1776946442918~vpv~1~as~false~v11ls~03cca750-3f0b-11f1-b92b-1fef903190e9~v11slnt~1776941026303~v11.cs~44156~v11.s~03cca750-3f0b-11f1-b92b-1fef903190e9~v11.vs~bd0af91b7db415eda09c18c6e954e6dc9d877a01~v11.fsvd~eyJub3RNb2RpZmllZFVybCI6Imh0dHBzOi8vd3d3LmNhcndhbGUuY29tL21hcnV0aS1zdXp1a2ktY2Fycy9mcm9ueC9zaWdtYS8iLCJ1cmwiOiJjYXJ3YWxlLmNvbS9tYXJ1dGktc3V6dWtpLWNhcnMvZnJvbngvc2lnbWEiLCJyZWYiOiIiLCJ1dG0iOltdfQ%3D%3D~v11.sla~1776945196617~v11.ss~1776945196624~lcw~1776946442919; bhs_cw=w2ooYdKFmgo2XdXQLLC5qy1ci.y7b1lvVedp.1776945189.1776946435.1776947054.4; __eoi=ID=68a27d1d8813ec1e:T=1776926875:RT=1776947058:S=AA-AfjaNmMEambtNL0Vlr2tvGnpk; _ga_Z81QVQY510=GS2.1.s1776945189$o2$g1$t1776947059$j53$l0$h0',
    }




    def start_requests(self):
        print("--------start_requests---------")

        self.folder = r"D:\vishal_kushvanshi\car_vale_scrapy\car_variant_pages"
        os.makedirs(self.folder, exist_ok=True)

        list_data = self.fetch_all_movies()  #  your DB function

        for data in list_data:
            variant_id = data.get("id")
            variant_url = data.get("variant_website_url")

            yield scrapy.Request(
                url=variant_url,
                cookies= self.cookies,
                headers= self.headers,
                callback=self.parse,
                meta={"data": data},
                dont_filter=True
            )


    def parse(self, response):
        data = response.meta["data"]

        variant_id = data.get("id")
        # print("data now : ", variant_id)
        # print("data now : ", type(variant_id))
        variant_name = data.get("variant_name")
        variant_url = data.get("variant_website_url")

        print("Processing:", variant_url)

        #  Save gzip
        with gzip.open(f"{self.folder}\\{variant_id}.html.gz", "wt", encoding='utf-8') as f:
            f.write(response.text)

        #  If redirected
        if response.url != variant_url:
            print(" Redirected:", response.url)

            yield {
                "type": "car_features_detail",
                "variant_id": variant_id,
                "variant_name": variant_name,
                "variant_website_url": variant_url,
                "Highlights": json.dumps([]),
                "Specification": json.dumps({}),
                "Safety": json.dumps({}),
                "Features": json.dumps({})
            }

            # insert_features_detail_table([features_detail], variant_id)
            self.update_status(variant_id, "success")
            return

        tree = html.fromstring(response.text)

        # ---------------- Highlights ----------------
        Highlights = tree.xpath("//h3[contains(text(),'Highlights')]/text()")
        Highlights = Highlights[0] if Highlights else "Highlights"

        Highlights_value = tree.xpath(
            "//p[contains(text(),'Available in Next Variant')]/text() | "
            "//div[contains(@class,'o-ne')]//span[@class='o-b']/text()"
        )

        Highlights_list = [i.strip() for i in Highlights_value if i.strip()]

        # ---------------- Specification ----------------
        specification_title = tree.xpath("//h3[contains(text(),'Specification')]/text()")
        specification_title = specification_title[0].strip() if specification_title else "Specification"

        specification_data = {}

        subcategories = tree.xpath("//div[@data-index='1']//div[@data-subcategoryid]")

        for sub in subcategories:
            sub_title = sub.xpath(".//p[contains(@class,'o-j3')]/text()")
            if not sub_title:
                continue

            sub_title = sub_title[0].strip()
            specification_data[sub_title] = {}

            items = sub.xpath(".//div[@data-itemid]")

            for item in items:
                key = item.xpath(".//p/text()")
                value = item.xpath(".//span[@class='o-b']/text()")

                if key and value:
                    specification_data[sub_title][key[0].strip()] = value[0].strip()

        # fallback
        if not specification_data:
            sections = tree.xpath("//div[@data-index='0']//li")

            for sec in sections:
                title = sec.xpath(".//p/text()")
                if not title:
                    continue

                title = title[0].strip()
                specification_data[title] = {}

                items = sec.xpath(".//div[@data-itemid]")

                for item in items:
                    key = item.xpath("./div[1]/text()")
                    value = item.xpath("./div[2]/text()")

                    if key and value:
                        specification_data[title][key[0].strip()] = value[0].strip()

        # ---------------- Safety ----------------
        safety_title = tree.xpath("//h3[contains(text(),'Safety')]/text()")
        safety_title = safety_title[0].strip() if safety_title else "Safety"

        safety_data = {}

        subcategories = tree.xpath("//div[@data-index='2']//div[@data-subcategoryid]")

        for sub in subcategories:
            title = sub.xpath(".//p/text()")
            if not title:
                continue

            title = title[0].strip()
            features = sub.xpath(".//div[@data-itemid]//span[last()]/text()")

            safety_data[title] = [f.strip() for f in features if f.strip()]

        # ---------------- Features ----------------
        features_title = tree.xpath("//h3[contains(text(),'Features')]/text()")
        features_title = features_title[0].strip() if features_title else "Features"

        features_data = {}

        subcategories = tree.xpath("//div[@data-index='3']//div[@data-subcategoryid]")

        for sub in subcategories:
            title = sub.xpath(".//p/text()")
            if not title:
                continue

            title = title[0].strip()
            features = sub.xpath(".//span[last()]/text()")

            features_data[title] = [f.strip() for f in features if f.strip()]

        # ---------------- Final Clean ----------------
        yield {
            "type": "car_features_detail",
            "variant_id": variant_id,
            "variant_name": variant_name,
            "variant_website_url": variant_url,
            "Highlights": json.dumps(Highlights_list),
            "Specification": json.dumps(specification_data),
            "Safety": json.dumps(safety_data),
            "Features": json.dumps(features_data)
        }

        self.update_status(variant_id, "success")
