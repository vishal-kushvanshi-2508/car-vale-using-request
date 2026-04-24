
import requests
import os
import gzip
from lxml import html
import json
from store_data_database import *



def specs_features_detail(list_data:list):
    # print("----------specs_features_detail--------")

    folder = r"D:\vishal_kushvanshi\car_vale_request\car_variant_pages"
    os.makedirs(folder, exist_ok=True)

    for dict_data in list_data:
        specs_features_data_list = []

        variant_id = dict_data.get("id")
        variant_name = dict_data.get("variant_name")
        variant_price = dict_data.get("variant_price")
        variant_website_url = dict_data.get("variant_website_url")
        print("variant_website_url : ", variant_website_url, "variant_id : ", variant_id)


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

        response = requests.get(variant_website_url, cookies=cookies, headers=headers)

        # with open("car_variant_url_1.html", "w", encoding='utf-8') as f:
        #     f.write(response.text)

        with gzip.open(f"{folder}\\{variant_id}.html.gz", "wt", encoding='utf-8') as f:
            f.write(response.text)

        if response.url == variant_website_url:
            # # Parse HTML
            tree = html.fromstring(response.text)

            # --------Highlights-----------
            Highlights = tree.xpath("//h3[contains(text(),'Highlights')]/text()")
            Highlights = Highlights[0] if Highlights else "Highlights"
            Highlights_value = tree.xpath(
                "//p[contains(text(),'Available in Next Variant')]/text() | //div[@class='o-ne o-bQ o-fb o-eD o-mf o-lT o-mO o-kY o-C o-o o-j2']//div[@class='o-av o-aC o-c4 o-cA  o-bM o-ci o-f']//span[@class='o-b']/text()"
            )

            # Clean data
            Highlights_list = [i.strip() for i in Highlights_value if i.strip()]

            # --------specification-----------
            # 1. Main Title
            main_title = tree.xpath("//h3[contains(text(),'Specification')]/text()")
            specification_title = main_title[0].strip() if main_title else "Specification"

            specification_data = {}
            # 2. Each Subcategory block
            subcategories = tree.xpath("//div[@data-index='1']//div[@data-subcategoryid]")

            for sub in subcategories:
                # Subcategory title
                sub_title = sub.xpath(".//p[contains(@class,'o-j3')]/text()")
                if not sub_title:
                    continue
                sub_title = sub_title[0].strip()
                specification_data[sub_title] = {}

                # 3. Each spec item
                items = sub.xpath(".//div[@data-itemid]")

                for item in items:
                    key = item.xpath(".//p/text()")
                    value = item.xpath(".//span[@class='o-b']/text()")

                    if key and value:
                        key = key[0].strip()
                        value = value[0].strip()

                        specification_data[sub_title][key] = value

            # -----------not specification_data------------
            if not specification_data:
                specification_data = {}

                # loop each main section
                sections = tree.xpath("//div[@data-index='0']//li[@class='o-kY o-mf o-lS o-lX o-mO']")

                for sec in sections:
                    # section title
                    title = sec.xpath(".//p[@class='o-jq o-j4']/text()")
                    if not title:
                        continue
                    title = title[0].strip()

                    specification_data[title] = {}

                    # each spec item
                    items = sec.xpath(".//div[@data-itemid]//div[@data-testid]")

                    for item in items:
                        key = item.xpath("./div[1]/text()")
                        value = item.xpath("./div[2]/text()")

                        if key and value:
                            key = key[0].strip()
                            value = value[0].strip()

                            specification_data[title][key] = value

            # --------Safety-----------
            # 1. Main Title
            safety_title = tree.xpath("//h3[contains(text(),'Safety')]/text()")
            safety_title = safety_title[0].strip() if safety_title else "Safety"

            safety_data = {}
            subcategories = tree.xpath("//div[@data-index='2']//div[@data-subcategoryid]")

            for sub in subcategories:
                # get subcategory title
                title = sub.xpath(".//p[@class='o-j3 o-jj o-jJ']/text()")
                if not title:
                    continue
                title = title[0].strip()

                # get groups
                groups = sub.xpath(".//div[contains(@class,'o-eU')]")

                # CASE 1: if groups exist
                if groups:
                    safety_data[title] = {}

                    for g in groups:
                        g_name = g.xpath(".//p[contains(@class,'o-j1')]/text()")
                        if not g_name:
                            continue
                        g_name = g_name[0].strip()

                        # get ONLY feature names
                        features = g.xpath(".//div[@data-itemid]//span[last()]/text()")
                        features = [f.strip() for f in features if f.strip()]

                        safety_data[title][g_name] = features

                # CASE 2: no groups → direct list
                else:
                    features = sub.xpath(".//div[@data-itemid]//span[last()]/text()")
                    features = [f.strip() for f in features if f.strip()]

                    safety_data[title] = features


            # --------features-----------
            features_title = tree.xpath("//h3[contains(text(),'Features')]/text()")
            features_title = features_title[0].strip() if features_title else "Features"

            features_data = {}
            # loop subcategories
            subcategories = tree.xpath("//div[@data-index='3']//div[@data-subcategoryid]")

            for sub in subcategories:
                title = sub.xpath(".//p[@class='o-j3 o-jj o-jJ']/text()")
                if not title:
                    continue
                title = title[0].strip()

                # extract features (NO GROUP CASE)
                features = sub.xpath(".//div[@data-itemid]//span[last()]/text()")
                features = [f.strip() for f in features if f.strip()]

                # assign directly as list
                features_data[title] = features

            # -----------not features_data------------
            if not features_data: 
                features_data = {}
                # loop each section (like Exterior, Safety)
                sections = tree.xpath("//div[@data-index='1']//li[@class='o-kY o-mf o-lS o-lX o-mO']")

                for sec in sections:
                    # section title
                    title = sec.xpath(".//p[@class='o-jq o-j4']/text()")
                    if not title:
                        continue
                    title = title[0].strip()

                    features_data[title] = {}

                    # each item
                    items = sec.xpath(".//div[@data-itemid]//div[@data-testid]")

                    for item in items:
                        key = item.xpath("./div[1]/text()")
                        value = item.xpath("./div[2]/text()")

                        if key and value:
                            key = key[0].strip()
                            value = value[0].strip()

                            features_data[title][key] = value

            Highlights_list = Highlights_list if isinstance(Highlights_list, list) else []
            specification_data = specification_data if isinstance(specification_data, dict) else {}
            safety_data = safety_data if isinstance(safety_data, dict) else {}
            features_data = features_data if isinstance(features_data, dict) else {}

            if "Specifications" != "Specification":
                specification_title = "Specification"

            features_detail = {
                "variant_id" : variant_id,
                "variant_name" : variant_name,
                "variant_website_url" : variant_website_url,
                Highlights : json.dumps(Highlights_list) ,
                specification_title : json.dumps(specification_data),
                safety_title : json.dumps(safety_data),
                features_title : json.dumps(features_data)
            }

        else:
            print("not same url : ", response.url)
            features_detail = {
                "variant_id" : variant_id,
                "variant_name" : variant_name,
                "variant_website_url" : variant_website_url,
                "Highlights" : json.dumps([]),
                "Specification" : json.dumps({}),
                "Safety" : json.dumps({}),
                "Features" : json.dumps({})
            }


        specs_features_data_list.append(features_detail)

        # # insert data into table
        insert_features_detail_table(list_data=specs_features_data_list, variant_id= variant_id)

        # # update status
        update_car_detail_status(variant_id, "success")



from concurrent.futures import ThreadPoolExecutor, as_completed

def run_threaded_fetch(variants_url_list, max_threads=5):

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [
            # pass ONE dict as list → because your function expects list
            executor.submit(specs_features_detail, [dict_data] )
            for dict_data in variants_url_list
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print("Thread error:", e)
    print("process done....")



def check_Highlights():
    folder_path = r"D:\vishal_kushvanshi\car_vale_request\car_variant_pages"
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".html.gz"):
            file_path = os.path.join(folder_path, file_name)
            variant_id = int(file_name.replace(".html.gz", ""))

            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                html_content = f.read()

            tree = html.fromstring(html_content)

            # --------Highlights-----------
            Highlights = tree.xpath('//div[@class="o-l4 o-aE o-bGZtyi o-dnZVfP  o-f"]//h3/text()')
            Highlights = Highlights[0] if Highlights else "Highlights"

            Highlights_value = tree.xpath(
                "//p[contains(text(),'Available in Next Variant')]/text() | //div[@class='o-ne o-bQ o-fb o-eD o-mf o-lT o-mO o-kY o-C o-o o-j2']//div[@class='o-av o-aC o-c4 o-cA  o-bM o-ci o-f']//span[@class='o-b']/text()"
            )

            # Clean data
            Highlights_list = [i.strip() for i in Highlights_value if i.strip()]
            if Highlights_list:
                print("Highlights_list is availabe" , variant_id)
                
                # update status
                update_car_detail_status(variant_id, "success")
            else:
                print("not data in Highlights_list variant_id : ", variant_id)
            # break

    print("process done")


