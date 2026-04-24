



import requests
import json
import os
from urllib.parse import urlencode
from store_data_database import *
import requests
import gzip
from lxml import html


import json




def brand_car_url(brand_name, brand_url):
    print("------brand_car_url---------")
    

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
        '_userModelHistory': '2033~2781',
        '_pageviews_modelid': '-1',
        '_cwv': 'w2ooYdKFmgo2XdXQLLC5qy1ci.vDXxiNjMia.1776932875.1776932875.1776932875.2',
        '_cwutmzsrc': 'D%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD',
        '_cwutmzmed': 'NN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN',
        'bhs_cw': 'w2ooYdKFmgo2XdXQLLC5qy1ci.u361tkeBeS.1776932877.1776932877.1776932879.2',
        '__eoi': 'ID=68a27d1d8813ec1e:T=1776926875:RT=1776932885:S=AA-AfjaNmMEambtNL0Vlr2tvGnpk',
        'cebsp_': '15',
        '_ce.s': 'lcw~1776932886365~v~bd0af91b7db415eda09c18c6e954e6dc9d877a01~vir~returning~lva~1776932886364~vpv~0~as~false~v11ls~5a45abd0-3eee-11f1-b1d0-3122e0fa70f0~v11.cs~44156~v11.s~5a45abd0-3eee-11f1-b1d0-3122e0fa70f0~v11.vs~bd0af91b7db415eda09c18c6e954e6dc9d877a01~v11.fsvd~eyJub3RNb2RpZmllZFVybCI6Imh0dHBzOi8vd3d3LmNhcndhbGUuY29tL21hcnV0aS1zdXp1a2ktY2Fycy8iLCJ1cmwiOiJjYXJ3YWxlLmNvbS9tYXJ1dGktc3V6dWtpLWNhcnMiLCJyZWYiOiIiLCJ1dG0iOltdfQ%3D%3D~v11.sla~1776932886289~v11.ss~1776932886297~lcw~1776932886366',
        '_ga_Z81QVQY510': 'GS2.1.s1776926803$o1$g1$t1776932975$j59$l0$h0',
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
        # 'cookie': 'CWC=w2ooYdKFmgo2XdXQLLC5qy1ci; _cwutmz=utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29%7Cutmtrm%3D%7Cutmcnt%3D; CurrentLanguage=en; _abtest=79; languageSelected=en; .AspNetCore.Antiforgery.9TtSrW0hzOs=CfDJ8JUh_GoNOjhBqUJ2d7OuCzSFiakfiDt9WyfchA_zA-n0CBND7WNAPPKAYbzn830z_LEGCuTQ-s7WHyq-5EMQP43g8mC4-WPF0ba6osrp-O4Q86DUq2erq60fmaFXPTVSJGZWHHqnmmGZNyNuz9Bvofs; _gcl_au=1.1.1073683189.1776926802; _carSearchType=1; BHC=w2ooYdKFmgo2XdXQLLC5qy1ci; _ga=GA1.1.52096775.1776926803; _fbp=fb.1.1776926808507.424654618933864771; cebs=1; _ce.clock_data=46%2C45.114.65.131%2C1%2Cb87543ecbc0ba610d9f06f9f2c432a46%2CChrome%2CIN; vernacularPopupClose=1; __gads=ID=9cb6bc50c25459ea:T=1776926875:RT=1776926875:S=ALNI_MZU_ylBXabM20P3nfK5o0SK5PxLlg; __gpi=UID=000013e12997897f:T=1776926875:RT=1776926875:S=ALNI_MYb89m-42YAon5LCxGhrjOM7L23LQ; _uetsid=359895203ee011f18389d591c81e3005; _uetvid=3598af703ee011f1a0a04794f1261607; FCCDCF=%5Bnull%2Cnull%2Cnull%2C%5B%22CQjHcoAQjHcoAEsACBENCbFgAAAAAEPgABBoAAAWEQD-F2I2EKFEGCuQUYIYBCuACAAxYBgAAwCBgAAGCQgQAgFJIIkCAEAIEAAEAAAQAgCAABQEBAAAIAAAAAqAACAABgAQCAQAIABAAAAgIAAAAAAEQAAIgEAAAAIAIABABAAAAQAkIAAAAAAAAECAAAAAAAAAAAAAAAAAAIAAEABgAAAAAABEAAAAAAAACAQIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAIAA.ILCIB_C7EbCFCiDJ3IKMEMAhXABBAYsAwAAYBAwAADBIQIAQCkkEaBASAFCACCAAAKASBAAAoCAgAAUAAIAAVAABAAAwAIBAIIEAAgAAAQEAIAAAACIAAEQCAAAAEAEAAkAgAAAIASEAAAAAAAACBAAAAAAAAAAAAAAAABAEAASAAwAAAAAAAiAAAAAAAABAIEAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAABAAAAAAAQgAAE.YAAAAAAAAAA%22%2C%222~~dv.61.89.122.161.184.196.230.314.442.445.494.550.576.827.1029.1033.1046.1047.1051.1097.1126.1166.1301.1342.1415.1725.1765.1942.1958.1987.2068.2072.2074.2107.2213.2219.2223.2224.2328.2331.2416.2501.2567.2568.2575.2657.2686.2778.2869.2878.2908.2920.2963.3005.3023.3126.3234.3235.3253.3309.3731.6931.8931.13731.15731.33931%22%2C%2266C94EF8-B31A-43DE-BC0D-DEB8ED4C1BD9%22%5D%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%223003381c-d732-4b25-9797-2ece13f25452%5C%22%2C%5B1776926809%2C100000000%5D%5D%22%5D%5D%5D; quizSlug2={%222781%22:%22question%22}; _userModelHistory=2033~2781; _pageviews_modelid=-1; _cwv=w2ooYdKFmgo2XdXQLLC5qy1ci.vDXxiNjMia.1776932875.1776932875.1776932875.2; _cwutmzsrc=D%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD%7CD; _cwutmzmed=NN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN%7CNN; bhs_cw=w2ooYdKFmgo2XdXQLLC5qy1ci.u361tkeBeS.1776932877.1776932877.1776932879.2; __eoi=ID=68a27d1d8813ec1e:T=1776926875:RT=1776932885:S=AA-AfjaNmMEambtNL0Vlr2tvGnpk; cebsp_=15; _ce.s=lcw~1776932886365~v~bd0af91b7db415eda09c18c6e954e6dc9d877a01~vir~returning~lva~1776932886364~vpv~0~as~false~v11ls~5a45abd0-3eee-11f1-b1d0-3122e0fa70f0~v11.cs~44156~v11.s~5a45abd0-3eee-11f1-b1d0-3122e0fa70f0~v11.vs~bd0af91b7db415eda09c18c6e954e6dc9d877a01~v11.fsvd~eyJub3RNb2RpZmllZFVybCI6Imh0dHBzOi8vd3d3LmNhcndhbGUuY29tL21hcnV0aS1zdXp1a2ktY2Fycy8iLCJ1cmwiOiJjYXJ3YWxlLmNvbS9tYXJ1dGktc3V6dWtpLWNhcnMiLCJyZWYiOiIiLCJ1dG0iOltdfQ%3D%3D~v11.sla~1776932886289~v11.ss~1776932886297~lcw~1776932886366; _ga_Z81QVQY510=GS2.1.s1776926803$o1$g1$t1776932975$j59$l0$h0',
    }

    # Folder to save JSON
    folder = r"D:\vishal_kushvanshi\car_vale_request\brand_pages"
    os.makedirs(folder, exist_ok=True)


    response = requests.get(brand_url, cookies=cookies, headers=headers)

    # with open("brand_name_page.html", "w", encoding='utf-8') as f:
    #     f.write(response.text)

    with gzip.open(f"{folder}\\{brand_name}_page.html.gz", "wt", encoding='utf-8') as f:
        f.write(response.text)
    
    tree = html.fromstring(response.text)

    cars_data_list = []

    for card in tree.xpath("//div[@class='o-C o-cF o-c9 o-bT o-cp o-f']"):
        a = card.xpath(".//a[h3]")

        if a:
            a = a[0]
            car_url = "https://www.carwale.com" + a.xpath("./@href")[0]
            car_name = a.xpath("./h3/text()")[0].strip()

            cars_data_list.append({
                "brand_name" : brand_name,
                "brand_url" : brand_url,
                "car_name": car_name,
                "car_url": car_url,
                "status" : "pending"
            })


    insert_cars_url_table(list_data=cars_data_list)






def extract_store_urls():
    # Folder to save JSON
    folder = r"D:\vishal_kushvanshi\car_vale_request\main_page"
    os.makedirs(folder, exist_ok=True)

    cookies = {
        'CWC': 'w2ooYdKFmgo2XdXQLLC5qy1ci',
        '_cwutmz': 'utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29%7Cutmtrm%3D%7Cutmcnt%3D',
        'CurrentLanguage': 'en',
        '_pageviews_modelid': '-1',
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
        '__eoi': 'ID=68a27d1d8813ec1e:T=1776926875:RT=1776926875:S=AA-AfjaNmMEambtNL0Vlr2tvGnpk',
        'FCCDCF': '%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%223003381c-d732-4b25-9797-2ece13f25452%5C%22%2C%5B1776926809%2C100000000%5D%5D%22%5D%5D%5D',
        'FCNEC': '%5B%5B%22AKsRol_kzpdLblvC5gQJIwD1V2FPqBsG4rvJY-zkJDoN-VJSrIwii_3dvMpLOY5S-A9HYMjsxY74C_Oqb3uoco6CTeBwXsEl3ayZ_1Qg_d3BrlD_tYVZ1bZAN5XslcXpZHrmnnOmYmrSoxMFcF9PUsCiEI5NudOzyg%3D%3D%22%5D%5D',
        '_uetsid': '359895203ee011f18389d591c81e3005',
        '_uetvid': '3598af703ee011f1a0a04794f1261607',
        '_cwv': 'w2ooYdKFmgo2XdXQLLC5qy1ci.uAKNoIpwm4.1776926802.1776926924.1776927159.1',
        '_cwutmzsrc': 'D%7CD%7CD',
        '_cwutmzmed': 'NN%7CNN%7CNN',
        'bhs_cw': 'w2ooYdKFmgo2XdXQLLC5qy1ci.uAKNoIpwm4.1776926802.1776926925.1776927167.1',
        '_ga_Z81QVQY510': 'GS2.1.s1776926803$o1$g1$t1776927169$j50$l0$h0',
        'cebsp_': '4',
        '_ce.s': 'lcw~1776927174845~v~bd0af91b7db415eda09c18c6e954e6dc9d877a01~vir~new~lva~1776927176124~vpv~0~as~false~v11.cs~44156~v11.s~34282a80-3ee0-11f1-9cda-f79718569390~v11.vs~bd0af91b7db415eda09c18c6e954e6dc9d877a01~v11.fsvd~eyJub3RNb2RpZmllZFVybCI6Imh0dHBzOi8vd3d3LmNhcndhbGUuY29tLyIsInVybCI6ImNhcndhbGUuY29tIiwicmVmIjoiIiwidXRtIjpbXX0%3D~v11.sla~1776926809387~v11.ss~1776926809393~v11ls~34282a80-3ee0-11f1-9cda-f79718569390~lcw~1776927176125',
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
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
        # 'cookie': 'CWC=w2ooYdKFmgo2XdXQLLC5qy1ci; _cwutmz=utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29%7Cutmtrm%3D%7Cutmcnt%3D; CurrentLanguage=en; _pageviews_modelid=-1; _abtest=79; languageSelected=en; .AspNetCore.Antiforgery.9TtSrW0hzOs=CfDJ8JUh_GoNOjhBqUJ2d7OuCzSFiakfiDt9WyfchA_zA-n0CBND7WNAPPKAYbzn830z_LEGCuTQ-s7WHyq-5EMQP43g8mC4-WPF0ba6osrp-O4Q86DUq2erq60fmaFXPTVSJGZWHHqnmmGZNyNuz9Bvofs; _gcl_au=1.1.1073683189.1776926802; _carSearchType=1; BHC=w2ooYdKFmgo2XdXQLLC5qy1ci; _ga=GA1.1.52096775.1776926803; _fbp=fb.1.1776926808507.424654618933864771; cebs=1; _ce.clock_data=46%2C45.114.65.131%2C1%2Cb87543ecbc0ba610d9f06f9f2c432a46%2CChrome%2CIN; vernacularPopupClose=1; __gads=ID=9cb6bc50c25459ea:T=1776926875:RT=1776926875:S=ALNI_MZU_ylBXabM20P3nfK5o0SK5PxLlg; __gpi=UID=000013e12997897f:T=1776926875:RT=1776926875:S=ALNI_MYb89m-42YAon5LCxGhrjOM7L23LQ; __eoi=ID=68a27d1d8813ec1e:T=1776926875:RT=1776926875:S=AA-AfjaNmMEambtNL0Vlr2tvGnpk; FCCDCF=%5Bnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%5B%5B32%2C%22%5B%5C%223003381c-d732-4b25-9797-2ece13f25452%5C%22%2C%5B1776926809%2C100000000%5D%5D%22%5D%5D%5D; FCNEC=%5B%5B%22AKsRol_kzpdLblvC5gQJIwD1V2FPqBsG4rvJY-zkJDoN-VJSrIwii_3dvMpLOY5S-A9HYMjsxY74C_Oqb3uoco6CTeBwXsEl3ayZ_1Qg_d3BrlD_tYVZ1bZAN5XslcXpZHrmnnOmYmrSoxMFcF9PUsCiEI5NudOzyg%3D%3D%22%5D%5D; _uetsid=359895203ee011f18389d591c81e3005; _uetvid=3598af703ee011f1a0a04794f1261607; _cwv=w2ooYdKFmgo2XdXQLLC5qy1ci.uAKNoIpwm4.1776926802.1776926924.1776927159.1; _cwutmzsrc=D%7CD%7CD; _cwutmzmed=NN%7CNN%7CNN; bhs_cw=w2ooYdKFmgo2XdXQLLC5qy1ci.uAKNoIpwm4.1776926802.1776926925.1776927167.1; _ga_Z81QVQY510=GS2.1.s1776926803$o1$g1$t1776927169$j50$l0$h0; cebsp_=4; _ce.s=lcw~1776927174845~v~bd0af91b7db415eda09c18c6e954e6dc9d877a01~vir~new~lva~1776927176124~vpv~0~as~false~v11.cs~44156~v11.s~34282a80-3ee0-11f1-9cda-f79718569390~v11.vs~bd0af91b7db415eda09c18c6e954e6dc9d877a01~v11.fsvd~eyJub3RNb2RpZmllZFVybCI6Imh0dHBzOi8vd3d3LmNhcndhbGUuY29tLyIsInVybCI6ImNhcndhbGUuY29tIiwicmVmIjoiIiwidXRtIjpbXX0%3D~v11.sla~1776926809387~v11.ss~1776926809393~v11ls~34282a80-3ee0-11f1-9cda-f79718569390~lcw~1776927176125',
    }

    response = requests.get('https://www.carwale.com/', cookies=cookies, headers=headers)

    with open("main_page.html", "w", encoding='utf-8') as f:
        f.write(response.text)

    with gzip.open(f"{folder}\\main_page.html.gz", "wt", encoding='utf-8') as f:
        f.write(response.text)

    # Parse HTML
    tree = html.fromstring(response.text)

    # Step 1: Get script content
    script = tree.xpath("//script[contains(., '__INITIAL_STATE__')]/text()")[0]

    # Step 2: Extract JSON using brace matching
    start = script.find("window.__INITIAL_STATE__")
    start = script.find("{", start)

    brace_count = 0
    end = start

    for i in range(start, len(script)):
        if script[i] == "{":
            brace_count += 1
        elif script[i] == "}":
            brace_count -= 1

        if brace_count == 0:
            end = i + 1
            break

    json_str = script[start:end]

    # Step 3: Convert to Python dict
    python_dict = json.loads(json_str)

    # Step 4: Save to file (indent = 4)
    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(python_dict, f, indent=4, ensure_ascii=False)

    # assuming your full JSON is stored in `python_dict`
    make_list = python_dict.get("homePage", {}).get("makeList", [])

    result = []

    for item in make_list:
        brand_name = item.get("makeName", "").lower()
        masking_name = item.get("maskingName", "")

        brand_url = f"https://www.carwale.com/{masking_name}-cars/"

        brand_car_url(brand_name, brand_url)

        result.append({
            "brand_name": brand_name,
            "url": brand_url
        })

    print("process Done!")
