import csv
import json
from queue import Queue
import threading
import requests

from bs4 import BeautifulSoup

class Parser:

    def __init__(self, config):
        self.shop_id = config['shop_id']
        self.num_threads = config['num_threads']
        self.base_url = config['base_url']
        self.url_set_queue = Queue()
        self.data_lock = threading.Lock()
        self.all_data = []
        self.file = open('data.csv', mode='w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(self.file, fieldnames=['title', 'sku', 'price', 'price_old', 'is_active', 'count'])
        self.session = requests.Session()
        self.session.headers.update({
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'ru,en;q=0.9',
            'cache-control': 'max-age=0',
            'cookie': '_gid=GA1.2.123900726.1718957273; _ym_uid=1718957274669523564; _ym_d=1718957274; BX_USER_ID=989cda687b75ee4c5882ae14fe0a1e74; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; IS_AUTHORIZED_USER=N; BITRIX_SM_advcake_trackid=37ed9e95a8a3165937a49c650d62b3e3; BITRIX_SM_advcake_url=%2F%3Fgsaid%3D88739%26_gs_ref%3D866f38613d8f51790d679a09dd3e5cc483b60ed7%26_gs_cttl%3D30%26advcake_params%3D866f38613d8f51790d679a09dd3e5cc483b60ed7%26utm_medium%3Dcpa%26advcake%3D1%26utm_content%3D88739%26utm_source%3Dadvcake%26utm_campaign%3Dgdeslon; BITRIX_SM_CPA_LASTCOOKIE=ADV_CAKE_UID; _userGUID=0:lxn6mqrh:KePUBoJmxBexrlYAB6LYIhp8W~HnhlPd; tmr_lvid=ed886d962782db54d49a523b5c76ca5a; tmr_lvidTS=1718957276413; _ga_05BC0PH6PR=GS1.1.1718957276.1.0.1718957276.60.0.0; advcake_last_utm=advcake; advcake_url=https%3A%2F%2Fwww.bethowen.ru%2F%3Fgsaid%3D88739%26_gs_ref%3D866f38613d8f51790d679a09dd3e5cc483b60ed7%26_gs_cttl%3D30%26advcake_params%3D866f38613d8f51790d679a09dd3e5cc483b60ed7%26utm_medium%3Dcpa%26advcake%3D1%26utm_content%3D88739%26utm_source%3Dadvcake%26utm_campaign%3Dgdeslon; advcakeUrl=https%3A%2F%2Fwww.bethowen.ru%2F%3Fgsaid%3D88739%26_gs_ref%3D866f38613d8f51790d679a09dd3e5cc483b60ed7%26_gs_cttl%3D30%26advcake_params%3D866f38613d8f51790d679a09dd3e5cc483b60ed7%26utm_medium%3Dcpa%26advcake%3D1%26utm_content%3D88739%26utm_source%3Dadvcake%26utm_campaign%3Dgdeslon; user_unic_ac_id=c1d90731-0819-4225-ffba-bb32a2717b7f; advcake_utm_content=88739; advcake_utm_campaign=gdeslon; advcake_utm_source=advcake; advcake_params=866f38613d8f51790d679a09dd3e5cc483b60ed7; flocktory-uuid=1256689d-0c73-4524-aa8f-7444a9b3b102-6; analytic_id=1718957277456528; gdeslon.ru.__arc_domain=gdeslon.ru; gdeslon.ru.user_id=eab14c05-5a5c-43ff-b99b-ea35c1ac95e5; _acfId=918ea827-a5f3-41b6-b50a-fae35b582058; aprt_last_partner=actionpay; aprt_last_apclick=; aprt_last_apsource=508534; BETHOWEN_GEO_TOWN=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3; BETHOWEN_GEO_TOWN_ID=85; digi_uc=W1sidiIsIjUyODA5MyIsMTcxODk2NzI0OTYyMV0sWyJ2IiwiMjE3NzM5IiwxNzE4OTczMjY2MDA2XV0=; _ym_isad=2; domain_sid=Tlc4h_yD-QBYmnMkid2TM%3A1719054597263; PHPSESSID=gqj9gnqo89mhodhl3juc9s5e6r; BITRIX_SM_SALE_UID=721020b80820aeddc3208c6611cd1c80; _ym_debug=null; iwaf_http_cookie_d49df1=a5762ee6229c74842b5c6f24be67821a034d66921aa4a45a2472e85459f57879; _ym_visorc=b; iwaf_js_cookie_d49df1=71c8fb39d55392e98c878fbfade810c35910b92bf1872b55e0332977896a2a78; dSesn=aca890a9-a305-8902-2cc3-42fa5f48d3f6; _dvs=0:lxqdc5pu:DRm7Uu~lr1FK3dFhChaMW~VMkg46MHkF; _ga_XV397Q6BXS=GS1.1.1719075782.8.1.1719075786.56.0.0; advcake_session=1; _ga=GA1.2.1271969666.1718883124; _gat_gtag_UA_74359728_1=1; _ga_DD0749TTTB=GS1.1.1719075782.11.1.1719075791.51.0.0; iwaf_fingerprint=2fcb664287fa66e237eddcd710d875fb; iwaf_scroll_event=176; mindboxDeviceUUID=e97a7db0-311c-406b-88c0-1e222d8a1372; directCrm-session=%7B%22deviceGuid%22%3A%22e97a7db0-311c-406b-88c0-1e222d8a1372%22%7D; tmr_detect=0%7C1719075794335; activity=8|10',
            'priority': 'u=0, i',
            'referer': 'https://www.bethowen.ru/',
            'sec-ch-ua': '"Chromium";v="124", "YaBrowser";v="24.6", "Not-A.Brand";v="99", "Yowser";v="2.5"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 YaBrowser/24.6.0.0 Safari/537.36'        })


    def run(self):
        self.url_set_queue.put(('pars_initial', self.base_url, {})) # добавляем функцию обработчика и начальную ссылку в очередь
        '''
        Здесь же можно указать ссылку(-ки) на необходимые категории, с функцией обрабатывающей категорию. 
        '''

        self.writer.writeheader()

        threads = []
        for _ in range(self.num_threads):
            thread = threading.Thread(target=self.worker)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        self.file.close()

    def worker(self):
        while not self.url_set_queue.empty():
            url_set = self.url_set_queue.get()
            method = getattr(self, url_set[0]) # кортеж содержит ссылку и название функции, которая выполнит запрос 
            method(url_set[1], url_set[2]) # кортеж содержит аргумент, в котором можно при необходимости передать данные 
            self.url_set_queue.task_done()

    def pars_initial(self, url, data=None):
        '''
        Одна из функций, которая будет взвана для обработки ссылки из очереди
        '''
        response = self.session.get(url)
        print('Initial', response, url)
        soup = BeautifulSoup(response.text, 'html.parser')
        urls = soup.select('.section_info li.sect a')
        for obj in urls:
            url = f'https://www.bethowen.ru{obj['href']}' 
            self.url_set_queue.put(('pars_list', url, None)) # добавляем извлеченную ссылку в очередь и указываем функцию обработчика

    def pars_list(self, url, data=None):
        response = self.session.get(url)
        print('List', response, url)
        soup = BeautifulSoup(response.text, 'html.parser')
        item_ids = soup.select('.bth-card-element')
        for item_id in item_ids:
            url = f'https://www.bethowen.ru/api/local/v1/catalog/list?limit=20&offset=0&sort_type=popular&id[]={item_id['data-product-id']}'
            self.url_set_queue.put(('pars_api_item', url, None))

        next_page = soup.select('.cur + a')
        if next_page:
            new_url = f'https://www.bethowen.ru{next_page[0]["href"]}'
            self.url_set_queue.put(('pars_list', new_url, None))

    def pars_api_item(self, url, data=None):
        response = self.session.get(url)
        print('Api_item', response, url)
        item = {}
        js_data = json.loads(response.text)
        try:
            item['title'] = js_data['products'][0]['name']
            for offer in js_data['products'][0]['offers']:
                url = f'https://www.bethowen.ru/api/local/v1/catalog/offers/{offer['id']}/details'
                self.url_set_queue.put(('pars_api_offer', url, item))
        except (IndexError, KeyError) as e:
            print(f"Error item data for URL {url}: {e}")
            
    def pars_api_offer(self, url, data=None):
        response = self.session.get(url)
        print('Api_offer', response, url)
        js_data = json.loads(response.text)
        item = data

        size = js_data['size']
        item['title'] = f'{item['title']} {size}'

        item['sku'] = js_data['vendor_code']

        item['price'] = js_data['retail_price']

        item['price_old'] = js_data['discount_price']
        if item['price'] == item['price_old']:
            item['price_old'] = None

        item['is_active'] = False
        item['count'] = None
        for shop in js_data['availability_info']['offer_store_amount']:
            if shop['shop_id'] == self.shop_id:
                item['is_active'] = True
                item['count'] = shop['availability']['text']

        url = js_data['sharing_url']

        with self.data_lock:
            self.writer.writerow(item)

        print(f'Save {item['sku']} ({url})')
