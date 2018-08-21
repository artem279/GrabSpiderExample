from grab.spider import Spider, Task
from grab import Grab
import json, datetime, time, csv
import threading, logging

logging.basicConfig(filename='log.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
mutex = threading.Lock()
csv.register_dialect('csvCommaDialect', delimiter='|', lineterminator='\n')


def getvalue(tag):
    value = ''
    try:
        value = tag.replace('\n', '').replace('\r', '').replace('\t', '').replace('|', '/').replace('"', '') \
            .replace('  ', '').replace(';', ',')
    except:
        value = ''
    return value


class SupplBizLinkScrapSpider(Spider):
    def __init__(self, id_list, thread_number):
        Spider.__init__(self, thread_number=thread_number)
        self.id_list = id_list

    def task_generator(self):
        for lang in self.id_list:
            try:
                for i in range(50):
                    try:
                        url = 'https://supl.biz/api/v1.0/suppliers-catalog/search/?query=&city=russia&page=' + str(
                            i) + '&verified=&price_lte=&price_gte=&category=' + str(lang)

                        yield Task('getlinks', url=url, lang=lang)
                    except:
                        continue
            except:
                continue

    def task_getlinks(self, grab, task):
        print(task.url)
        response = json.loads(grab.response.unicode_body())
        with open('links.json', 'at', encoding='cp1251', errors='ignore') as f:
            f.write(str(task.lang) + '|' + json.dumps(response, ensure_ascii=False, sort_keys=True) + '\n')
            f.flush()


class SupplBizCategorySpider(Spider):
    def task_generator(self):
        for lang in ('9742', '9403', '8277', '5372', '10647', '8667', '7624', '11088', '7702', '9322', '7924', '8517',
                     '8097', '9270', '10523', '7592', '8980', '10375', '7777', '9879', '8646', '11439', '7756', '7696'):
            try:
                url = 'https://supl.biz/api/v1.0/suppliers-catalog/categories/' + lang + '/menu/'
                yield Task('getcategory', url=url, lang=url)
            except:
                continue

    def task_getcategory(self, grab, task):
        print(task.url)
        response = json.loads(grab.response.unicode_body())
        with open('category.json', 'at', encoding='cp1251', errors='ignore') as f:
            f.write(json.dumps(response, ensure_ascii=False, sort_keys=True) + '\n')
            f.flush()
        for child in response.get('children'):
            child_url = 'https://supl.biz/api/v1.0/suppliers-catalog/categories/' + str(child['id']) + '/menu/'
            yield Task('getcategory', url=child_url)


t0 = time.time()
bot1 = SupplBizCategorySpider(thread_number=12)
bot1.load_proxylist('proxylist.txt', source_type='text_file', proxy_type='socks4', auto_change=True)

bot1.run()

data = []
with open('category.json', 'rt', encoding='cp1251', errors='ignore') as f:
    while True:
        line = f.readline()
        if not line:
            break
        j = json.loads(line)
        data.append(j.get('id'))
        for x in j.get('children'):
            data.append(x['id'])

bot2 = SupplBizLinkScrapSpider(thread_number=12, id_list=data)
bot2.load_proxylist('proxylist.txt', source_type='text_file', proxy_type='socks4', auto_change=True)

bot2.run()

t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write(datetime.date.today().isoformat() + ' Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
