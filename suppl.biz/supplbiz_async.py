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


class SupplBizSpider(Spider):
    def task_generator(self):
        for lang in open('supplbiz.csv', 'rt', encoding='utf-8', errors='ignore'):
            try:
                url = getvalue(lang)
                yield Task('parse', url=url, lang=url)
            except:
                continue

    def task_parse(self, grab, task):
        suppliers = []
        supplier = {}
        try:
            response = json.loads(grab.response.unicode_body())
            # To DO
            supplier['id'] = getvalue(str(response.get('id')))
            supplier['url'] = task.lang
            supplier['title'] = getvalue(response.get('title'))
            supplier['company_name'] = getvalue(response.get('company_name'))
            supplier['score'] = getvalue(response.get('score'))
            supplier['rating'] = getvalue(response.get('rating'))
            supplier['reviews'] = getvalue(response.get('reviews'))
            supplier['name'] = getvalue(response.get('name'))
            supplier['phone'] = getvalue(response.get('phone'))
            supplier['address'] = getvalue(response.get('address'))
            supplier['inn'] = getvalue(response.get('company').get('inn'))
            supplier['kpp'] = getvalue(response.get('company').get('kpp'))
            supplier['ogrn'] = getvalue(response.get('company').get('ogrn'))
            supplier['origin'] = getvalue(response.get('origin'))
            supplier['views_amount'] = getvalue(response.get('views_amount'))
            supplier['check_date'] = getvalue(response.get('check_date'))
            suppliers.append(supplier)
            print(task.lang)
        except:
            print('oops!')

        if len(suppliers) > 0:
            data.extend(suppliers)


t0 = time.time()
data = []
bot = SupplBizSpider(thread_number=12)
bot.load_proxylist('proxylist.txt', source_type='text_file', proxy_type='socks4', auto_change=True)
bot.run()

with open('data_async.csv', 'at', encoding='cp1251', errors='ignore') as file:
    writer = csv.DictWriter(file, ['id', 'url', 'title', 'company_name', 'score', 'rating', 'reviews', 'name', 'phone',
                                   'address', 'inn', 'kpp', 'ogrn', 'origin', 'views_amount', 'check_date'],
                            dialect='csvCommaDialect')
    writer.writerows(data)

t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('timelog.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write(datetime.date.today().isoformat() + ' Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
