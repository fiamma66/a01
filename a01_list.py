import requests
import pandas as pd
import log
from bs4 import BeautifulSoup
import time
import datetime
from a01_sele import jav, engine


HTML = 'https://www.av01.tv/videos?o=mr&page={}'
domain_name = 'https://av01.tv'
logger = log.getLogger('a01 Update List')


def get_sec(time_str):
    """Get Seconds from time."""
    try:
        h, m, s = time_str.split(':')
    except ValueError:
        m, s = time_str.split(':')
        h = 0

    return int(h) * 3600 + int(m) * 60 + int(s)


def get_crawl_list(html):

    craw_list = []
    logger.info('Getting New Upload Page for 8 Page')

    for i in range(1, 8):

        r = requests.get(html.format(i))

        if r.status_code == 200:
            content = r.content.decode('UTF-8')
            bs = BeautifulSoup(content)
        else:
            logger.error('Connection Error !')
            raise RuntimeError

        for css in bs.select('div.col-sm-6.col-md-4.col-lg-4'):
            link = domain_name + css.find('a').get('href')

            # Name Update at 2020/08/31
            name = css.select('div.video-views')[0].text.replace('\n', '')

            duration = css.select('div.duration')[0].text.replace('\n', '')

            craw_list.append({'href': link, 'name': name, 'status': 'N',
                              'duration': '',
                              'finish time': '',
                              'update time': str(datetime.datetime.now()),
                              'chunk time': get_sec(duration)})

        logger.info('Loading Page {}'.format(i))
        time.sleep(2)

    return craw_list


def merge_status(html):
    craw_list = get_crawl_list(html)

    new_df = pd.DataFrame(craw_list)

    logger.info('Reading jav_index ')

    ori_df = pd.read_sql(jav.select(), con=engine)

    logger.info('Success Reading jav_index')

    logger.info('Concat Table')
    m = pd.concat([ori_df, new_df]).drop_duplicates(['name', 'href'])

    logger.info('Insert Postgres ')
    m.to_sql('jav_index', con=engine, schema='public', if_exists='replace', index=False)
    logger.info('Success Insert')
    logger.info('New Row Count : {}'.format(m.shape[0] - ori_df.shape[0]))


if __name__ == '__main__':
    merge_status(html=HTML)
