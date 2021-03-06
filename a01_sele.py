from selenium import webdriver, common
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as Ec
import time
import re
# import json
import log
import requests
import pathlib
import concurrent.futures
import subprocess
import math
import sys
import datetime
from sqlalchemy import create_engine, Table, MetaData
# import fcntl
import threading
import random
from ip import get_ip

# import logging
# HTML = 'https://www.av01.tv/videos?o=mr'
# domain_name = 'https://av01.tv'

engine = create_engine('postgresql+psycopg2://trinity:trinity@10.140.0.2:5432/jav')
meta = MetaData()
jav = Table('jav_index', meta, autoload=True, autoload_with=engine)

main_host = '10.140.0.2'
main_user = 'fiamma0320'

logger = log.getLogger(__name__)
# logger.level = logging.DEBUG

folder_path = pathlib.Path('av01')

url_pattern = re.compile(r'(https?://cdn\.av01\.tv/v[0-9]/[0-9]*[a-z]*/.*/[a-z]*/[a-z]*[0-9]*[a-z]*-)([0-9]*)(-.*)',
                         re.IGNORECASE)

g_header = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) \
    Chrome/84.0.4147.125 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,\
    image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7',
    'Connection': 'close',

}


def get_network_resources(_driver):
    resources = _driver.execute_script("return window.performance.getEntries();")

    return resources


def p1080_or_720(driver):
    p_1080 = driver.find_element_by_css_selector('div.vjs-menu.vjs-lock-showing')
    p_1080 = p_1080.find_element_by_css_selector('ul.vjs-menu-content')

    try:
        logger.info('Change to 1080P')
        p_1080.find_element_by_xpath('//span[text()="1080p"]').click()
    except common.exceptions.NoSuchElementException:
        logger.info('Change to 720P')
        p_1080.find_element_by_xpath('//span[text()="720p"]').click()


class VideoCatch:
    # Selenium Option And Initialize
    options = webdriver.ChromeOptions()
    options.add_argument("--enable-javascript")
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")

    class_to_use = 'video-container video-js vjs-16-9 vjs-default-skin \
    vjs-big-play-centered vjs-controls-enabled vjs-workinghover vjs-v7 \
    vjs-http-source-selector vjs-seek-buttons vjs-has-started my-video-dimensions \
    vjs-vtt-thumbnails vjs-paused vjs-user-active'
    # class_to_use = re.sub(r' +', ' ', class_to_use)

    def __init__(self, url, sub_folder, chunk):
        self.max_range = chunk
        self.url = url
        self.video_url = None
        self.path = folder_path / sub_folder
        self.list_file = None
        self.name = sub_folder
        self.file_name = '{}.mp4'.format(sub_folder)
        # Making Sure Path is Created
        if not self.path.exists():
            self.path.mkdir(parents=True)

        self.lock = threading.Lock()
        self.retry_list = []

        self.file = self.path / self.file_name

    def get_network_url(self):

        driver = webdriver.Chrome(options=self.options)
        wait = WebDriverWait(driver, 80)

        logger.info('Initializing Selenium Driver Open Page')

        driver.get(self.url)
        logger.info('Success Open Page ')

        # time.sleep(2)
        button = wait.until(
            Ec.element_to_be_clickable(
                (By.CSS_SELECTOR, 'button.vjs-big-play-button')
            )
        )
        try:
            button.click()
        except Exception as e:
            logger.debug(e)
            driver.execute_script("arguments[0].click();", button)

        logger.info('PLAYING Button Click ')
        # button = driver.find_element_by_class_name('vjs-big-play-button')
        # button.click()
        # time.sleep(5)

        # Find Change 畫質
        try:
            user_active = wait.until(
                Ec.presence_of_element_located(
                    (By.ID, 'my-video')
                )
            )
            time.sleep(7)

            wait.until(
                Ec.presence_of_element_located(
                    (By.XPATH, '//button[@class="vjs-menu-button vjs-menu-button-popup vjs-icon-cog vjs-button"]')
                )
            )

            logger.info('Setting Attribute')
            driver.execute_script(
                "arguments[0].setAttribute('class','{}')".format(self.class_to_use),
                user_active
            )

            logger.info('Checking Attribute @ Class : {}'.format(user_active.get_attribute('class')))
            change_button = wait.until(
                Ec.presence_of_element_located(
                    (By.XPATH, '//button[@class="vjs-menu-button vjs-menu-button-popup vjs-icon-cog vjs-button"]')
                )
            )
            try:
                change_button.click()
            except Exception as e:
                logger.debug(e)
                logger.error('Cant Click on Change Button, Using JavaScript !')
                driver.execute_script("arguments[0].click();", change_button)

        except Exception as e:
            logger.debug(e)
            logger.error('Timeout For Ad !')
            driver.close()
            return

        p1080_or_720(driver)
        time.sleep(10)

        logger.info('Getting Network Resource Log')
        network = get_network_resources(driver)
        new_url = ''
        for a in network:
            logger.debug(a.get('name'))

            matcher = re.match(url_pattern, a.get('name'))

            if matcher:
                new_url = matcher.group(1) + '{}' + matcher.group(3)
                logger.debug('Success get url : {}'.format(new_url))

        driver.close()

        self.video_url = new_url

    def _retry_api_url(self):
        """

        :return: Change class self.video_url
        """

        lock = self.lock.acquire(timeout=5)

        if lock:
            logger.warning('Locking Driver !!!')

            try:
                self.get_network_url()
                # logger.warning('Getting New URL : {}'.format(self.video_url))
            except Exception as e:
                logger.error(e)
            finally:
                self.lock.release()

        else:
            # Catch BlockIOError
            logger.warning('Driver Locked ! Waiting...')
            # lock.close()
            time.sleep(random.random() * 180)
            logger.warning('Exit Waiting Driver Lock')

    def download_and_check(self, url, _num, retry=False):

        # Downloading
        time.sleep(random.random() * 10)
        logger.debug('Getting Chunk Number : {}'.format(_num))
        if _num % 100 == 0:
            logger.info('Chunk Now Reach : {}'.format(_num))
        logger.debug('Chunk URL : {}'.format(url))
        h = requests.get(url, headers=g_header)

        tv = pathlib.Path()

        # Looking for new url if 403 forbidden
        # Changing global url
        if h.status_code == 403:
            logger.warning('Chunk URL Been Baned !')
            logger.warning('Regetting ChunkURL !')
            logger.info('Getting File Lock')

            self._retry_api_url()
            # self.url = self.get_network_url()
            # global change

            self.download_and_check(url, _num)

        elif h.status_code == 200:

            try:
                h.content.decode('utf-8')
                time.sleep(random.randint(1, 5))
                logger.error('Response not BLOB ! Retry')
                self.download_and_check(url, _num)
            except UnicodeDecodeError:
                pass

            if not self.path.exists():
                logger.info('Making Dir : {}'.format(self.path))
                self.path.mkdir(parents=True)

            tv = self.path / (str(_num) + '.ts')
            logger.debug('Write Video Raw {}'.format(tv.name))
            with open(tv, 'wb') as f:
                f.write(h.content)
        # status Code 404 or 400
        elif h.status_code == 404:
            logger.debug('Cant find Chunk ! Ignore !')
            return

            # return 'Done By Reach Max Range !'

        elif h.status_code == 429:
            # 429 too many requests
            # Change request api url
            logger.warning('Too Many Requests on Current API : {}'.format(self.video_url))
            # logger.warning('Response Header : ')
            # logger.warning(json.dumps(dict(h.headers), indent=3))
            self._retry_api_url()

            self.download_and_check(url, _num)

        else:
            if not retry:
                logger.warning('Getting Response Code {}'.format(h.status_code))
                # logger.error(h.content)

                logger.warning('Retry')
                # time.sleep(random.randint(3, 10))
                logger.warning('Retry Chunk : {}'.format(_num))
                # self.download_and_check(self.url.format(_num), _num)
                # self.max_range = _num
                self.retry_list.append(_num)
                return _num
            else:
                logger.warning('Retry Sleep 60')
                time.sleep(random.randint(40, 100))
                self.download_and_check(url, _num)

            # return 'Done By Reach Max Range !'

        # Checking
        file_stat = tv.stat().st_size
        logger.debug('Chunk Size : {} Byte'.format(file_stat))
        if tv.is_file() and file_stat / 1024 <= 1:
            logger.warning('Chunk Size abnormal !')
            logger.warning('Chunk Name : {}'.format(tv.name))
            logger.warning('Delete File')
            tv.unlink()

        logger.debug('Safe Sleep')
        time.sleep(random.randint(2, 8))

        # return 'Done'

    def run_normal(self):
        self.get_network_url()

        with concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix='Crawl_Thread') as w:
            future = {w.submit(self.download_and_check,
                               self.video_url.format(i),
                               i) for i in range(1, self.max_range)}
            logger.debug(future)

        logger.info('Complete ! Entering Post List Create')

        self._post_retry()

        self._post_run_list()

        self._post_merge_file()

        self._post_update_status()

        self._post_scp()

    def _post_retry(self):
        manual_list = []

        if not self.retry_list:
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix='Retry_thread') as w:
            future = {w.submit(self.download_and_check,
                               self.video_url.format(i),
                               i,
                               True) for i in self.retry_list}
            for fu in concurrent.futures.as_completed(future):
                if fu.result():
                    manual_list.append(fu.result())

        manual_list.sort()

        with open(self.path / 'manual_list.txt', 'w+') as f:
            for _ in manual_list:
                f.write(_)
                f.write('\n')

    def _post_run_list(self):
        logger.info('Writting Download List')
        self.list_file = self.path / 'list.txt'
        file_list = [fn.name for fn in self.path.glob('*.ts')]
        file_list.sort(key=lambda x: int(re.sub(r'\D', '', x)))

        with open(self.list_file, 'w') as f:
            for fname in file_list:
                f.write('file ')
                f.write("'{}'".format(fname))
                f.write('\n')

    def _post_merge_file(self):
        logger.info('Merge Download Video')
        command = [
            'ffmpeg',
            '-y',
            '-f',
            'concat',
            '-safe',
            '0',
            '-i',
            '{}'.format(str(self.list_file)),
            '-bsf:a',  # Fix Re-encoding Video File
            'aac_adtstoasc',
            '-c',
            'copy',
            '{}'.format(str(self.file))
        ]

        with subprocess.Popen(command, universal_newlines=True, stdout=subprocess.PIPE, bufsize=1) as p:
            for line in p.stdout:
                print(line)

    def _post_update_status(self):
        command = [
            'ffprobe',
            '-v',
            'error',
            '-show_entries',
            'format=duration',
            '-of',
            'default=noprint_wrappers=1:nokey=1',
            '{}'.format(str(self.file))
        ]
        logger.info('Getting Concat Video Duration')

        p = subprocess.Popen(command, universal_newlines=True, stdout=subprocess.PIPE, bufsize=1)
        second, _ = p.communicate()
        duration = datetime.timedelta(seconds=float(second))
        timestamp = str(datetime.datetime.now())

        smt = jav.update().where(jav.columns.name == self.name). \
            values({'status': 'Y', 'duration': str(duration), 'finish time': timestamp})
        logger.info('Executing Update')
        with engine.connect() as connection:
            update_rs = connection.execute(smt)

        logger.info('Rows Updated : {}'.format(update_rs.rowcount))

        logger.info('Delete ts File')

        delete_list = self.path.rglob('*.ts')
        for ts in delete_list:
            logger.debug('Delete {}'.format(ts.name))
            ts.unlink()

    def _post_scp(self):
        myip = get_ip()
        command = [
            'rsync',
            '-e',
            'ssh -o StrictHostKeyChecking=no',
            '-savrh',
            '{}'.format(str(self.path.absolute())),
            '{}@{}:{}'.format(main_user, main_host, str(self.path.absolute())),
        ]

        if myip != main_host:
            logger.info('SCP TO Main HOST')
            logger.info('Command : {}'.format(command))
            with subprocess.Popen(command, universal_newlines=True, stdout=subprocess.PIPE, bufsize=1) as p:
                for line in p.stdout:
                    print(line)

    def sp_rerun(self, sp):
        self.get_network_url()

        with concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix='Crawl_Thread') as w:
            future = {w.submit(self.download_and_check,
                               self.video_url.format(i),
                               i) for i in range(sp, self.max_range)}
            logger.debug(future)

        logger.info('Complete ! Entering Post List Create')

        self._post_retry()

        self._post_run_list()

        self._post_merge_file()

        self._post_update_status()

        self._post_scp()

    def run(self, mode, chunk_num=0):
        if mode == 'normal':
            self.run_normal()

        elif mode == 'sp':
            self.run_normal()

        else:
            logger.info('SP_RETRY from chunk_num : {}'.format(chunk_num))
            self.sp_rerun(chunk_num)


def main(mode='normal', sp_name=None, chunk_num=0):
    mode = mode.lower()

    if mode == 'normal':
        statement = jav.select().with_for_update().where(jav.columns.status == 'N'). \
            order_by(jav.columns['update time'].desc()).limit(1)

    elif mode == 'sp':
        logger.info('Running In sp_select Mode')
        if not sp_name:
            raise RuntimeError('In SP mode, Name must be provided !')
        statement = jav.select().with_for_update().where(jav.columns.name.like(sp_name)). \
            order_by(jav.columns['update time'].desc()).limit(1)

    else:
        logger.info('Running In sp_retry Mode')
        # Mode Only in sp_retry
        if not sp_name:
            raise RuntimeError('In SP_retry mode, Name must be provided !')
        if chunk_num == 0:
            raise RuntimeError('In SP_retry mode, Chunk Must Not Be 0 !')
        statement = jav.select().with_for_update().where(jav.columns.name.like(sp_name)). \
            order_by(jav.columns['update time'].desc()).limit(1)

    logger.info('Connecting DB')
    with engine.connect() as con:

        result = con.execute(statement)

        rs = [row for row in result]

        if len(rs) > 0:
            logger.info('Crawl Initializing ')
        else:
            logger.error('No New Video To Crawl')
            sys.exit(0)

        rs = rs[0]
        update_st = jav.update().where(jav.columns.name == rs[1]). \
            values({'status': 'P', 'update time': str(datetime.datetime.now())})
        con.execute(update_st)

    logger.info('Now Crawling Name : {}'.format(rs[1]))
    logger.info('URL : {}'.format(rs[0]))
    size = math.ceil(rs[6] / 4)
    v = VideoCatch(rs[0], rs[1], size)

    v.run(mode=mode, chunk_num=chunk_num)


if __name__ == '__main__':
    main()
