from selenium import webdriver
import re
from selenium.webdriver.common.by import By
import asyncio, time
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

GAP = 2  # 2 sec
pattern = re.compile(r'\b\d{7,}\b')


def get_data():
    get_address = lambda x: x['도로명주소'] if pd.notna(x['도로명주소']) else x['지번주소']

    data = pd.read_csv('../data/서울시 일반음식점 인허가 정보.csv',
                       usecols=['도로명주소', '지번주소', '사업장명', '영업상태코드'],
                       encoding='cp949')

    # preprocessing
    data = data[data['영업상태코드'] == 1]
    data = data.dropna(subset=['사업장명'])
    data['주소'] = data.apply(get_address, axis=1)
    data = data.sample(frac=1)
    return data


class Crawling:
    xpaths = {
        'div': '//*[@id="app-root"]/div/div/div/div[2]/div[1]',
        'category': '//*[@id="_title"]/div/span[2]',
        'review_div': '//*[@id="app-root"]/div/div/div/div[2]/div[1]/div[2]',
        'visitor_review': '//*[@id="app-root"]/div/div/div/div[2]/div[1]/div[2]/span[2]/a',
        'blog_review': '//*[@id="app-root"]/div/div/div/div[2]/div[1]/div[2]/span[3]/a',
        'visitor_review_no_star': '//*[@id="app-root"]/div/div/div/div[2]/div[1]/div[2]/span[1]/a',
        'blog_review_no_star': '//*[@id="app-root"]/div/div/div/div[2]/div[1]/div[2]/span[2]/a',
        'title': '//*[@id="_title"]/div/span[1]',
        'search_result_ul': '//*[@id="_pcmap_list_scroll_container"]/ul',
        'searchIframe': '//*[@id="searchIframe"]',
        'entryIframe': '//*[@id="entryIframe"]'
    }
    class_name = {
        'visitor_review': 'PXMot',
        'blog_review': 'PXMot'
    }

    def __init__(self, data, random_num):
        self.data = data
        self.random_num = random_num
        options = Options()
        # options.add_argument("--headless=new")
        # options.add_argument("--window-size=1280,700")
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, GAP)

    def search(self, addr, place):
        self.driver.get(f'https://map.naver.com/p/search/{place}?c=15.00,0,0,0,dh')

    def get_element_by_xpath(self, xpath):
        # span = self.driver.find_element(By.XPATH, xpath)
        # self.driver.implicitly_wait(GAP * 5)
        span = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
        return span

    async def get_info(self, place):
        code = pattern.findall(self.driver.current_url)
        code = code[0] if len(code) > 0 else 'failed'

        self.driver.switch_to.frame(self.get_element_by_xpath(self.xpaths['entryIframe']))

        review_div = self.get_element_by_xpath(self.xpaths['review_div'])

        visitor_key = 'visitor_review_no_star'
        blog_key = 'blog_review_no_star'
        if len(review_div.find_elements(By.TAG_NAME, 'span')) >= 3:
            visitor_key = 'visitor_review'
            blog_key = 'blog_review'

        title, category, visitor, blog = place, '없음', 0, 0
        try:
            span = self.get_element_by_xpath(self.xpaths['title'])
            title = span.text
        except Exception as e:
            print('failed to get title')
            print(e)

        try:
            span = self.get_element_by_xpath(self.xpaths['category'])
            category = span.text
        except Exception as e:
            print('failed to get category')
            print(e)

        try:
            span = self.get_element_by_xpath(self.xpaths[visitor_key])
            visitor = int(span.text.split()[1].replace(',', ''))
        except Exception as e:
            print('failed to get visitor_review')
            print(e)
        try:
            span = self.get_element_by_xpath(self.xpaths[blog_key])
            blog = int(span.text.split()[1].replace(',', ''))
        except Exception as e:
            print('failed to get blog_review')
            print(e)

        return [title, code, category, visitor, blog]

    def select_first_child(self):
        try:
            print('select first child')
            self.driver.switch_to.frame('searchIframe')

            search_result_ul = self.get_element_by_xpath(self.xpaths['search_result_ul'])
            try:
                first_child_element = search_result_ul.find_element(By.CLASS_NAME, 'YwYLL')
                first_child_element.click()
            except Exception as e:
                print('YmYLL 실패')
                first_child_element = search_result_ul.find_element(By.CLASS_NAME, 'TYaxT')
                first_child_element.click()
            finally:
                self.driver.switch_to.default_content()
        except Exception as e:
            print('failed to click')
            print(e)

    async def run(self):
        places = self.data['사업장명']
        address = self.data['주소']

        result_data = []
        total = len(self.data)
        cnt = 0
        for addr, p in zip(address, places):
            # addr = addr.split()[1]  ## 00구
            cnt += 1
            print(f'[{cnt}/{total}]{addr} {p}')
            try:
                self.search(addr, p)
                await asyncio.sleep(GAP)
                try:
                    self.driver.find_element(By.CLASS_NAME, 'FYvSc')
                    print('조건에 맞는 업체가 없음.')
                    continue
                except Exception as e:
                    pass

                if 'isCorrectAnswer=true' not in self.driver.current_url:
                    self.select_first_child()
                    await asyncio.sleep(GAP)

                result = await self.get_info(p)

                print(result)
                result_data.append(result)
            except Exception as e:
                print(e)
                print(f'{p} failed')

            if cnt % 100:
                df = pd.DataFrame(result_data, columns=['사업장명', '네이버등록코드', '카테고리', '방문자리뷰', '블로그리뷰'])
                df.to_csv(f'../data/{self.random_num}.csv', index=False, encoding='utf-8')

        print(df)
        df.to_csv(f'../data/{self.random_num}.csv', index=False, encoding='utf-8')
        self.driver.quit()
