from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
import pandas as pd
from urllib.parse import urljoin
import random
from collections import OrderedDict
import os
import re


class EthereumCrawler:
    def __init__(self, chain_url_dict,proxy=None):
        self.proxy = proxy
        self.driver = None
        self.base_url = "https://cn.rootdata.com"
        self.login_url = f"{self.base_url}/login"
        # 定义多个目标链接及其对应的文件名
        self.target_urls = chain_url_dict
        self.output_dir = "D:/PythonProject/test/project11"  # 输出目录

    def _init_browser(self):
        options = Options()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1200,800")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        if self.proxy:
            options.add_argument(f"--proxy-server={self.proxy}")
            print(f"✅ 使用代理: {self.proxy}")

        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(60)
            return driver
        except Exception as e:
            print(f"❌ 浏览器初始化失败: {e}")
            raise

    def _wait_for_login(self):
        print("\n" + "=" * 50)
        print(" 请手动完成登录 ")
        print("=" * 50)
        print(f"浏览器已打开: {self.login_url}")
        print("完成后请点击页面右上角头像以确认已登录")
        print("=" * 50 + "\n")

        try:
            self.driver.get(self.login_url)
            login_success = False
            start_time = time.time()

            while time.time() - start_time < 400:  # 最多等5分钟
                try:
                    current_url = self.driver.current_url
                    if "login" not in current_url:  # 检查是否已跳转登录后页面
                        user_menu = self.driver.find_elements(By.XPATH,
                                                              '//*[contains(text(),"我的账户") or contains(text(),"退出登录")]')
                        if user_menu:
                            print("✅ 确认检测到登录成功元素")
                            login_success = True
                            break

                    print("⏳ 等待用户完成登录...")
                    time.sleep(5)

                except WebDriverException as e:
                    print(f"⚠️ 登录检测异常: {e}")
                    time.sleep(5)
                    continue

            if login_success:
                print("✅ 登录确认成功，准备开始爬取")
                return True
            else:
                print("❌ 登录超时未检测到成功，保存截图 login_failed.png")
                self.driver.save_screenshot("login_failed.png")
                return False

        except Exception as e:
            print(f"❌ 登录过程发生异常: {e}")
            self.driver.save_screenshot("login_error.png")
            return False

    def _crawl_single_project(self, target_name, target_url):
        print(f"\n开始爬取: {target_name}")

        # 确保目录存在
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # 为每个项目创建单独的文件
        filename = os.path.join(self.output_dir, f"{target_name}_projects.csv")
        project_urls = OrderedDict()  # 使用OrderedDict保持顺序

        try:
            # 跳转到目标页面
            self.driver.get(target_url)

            # 确保页面加载完成
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.XPATH, f'//h1[contains(text(),"{target_name}")]'))
            )

            page = 1
            while True:
                print(f"\n=== 第 {page} 页 ===")
                time.sleep(random.uniform(5, 10))

                # 获取当前页所有项目链接
                try:
                    elements = WebDriverWait(self.driver, 30).until(
                        EC.presence_of_all_elements_located((By.XPATH, '//a[contains(@href, "/Projects/detail/")]'))
                    )
                    new_urls = 0
                    for el in elements:
                        url = urljoin(self.base_url, el.get_attribute("href"))
                        if url not in project_urls:
                            project_urls[url] = None
                            new_urls += 1
                    print(f"发现 {len(elements)} 个链接，其中 {new_urls} 个新链接，总计 {len(project_urls)}")

                    # 边爬边写入
                    if new_urls > 0:
                        df = pd.DataFrame(list(project_urls.keys()), columns=["project_url"])
                        df.to_csv(filename, index=False, encoding="utf-8-sig")
                        print(f"✅ 已更新 {new_urls} 条数据到 {filename}")

                except Exception as e:
                    print(f"❌ 提取失败: {e}")
                    self.driver.save_screenshot(f'extract_error_{target_name}.png')
                    break

                # 尝试翻页
                try:
                    # 改进的下一页按钮定位
                    next_btn = WebDriverWait(self.driver, 20).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, '//button[contains(@class, "btn-next")]'))
                    )

                    # 检查是否已经是最后一页
                    if "disabled" in next_btn.get_attribute("class") or "ant-disabled" in next_btn.get_attribute(
                            "class"):
                        print("⏹ 已到最后一页")
                        break

                    # 滚动到元素并点击
                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                                               next_btn)
                    time.sleep(random.uniform(3, 9))

                    # 使用JavaScript点击避免被拦截
                    self.driver.execute_script("arguments[0].click();", next_btn)
                    print("✅ 已点击下一页")

                    # 等待新页面加载完成
                    WebDriverWait(self.driver, 50).until(
                        EC.presence_of_element_located((By.XPATH, '//a[contains(@href, "/Projects/detail/")]'))
                    )
                    page += 1

                except TimeoutException:
                    print("⏹ 翻页超时，可能已到最后一页")
                    break
                except Exception as e:
                    print(f"❌ 翻页失败: {e}")
                    self.driver.save_screenshot(f'page_turn_error_{target_name}.png')
                    break

            print(f"\n✅ {target_name} 爬取完成，共 {len(project_urls)} 个项目")
            return True

        except Exception as e:
            print(f"\n❌ {target_name} 爬取异常: {e}")
            self.driver.save_screenshot(f'crawl_error_{target_name}.png')
            return False

    def run(self):
        try:
            self.driver = self._init_browser()
            if not self._wait_for_login():
                return False

            # 依次爬取每个目标
            for target_name, target_url in self.target_urls.items():
                if not self._crawl_single_project(target_name, target_url):
                    print(f"⚠️ {target_name} 爬取失败，继续下一个目标...")
                    continue

            return True
        except Exception as e:
            print(f"\n❌ 程序运行异常: {e}")
            return False
        finally:
            if hasattr(self, 'driver') and self.driver:
                self.driver.quit()
                print("\n🛑 浏览器已关闭")


if __name__ == "__main__":
    PROXY = "http://192.168.1.24:7890"
    print("\n" + "=" * 50)
    print(" RootData 多项目爬取程序 v2.0 ")
    print("=" * 50)

    # 读取 Excel 文件到 DataFrame
    df = pd.read_excel('rootdata.xlsx')
    # 定义一个空字典
    chain_url_dict = {}
    # 使用 iterrows 遍历 DataFrame
    for index, row in df.iterrows():
        url = row["url"]
        url_suffix = url.split('/')[-1]

        # 提取等号后的部分，并将 %20 替换为空格
        chain = re.sub(r'%20', ' ', url_suffix.split('=')[1])
        chain_url_dict[chain] = url
    print(chain_url_dict)
    crawler = EthereumCrawler(chain_url_dict=chain_url_dict,proxy=PROXY)
    if crawler.run():
        print("\n🎉 所有项目执行成功！结果已保存")
    else:
        print("\n❌ 执行过程中有失败，请检查截图和日志")