import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import random


def header():
    return {
        "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        "cookie": '_ga=GA1.1.2064334317.1739942185; rd_v1.i18n_redirected=zh; rd_v1.theme=light; rd_v1.uuid=1430729d-51b6-4e78-8b33-84702139ac63; rd_v1.auth._token.local3=xxx; rd_v1.auth._token_expiration.local3=1753837652983; _ga_TXPS04VGH2=GS2.1.s1751331263$o11$g1$t1751331489$j51$l0$h0'
    }

def get_project_data(url):
    proxy = {
        "http": "http://192.168.1.24:7890",
        "https": "http://192.168.1.24:7890"
    }

    html = requests.get(url, proxies=proxy, headers=header(), timeout=60)
    soup = BeautifulSoup(html.text, 'html.parser')

    result = {
        "name": "",
        "official_url": "",
        "doc_url": "",
        "github_url": "",
        "simple_desc": "",
        "detailed_desc": "",
        "tags": "",
        "ecosystem_sj_name": "",
        "ecosystem_sj_id": "",
        "ecosystem_cs_name": "",
        "ecosystem_cs_id": "",
        "ecosystem_jh_name": "",
        "ecosystem_jh_id": "",
        "founding_date": "",
        "country": "",
        "former_name": "",
        "contracts": ""
    }

    try:
        result["name"] = soup.find("h1", class_="name").text.strip()
    except:
        pass

    # 右侧链接按钮区（官网、GitHub、Doc等）
    try:
        link_divs = soup.select("div.links a")
        for link in link_divs:
            href = link.get("href")
            if not href:
                continue
            if "github.com" in href.lower():
                result["github_url"] = href
            elif "docs" in href.lower() or "doc" in href.lower():
                result["doc_url"] = href
            elif "http" in href.lower() and result["official_url"] == "":
                result["official_url"] = href
    except:
        pass

    # 简介
    try:
        result["simple_desc"] = soup.find("p", class_="detail_intro").text.strip()
    except:
        pass

    # 详细介绍
    try:
        result["detailed_desc"] = soup.select_one("p.intd_text").text.strip()
    except:
        pass

    # 标签
    try:
        tag_div = soup.select_one("div.tag_item")
        result["tags"] = "、".join([span.text.strip() for span in tag_div.select("span") if span.text.strip() != "标签:"]).replace('Tags:、','')
    except:
        pass

    # 所在生态
    try:
        scripts = soup.find_all("script")
        for script in scripts:
            text = script.text
            if "sjList" in text or "csList" in text or "jhList" in text:
                sj_match = re.search(r'sjList:\[(.*?)\](?:,|\})', text, re.DOTALL)
                if sj_match:
                    sj_block = sj_match.group(1)
                    result["ecosystem_sj_id"] = ",".join(re.findall(r'id:\s*(\d+)', sj_block))
                    result["ecosystem_sj_name"] = ",".join(re.findall(r'name:\s*"?([^",]+)"?', sj_block))

                cs_match = re.search(r'csList:\[(.*?)\](?:,|\})', text, re.DOTALL)
                if cs_match:
                    cs_block = cs_match.group(1)
                    result["ecosystem_cs_id"] = ",".join(re.findall(r'id:\s*(\d+)', cs_block))
                    result["ecosystem_cs_name"] = ",".join(re.findall(r'name:\s*"?([^",]+)"?', cs_block))

                jh_match = re.search(r'jhList:\[(.*?)\](?:,|\})', text, re.DOTALL)
                if jh_match:
                    jh_block = jh_match.group(1)
                    result["ecosystem_jh_id"] = ",".join(re.findall(r'id:\s*(\d+)', jh_block))
                    result["ecosystem_jh_name"] = ",".join(re.findall(r'name:\s*"?([^",]+)"?', jh_block))
                break
    except:
        pass
    # 基本信息区块
    try:
        base_infos = soup.select("div.side_bar_info span.info_text")
        result["former_name"] = base_infos[0].text.strip() if len(base_infos) >= 1 else ""
        result["founding_date"] = base_infos[1].text.strip() if len(base_infos) >= 2 else ""
        result["country"] = base_infos[2].text.strip() if len(base_infos) >= 3 else ""
    except:
        pass
    return result


if __name__ == "__main__":
    output_file = "project_data_all.csv"
    first_write = not os.path.exists(output_file)

    with open("url_list.txt", encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]

    for idx, url in enumerate(urls, 1):
        try:
            print(f"抓取第 {idx} 条：{url}")
            data = get_project_data(url)
            df = pd.DataFrame([data])
            df.to_csv(output_file, mode='a', index=False, encoding="utf-8-sig", header=first_write)
            first_write = False  # 之后不再写入 header
        except Exception as e:
            print(f"❌ 抓取失败: {url}, 错误: {e}")
        finally:
            time.sleep(random.uniform(5, 10))