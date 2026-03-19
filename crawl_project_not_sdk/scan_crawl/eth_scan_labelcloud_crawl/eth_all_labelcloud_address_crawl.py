import requests
import json
import pandas as pd
import csv
import os
import time
import random
from bs4 import BeautifulSoup
import traceback

# 运行时cookie需要更新
headers = {
    'content-type':'application/json',
    'cookie':'ASP.NET_SessionId=4yg2cwy2b1gkut3xzdvllihj; etherscan_offset_datetime=+8; __stripe_mid=838baa5e-f283-4b93-9856-06ac340e1a69048572; etherscan_pwd=4792:Qdxb:X6c+LukkTt9nCKKvtVK0zdMZRlDKWi+lNJp08O7TTaU=; etherscan_userid=skiny1; etherscan_autologin=True; etherscan_switch_token_amount_value=value; __cflb=02DiuFnsSsHWYH8WqVXaqSXf986r8yFDrp6soMT6wYNYt; _gid=GA1.2.360471044.1773891129; cf_clearance=6oQeK07bx6A1_wKHxTnQ_Sdo.ITa44Fhy.DtdlHQlfY-1773898110-1.2.1.1-CGiuWJMEmdf2sa8yVm8ykqH012uQ2m1C0LxSEqnqAF1m7ak2HRcPCf3lo33M3Hbz5BQ8dCZAndCNCv6LYsN1yqOa96K059LTlLY.x487htzxiZgrH676sF3aAr2wk8WpHs6ip3JM54arhsiy3lhW2eoS0bfBfnCkHBrKfP.9BgrZ5Cze08DcEQEo7smWvQz9XDhy_ZeZzdn6qtxrwMTgUTz5wS7YiDxwj34doK7TPYSe0Pp8C0MUJuxuQldt5Hda; _ga=GA1.2.1352231755.1773724205; _ga_T1JC9RNQXV=GS2.1.s1773898099$o6$g1$t1773899039$j60$l0$h0',
    'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
}

proxy = {
    "http": "http://192.168.1.24:7988",
    "https": "http://192.168.1.24:7988"
}

base_url = "https://etherscan.io"

LABELCLOUD_URL = "https://etherscan.io/labelcloud"
ACCOUNTS_API_URL = "https://etherscan.io/accounts.aspx/GetTableEntriesBySubLabel"

LABEL_FILE = "addresses.txt"
LABELCLOUD_FILE = "etherscan_labelcloud.csv"
OUTPUT_FILE = "etherscan_label_accounts.csv"

length_per_page = 100
max_retries = 3

print("=" * 50)
print("步骤1: 获取label列表")
print("=" * 50)

resp = requests.get(LABELCLOUD_URL, headers=headers, proxies=proxy, timeout=10)

soup = BeautifulSoup(resp.text, "html.parser")

div_list = soup.find_all("div", class_="row mb-3")

labels = []

with open(LABELCLOUD_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["label", "url"])

    for div in div_list:

        a_tags = div.find_all(
            "a",
            class_="dropdown-item d-flex align-items-center gap-2"
        )

        for a in a_tags:

            href = a.get("href", "")

            if href.startswith("/accounts/label/"):

                full_url = base_url + href
                label = href.split("/")[-1]

                labels.append(label)

                writer.writerow([label, full_url])

                print(f"获取label: {label}")

print(f"\n共获取 {len(labels)} 个label")

print("\n" + "=" * 50)
print("步骤2: 抓取每个label下的账户信息")
print("=" * 50)

write_header = not os.path.exists(OUTPUT_FILE)

for label in labels:

    print(f"\n开始抓取 label: {label}")

    page = 0

    while True:

        start_value = page * length_per_page

        payload = {
            "dataTableModel": {
                "draw": 2,
                "columns": [
                    {"data": "address"},
                    {"data": "nameTag"},
                    {"data": "balance"},
                    {"data": "txnCount"}
                ],
                "order": [{"column": 1, "dir": "asc"}],
                "start": start_value,
                "length": length_per_page,
                "search": {"value": "", "regex": "false"}
            },
            "labelModel": {
                "label": label
            }
        }

        success = False
        data_list = []

        for attempt in range(max_retries):

            try:

                res = requests.post(
                    ACCOUNTS_API_URL,
                    data=json.dumps(payload),
                    headers=headers,
                    proxies=proxy,
                    timeout=30
                )

                if res.status_code != 200:
                    print(f"HTTP异常 {res.status_code}")
                    time.sleep(3)
                    continue

                result = res.json()

                data_list = result.get("d", {}).get("data", [])

                success = True
                break

            except Exception as e:

                print(f"请求异常 {e}")
                time.sleep(3)

        if not success:
            print("请求失败，跳过当前页")
            page += 1
            continue

        if not data_list:
            print(f"{label} 已无数据")
            break

        rows = []

        for item in data_list:

            address_html = item.get("address", "")

            soup = BeautifulSoup(address_html, "html.parser")

            tag = soup.find(attrs={"data-bs-title": True})

            address = tag["data-bs-title"] if tag else ""

            nameTag = item.get("nameTag", "")

            rows.append([
                label,
                address,
                nameTag
            ])

        if rows:

            df = pd.DataFrame(
                rows,
                columns=[
                    "label",
                    "address",
                    "nameTag"
                ]
            )

            df.to_csv(
                OUTPUT_FILE,
                mode="a",
                index=False,
                header=write_header,
                encoding="utf-8-sig"
            )

            write_header = False

            print(f"{label} 第 {page} 页写入 {len(rows)} 条")

        if len(data_list) < length_per_page:

            print(f"{label} 已到最后一页")
            break

        sleep_time = random.uniform(1.5, 3)

        print(f"休眠 {sleep_time:.1f}s")

        time.sleep(sleep_time)

        page += 1


print("\n" + "=" * 50)
print("全部抓取完成")
print("=" * 50)
