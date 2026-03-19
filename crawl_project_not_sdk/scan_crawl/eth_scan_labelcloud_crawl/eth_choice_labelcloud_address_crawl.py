import requests
import json
import pandas as pd
import os
import time
import random
from bs4 import BeautifulSoup
import traceback

# cookie必须更新，不然一页只能拿到7/17条数据
headers = {
    'content-type':'application/json',
    'cookie':'etherscan_offset_datetime=+8; etherscan_xchain_tx_view=source; etherscan_xchain_addr_view=recipient; cards-currentTab=all; _ga_T1JC9RNQXV=deleted; __stripe_mid=e2a8d468-eb4d-4250-9827-103d907b0d8b241545; etherscan_cookieconsent=True; etherscan_address_format=0; etherscan_datetime_format=UTC; etherscan_isHighlight=true; etherscan_settings=x0:0|x1:0|x2:en|x3:USD|x4:0|x5:0|x6:|x7:UTC|x8:1|x9:1|x10:1|x11:0; CultureInfo=en; etherscan_switch_age_datetime=Date Time (UTC); etherscan_switch_token_amount_value=amount; _ga_XPR6BMZXSN=GS2.1.s1771899586$o19$g0$t1771899612$j34$l0$h0; ASP.NET_SessionId=wi2fsegicazeyn4maxjey0yg; _gid=GA1.2.785491707.1773623536; etherscan_pwd=4792:Qdxb:X6c+LukkTt9nCKKvtVK0zdMZRlDKWi+lNJp08O7TTaU=; etherscan_userid=skiny1; etherscan_autologin=True; __cflb=02DiuFnsSsHWYH8WqVXaqSXf986r8yFDrhReyywp2bdWc; _ga=GA1.2.1130231972.1762484986; _gat_gtag_UA_46998878_6=1; cf_clearance=Ue5HekPP1aX1GbNmcs1qnrj8vkyr6Ncen3jhOHHU6zs-1773711232-1.2.1.1-y8N1C7qFcDW_nCwkkTTVaUTSQy0HfbrJhilcwC9qSqhMb28mZPRNOHAfCdU4azeao5udxmeok_5YsbPd6FpOjTm2eJyKaBuL3cof4r4Ll77uCCfQ2lXiG21FAH9KoNBjcPoeI_UwI_wSJAuCU_DRlfE3EH4JSpBO20fEEvUiRRoXRWw8JyAjUsIMvoxut7hJQJ_sh4v22_3eK8uW91G8jLQwLg6AYwviWCiD0VX7ulmVabP4rF4ntEkGZmWMt4ss; _ga_T1JC9RNQXV=GS2.1.s1773709487$o161$g1$t1773711240$j46$l0$h0',
    'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
}

proxy = {
    "http": "http://192.168.1.24:7988",
    "https": "http://192.168.1.24:7988"
}

url = "https://etherscan.io/accounts.aspx/GetTableEntriesBySubLabel"

LABEL_FILE = "labelcloud.txt" # 需要获取的词云
OUTPUT_FILE = "etherscan_label_accounts.csv"

length_per_page = 100
max_retries = 3

write_header = not os.path.exists(OUTPUT_FILE)

# 读取 label
with open(LABEL_FILE, "r", encoding="utf-8") as f:
    labels = [x.strip() for x in f if x.strip()]

print(f"共 {len(labels)} 个 label")

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
                    url,
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

        # ⭐ 如果小于100条 → 结束当前label
        if len(data_list) < length_per_page:

            print(f"{label} 已到最后一页")
            break

        sleep_time = random.uniform(1.5, 3)

        print(f"休眠 {sleep_time:.1f}s")

        time.sleep(sleep_time)

        page += 1


print("\n全部抓取完成")