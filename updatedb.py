import sys
from PyQt5.QtWidgets import *
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pandas import Series, DataFrame
import sqlite3
import Kiwoom

con = sqlite3.connect(
    "C:\Program Files\DB Browser for SQLite\created dbs\sumfinst.db")


class updateDb:
    def __init__(self):
        self.kiwoom = Kiwoom.Kiwoom()
        self.kiwoom.comm_connect()

    def get_code_name(self):
        code_list = self.kiwoom.get_code_list_by_market(
            '0') + self.kiwoom.get_code_list_by_market('10')
        for n in code_list:
            codes_name = []
            codes_names = self.kiwoom.get_master_code_name(n)
            codes_name.append(codes_names)
        return codes_name

    def run(self):
        code_list = self.kiwoom.get_code_list_by_market(
            '0') + self.kiwoom.get_code_list_by_market('10')
        for n in code_list:
            codes_name = self.kiwoom.get_master_code_name(n)
            # code = "035720"
            url = f"https://finance.naver.com/item/main.nhn?code={n}"

            headers = {
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
            result = requests.get(url, headers=headers)
            soup = BeautifulSoup(result.text, "html.parser")
            div = soup.find("div", {"class": "section cop_analysis"})
            try:
                thead = div.find("thead").find_all("tr")[1]
                date = thead.find_all("th")
            except:
                continue

            d_1 = date[0].text.strip()
            d_1 = d_1.replace(".12", "(연간)")
            d_2 = date[1].text.strip()
            d_2 = d_2.replace(".12", "(연간)")
            d_3 = date[2].text.strip()
            d_3 = d_3.replace(".12", "(연간)")
            d_4 = date[3].text.strip()
            d_4 = d_4.replace(".12", "(연간)")
            d_5 = date[4].text.strip() + " (분기1)"
            d_6 = date[5].text.strip() + " (분기2)"
            d_7 = date[6].text.strip() + " (분기3)"
            d_8 = date[7].text.strip() + " (분기4)"
            d_9 = date[8].text.strip() + " (분기5)"
            d_10 = date[9].text.strip() + " (분기예상)"
            try:
                rows = div.find("tbody").find_all("tr")
            except AttributeError:
                continue

            data_list_1 = []
            data_list_2 = []
            data_list_3 = []
            data_list_4 = []
            data_list_5 = []
            data_list_6 = []
            data_list_7 = []
            data_list_8 = []
            data_list_9 = []
            data_list_10 = []
            name_list = []
            data_dic = {d_1: [], d_2: [], d_3: [], d_4: [], d_5: [
            ], d_6: [], d_7: [], d_8: [], d_9: [], d_10: []}
            for row in rows:
                try:
                    name = row.find_all("th")
                    data = row.find_all("td")
                    term = name[0].text
                    r_1 = data[0].text
                    r_1 = r_1.replace("\t", "")
                    r_1 = r_1.replace("\n", "")
                    r_2 = data[1].text
                    r_2 = r_2.replace("\t", "")
                    r_2 = r_2.replace("\n", "")
                    r_3 = data[2].text
                    r_3 = r_3.replace("\t", "")
                    r_3 = r_3.replace("\n", "")
                    r_4 = data[3].text
                    r_4 = r_4.replace("\t", "")
                    r_4 = r_4.replace("\n", "")
                    r_5 = data[4].text
                    r_5 = r_5.replace("\t", "")
                    r_5 = r_5.replace("\n", "")
                    r_6 = data[5].text
                    r_6 = r_6.replace("\t", "")
                    r_6 = r_6.replace("\n", "")
                    r_7 = data[6].text
                    r_7 = r_7.replace("\t", "")
                    r_7 = r_7.replace("\n", "")
                    r_8 = data[7].text
                    r_8 = r_8.replace("\t", "")
                    r_8 = r_8.replace("\n", "")
                    r_9 = data[8].text
                    r_9 = r_9.replace("\t", "")
                    r_9 = r_9.replace("\n", "")
                    r_10 = data[9].text
                    r_10 = r_10.replace("\t", "")
                    r_10 = r_10.replace("\n", "")
                    data_list_1.append(r_1)
                    data_list_2.append(r_2)
                    data_list_3.append(r_3)
                    data_list_4.append(r_4)
                    data_list_5.append(r_5)
                    data_list_6.append(r_6)
                    data_list_7.append(r_7)
                    data_list_8.append(r_8)
                    data_list_9.append(r_9)
                    data_list_10.append(r_10)
                    name_list.append(term)
                except IndexError:
                    continue

            data_dic[d_1] = data_list_1
            data_dic[d_2] = data_list_2
            data_dic[d_3] = data_list_3
            data_dic[d_4] = data_list_4
            data_dic[d_5] = data_list_5
            data_dic[d_6] = data_list_6
            data_dic[d_7] = data_list_7
            data_dic[d_8] = data_list_8
            data_dic[d_9] = data_list_9
            data_dic[d_10] = data_list_10

            columns = [d_1, d_2, d_3, d_4, d_5, d_6, d_7, d_8, d_9, d_10]

            df = pd.DataFrame(data_dic, index=name_list, columns=columns)
            try:
                df.to_sql(codes_name, con, if_exists='replace')
            except ValueError:
                continue

    def read_db(self, num):
        names = get_code_name()
        datum = []
        for name in names:
            read_datum = pd.read_sql(f"SELECT * FROM '{name}'", con)
            read_data = read_datum.loc[num]
            datum.append(read_data)
        return datum
