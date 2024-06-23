import requests
from datetime import datetime, timezone, timedelta
import re
from bson.objectid import ObjectId

class ReadwiseTool():
    def __init__(self, MDB_client, token=None):
        self.client = MDB_client
        self.collection = MDB_client["users"]["readwise_notes"]
        self.token = token
    
    # 將日期字串轉換為ISO 8601格式
    @staticmethod
    def str2isoformat(date_str):
        # Convert to datetime object(# date_str格式'2024-01-01')
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')    
        # Convert to ISO 8601 format
        iso_date_str = date_obj.isoformat()
        return iso_date_str
        
    def fetch_data_from_readwsie_api(self, token, updated_after=None):
        article_meta_list = []
        next_page_cursor = None
        while True:
            params = {}
            if next_page_cursor:
                params['pageCursor'] = next_page_cursor
            if updated_after:
                params['updatedAfter'] = updated_after
            
            response = requests.get(
                url="https://readwise.io/api/v2/export/",
                params=params,
                headers={"Authorization": f"Token {token}"}, verify=False
            )
            article_meta_list.extend(response.json()['results'])
            next_page_cursor = response.json().get('nextPageCursor')
            if not next_page_cursor:
                break
        return article_meta_list

    def _clean_highlight_text(self, text):
        # 提取url (list)
        urls_list = re.findall(r'https://[^\s\(\)]+', text)
        # 去除[]符號和url，並替換•為空格，去除多餘空格
        text = re.sub(r'\[\]|\(https://[^\s\(\)]+\)|•', '', text)
        # 去除字串中間多餘的空格
        text = re.sub(r'\s+', ' ', text)
        return text.strip(), urls_list

    def _clean_article_meta(self, article_meta_list):
        clean_article_meta_list = []
        for article_meta in article_meta_list:
            book_tag_list = [tag["name"].lower() for tag in article_meta["book_tags"]]
        
            clean_highlight_meta_list = []
            for highlight_meta in article_meta["highlights"]:
                # 解析出highlight_tag，並合併去除重複的tag
                highlight_tag_list = list(set(tag["name"].lower() for tag in highlight_meta["tags"]).union(book_tag_list))
                text, urls_list = self._clean_highlight_text(highlight_meta["text"])
        
                clean_highlight_meta = {
                    "highlight_text": text,
                    "urls": urls_list,
                    "tags": highlight_tag_list,
                }
                clean_highlight_meta_list.append(clean_highlight_meta)

            date_str = article_meta["highlights"][0]["created_at"]
            date_datetime = datetime.fromisoformat(date_str)
            # 轉換為台北時間（UTC+8）後，移除附帶的時區資訊
            date_datetime = date_datetime.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
            
            clean_article_meta = {
                "date": date_datetime,
                "article_title": article_meta["readable_title"],
                "article_summary": article_meta["summary"],
                "article_url": article_meta["source_url"],
                "highlights": clean_highlight_meta_list,
            }
            clean_article_meta_list.append(clean_article_meta)
        return clean_article_meta_list

    def get_lastest_article_date(self, user_id):
        article_meta = self.collection.find_one({"uploader": ObjectId(user_id)}, sort=[("date", -1)], limit=1)
        return article_meta["date"]
        
    # 從最新一筆的日期開始抓取到現在，# 依照指定的日期區間重新上傳
    def upload_articles_to_MDB(self, user_id, days=None):
        if days == None:
            start_date = self.get_lastest_article_date(user_id)
        else:
            start_date = datetime.now() - timedelta(days=days)

        print(f"[SAVE][Readwise] fetch from {start_date}")
        raw_article_meta_list = self.fetch_data_from_readwsie_api(self.token, start_date.isoformat())
        # 若近期沒有新的筆記則跳過
        if raw_article_meta_list:
            clean_article_meta_list = self._clean_article_meta(raw_article_meta_list)
            user_id = ObjectId(user_id)
            # 對每個數據項執行 update_one
            for article_meta in clean_article_meta_list:
                article_meta["uploader"] = user_id
                self.collection.update_one(
                        {"article_title": article_meta["article_title"]},  # 用article_title作為條件來查找文檔
                        {"$set": article_meta,  # 如果文檔已存在，更新所有字段
                    },
                        upsert=True  # 開啟 upsert 選項
                    )

    def get_article_meta_list(self, days=7, user_id=None):
        start_date = datetime.now() - timedelta(days=days)
        if user_id:
            query = {"date": {"$gte": start_date}, "uploader": ObjectId(user_id)}
        else:
            query = {"date": {"$gte": start_date}}
            
        article_meta_list = list(self.collection.find(query))
        return article_meta_list

    def get_recent_tag_list(self, article_meta_list):
        recent_tag_list = list()
        for article_meta in article_meta_list:
            for hightlight_meta in article_meta["highlights"]:
                recent_tag_list.extend(hightlight_meta["tags"])
        return list(set(recent_tag_list))

    def search_highlights_by_tags(self, article_meta_list, tag_list):
        # 将所有搜索标签转换为小写
        search_tags_set = set(tag.lower() for tag in tag_list)
        selected_article_meta_list = []
        for article_meta in article_meta_list:
            selected_highlight_meta_list = []
            for highlight_meta in article_meta["highlights"]:
                # 将highlight的标签转换为小写集合
                highlight_tags_set = set(tag.lower() for tag in highlight_meta["tags"])
                # 检查是否有交集
                if not search_tags_set.isdisjoint(highlight_tags_set):
                    selected_highlight_meta_list.append(highlight_meta)
            
            if selected_highlight_meta_list:
                # 将符合tag筛选的highlights覆盖原本的
                new_article_meta = article_meta.copy()
                new_article_meta["highlights"] = selected_highlight_meta_list
                selected_article_meta_list.append(new_article_meta)
        
        return selected_article_meta_list