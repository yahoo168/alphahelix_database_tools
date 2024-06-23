import requests
from pprint import pprint

# 教學：https://tonisives.com/blog/2021/11/30/add-text-and-images-to-notion-via-the-official-api-and-python/
class Notion:
    def __init__(self, notion_token: str):
        self.headers = {
            'Notion-Version': '2022-06-28',
            'Authorization': 'Bearer ' + notion_token
        }
        self.base_url = "https://api.notion.com/v1"

    def _upload_blocks(self, parent_id: str, element_list: []):
        url = self.base_url + f"/blocks/{parent_id}/children"
        res = requests.request("PATCH", url, headers=self.headers, json={"children": element_list})
        
        if res.json()["object"] == "error":
            print(res.text)
        else:
            print("sucessfully uploaded")
    
    def _get_block_title_element(self, title_text: str, title_type: str):
        #檢查heading格式
        assert(title_type in ["heading_1", "heading_2", "heading_3"])
        title_block = {
                "type": title_type,
                 title_type: {
                    "rich_text": [{
                        "type": "text",
                        "text": {
                            "content": title_text,
                        }
                    }]
                }
        }
        return title_block
        
    def _get_bullet_block_text_element(self, text: str):
        text_block = {
          "object": "block",
          "type": "bulleted_list_item",
          "bulleted_list_item": {
            "rich_text": [
              {
                "type": "text",
                "text": {
                  "content": text
                }
              }
            ]
          }
        }
        return text_block
                          
    def append_paragraph(self, parent_id, body_text_list: [], title_text=None, title_type=None):
        element_list = list()
        
        if title_text != None:
            element = self._get_block_title_element(title_text=title_text, title_type=title_type)
            element_list.append(element)
            
        for body_text in body_text_list:
            element = self._get_bullet_block_text_element(text=body_text)
            element_list.append(element)
            
        self._upload_blocks(parent_id=parent_id, element_list=element_list)
    
    def create_page(self, parent_id: str, title: str):
        url = 'https://api.notion.com/v1/pages'

        json = {
            "parent": {"page_id": parent_id},
            "properties":{
                    "title": [
                        {
                            "text": {
                                "content": title
                            }
                        }
                    ]
            }
        }
        res = requests.request("POST", url=url, headers=self.headers, json=json)
        new_page_id = res.json()["id"]
        return new_page_id
    
    #Notion API 不支持直接從本地系統上傳檔案。因此，你需要先將圖片上傳到一個線上文件儲存服務，例如 AWS S3、Google Cloud Storage 或 Dropbox 等。
    def image_add(self, parent_id: str, image_url: str):
        element_list = [
            {"type": "image",
            "image": {
              "type": "external",
              "external": {"url": image_url}
              }
            }
          ]
        return self._upload_blocks(parent_id, element_list)