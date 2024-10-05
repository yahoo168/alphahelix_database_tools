# 定義所有通知模板（包括title和message），<a>以html格式呈現
_all_notification_template_dict = {
    "update": {
        "stock_report_update": {
          "title": "【個股報告】【{{ticker}}】{{title}}",
          "message":'''
                    您追蹤的個股({{ticker}})，有新報告<a href='{{page_url}}' target='_blank'>{{title}}</a> 更新
                    
                    請點擊連結查看，或使用以下路徑進入報告頁面:
                    
                    Home > 研究管理 > {{ticker}} > market info (<i class='bi bi-graph-up button-like'> </i>)
                    '''
        },
        
        "investment_issue_review_update": {
          "title": "【投資議題】{{issue}}",
          "message":'''
                    您追蹤的投資議題 「<a href='{{page_url}}' target='_blank'>{{issue}} </a> 」已更新完成。
                    
                    請點擊連結查看！
                    '''
        },
        
        "stock_news_summary_update": {
            "title": "【每日新聞】{{date}} ",
            "message":'''
                    「個股新聞總結」-{{date}}，更新已完成，點擊<a href='{{page_url}}' target='_blank'>查看</a>。
                    '''
        }
    }
}