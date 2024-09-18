# 定義所有通知模板（包括title和message），<a>以html格式呈現
_all_notification_template_dict = {
    "update": {
        "stock_report_update": {
          "title": "Stock Report Updated: '{{ticker}}' ",
          "message":'''
                    A new stock report for '{{ticker}}' has been updated.
                    
                    Market Info Function Path: Home > Pool list管理 > {{ticker}} > market info (<i class='bi bi-graph-up button-like'> </i>)
                    
                    or click <a href='{{report_page_url}}' target='_blank'>here </a> to read:
                    
                    ***
                        - You are receiving this notification because you are following this stock ({{ticker}}).
                        - If you wish to modify your following status, please visit the relevant page.
                    ***
                    '''
        },
    }
}