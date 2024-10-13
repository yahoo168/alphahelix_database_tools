# 定義所有通知模板（包括title和message），<a>以html格式呈現
# 換行須以<br>呈現，不可使用\n（會在後端被轉換為空白，以避免顯示錯誤）
_all_notification_template_dict = {
    "update": {
        "stock_report_update": {
          "title": "【個股報告】【{{ticker}}】{{title}}",
          "message":'''
                    您追蹤的個股({{ticker}})，有新報告「<a href='{{page_url}}' target='_blank'>{{title}}</a>」更新
                    <br><br>
                    請點擊查看，或使用以下路徑進入報告頁面:
                    <br><br>
                    Home > 研究管理 > {{ticker}} > market info (<i class='bi bi-graph-up button-like'> </i>)
                    
                    '''
        },
        
        "investment_issue_review_update": {
          "title": "【投資議題】{{issue}}",
          "message":'''
                    您追蹤的投資議題 「<a href='{{page_url}}' target='_blank'>{{issue}} </a> 」已更新完成，請點擊查看
                    '''
        },
        
        "stock_news_summary_update": {
            "title": "【新聞總結】{{date}} ",
            "message":'''
                    個股新聞總結（{{date}}）已更新完成，請點擊<a href='{{page_url}}' target='_blank'>查看</a>
                    '''
        }
    },
    
    "alert":{
        "ticker_event_alert": {
            "title": "【事件提醒】{{event_tickers_str}}",
            "message": '''
                <div class="alert alert-info" role="alert">
                    以下為【{{start_date_str}}~{{end_date_str}}】，公司追蹤之個股及S&P500事件清單，詳情請點擊<a href='{{page_url}}' target='_blank'>事件總覽</a>查詢
                </div>
                <div class="card">
                    <div class="card-body">
                        <div class="card_text">
                            <p>
                            <i class="bi bi-calendar3-event"></i>
                            {% if responsible_event_tickers_str %}
                            其中您負責研究的個股包括：{{responsible_event_tickers_str}}
                            {% else %}
                            您負責研究的個股近{{days}}天沒有公司事件
                            {% endif %}
                            </p> 
                            
                            <p>
                            <i class="bi bi-calendar3-event"></i>
                            {% if following_event_tickers_str %}
                            其中您追蹤的個股包括：{{following_event_tickers_str}}
                            {% else %}
                             您追蹤的個股近{{days}}天沒有公司事件 
                            {% endif %}
                            </p> 
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <div class="card-body">
                    <table id="Table" class="table table-hover">
                        <thead>
                        <tr>
                            <th scope="col">#</th>
                            <th scope="col">Ticker</th>
                            <th scope="col">Date</th>
                            <th scope="col">Event</th>
                        </tr>
                        </thead>
                        <tbody>
                        {% for item_meta in event_meta_list %}
                            <tr {% if item_meta['ticker'] in following_event_ticker_list %} style="color: red;" {% endif %}>
                                <td>{{ loop.index }}</td>
                                <td>{{ item_meta['ticker'] }}</td>
                                <td>{{ item_meta['event_timestamp'] }}</td>
                                <td>{{ item_meta['event_title'] }}</td>
                            </tr>
                        {% endfor %}
                        </tbody>
                    </table>
                    </div>
                </div>
            '''
        }
    }
}