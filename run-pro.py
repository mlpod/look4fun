import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine

time_step = time.strftime('%Y.%m',time.localtime(time.time()))
db_name = f'DATA.{time_step}.db'
if os.path.exists(db_name):
    pass
else:
    rec = [{'comment_id': '-1',
            'comment_parent': '-1',
            'comment_author': '-1',
            'comment_date': '-1',
            'vote_positive': 0,
            'vote_negative': 0,
            'comment_content': '-1'}]
    db_engine = create_engine(f'sqlite:///{db_name}')
    pd.DataFrame(rec).to_sql('comments', db_engine, index=False)
    rec = [{'comment_id': '-1',
            'comment_author': '-1',
            'comment_date': '-1',
            'vote_positive': '-1',
            'vote_negative': '-1',
            'text_content': '',
            'pics': '-1'}]
    pd.DataFrame(rec).to_sql('pics', db_engine, index=False)
    sql = "DELETE FROM pics WHERE comment_id = ?"
    db_engine.execute(sql,('-1',))
    sql = "DELETE FROM comments WHERE comment_id = ?"
    db_engine.execute(sql,('-1',))
    

engine = create_engine(f'sqlite:///{db_name}') 

headers = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
    'Connection': 'keep-alive',
    'Referer': 'http://www.baidu.com/'
}

def update_pics(record, engine):
    #print(record)
    sql = "DELETE FROM pics WHERE comment_id = ?"
    engine.execute(sql,(record['comment_id'],))
    sql = "INSERT INTO pics (comment_id,comment_author,comment_date,vote_positive,vote_negative,\
        text_content,pics) VALUES (?,?,?,?,?,?,?)"
    engine.execute(sql,(record['comment_id'],record['comment_author'],record['comment_date'],record['vote_positive'],
                        record['vote_negative'],record['text_content'],record['pics']))
    
def update_comments(record, engine):
    #print(record)
    sql = "DELETE FROM comments WHERE comment_id = ?"
    engine.execute(sql,(record['comment_id'],))
    sql = "INSERT INTO comments (comment_id,comment_parent,comment_author,comment_date,vote_positive,vote_negative,\
        comment_content) VALUES (?,?,?,?,?,?,?)"
    engine.execute(sql,(record['comment_id'],record['comment_parent'],record['comment_author'],record['comment_date'],record['vote_positive'],
                        record['vote_negative'],record['comment_content']))
    
records = []
pages = [3, 7, 12, 20]
for page in pages:
    try:
        url = f"http://i.jandan.net/?oxwlxojflwblxbsapi=jandan.get_pic_comments&page={page}"
    except:
        continue
    pics = requests.get(url, headers=headers).json()    
    for pic in pics['comments']:
        record = {
            'comment_id': pic['comment_ID'],
            'comment_author': pic['comment_author'],
            'comment_date': pic['comment_date'],
            'vote_positive': pic['vote_positive'],
            'vote_negative': pic['vote_negative'],
            'text_content': pic['text_content'].strip(),
            'pics': ",".join(pic['pics']),
        }
        update_pics(record, engine)
        try:
            tucaos = requests.get(f"http://i.jandan.net/tucao/{pic['comment_ID']}", headers=headers).json()
        except:
            continue
        for tucao in tucaos['tucao']:
            record = {
                'comment_id': str(tucao['comment_ID']),
                'comment_parent': str(tucao['comment_parent']),
                'comment_author': tucao['comment_author'],
                'comment_date': tucao['comment_date'],
                'vote_positive': tucao['vote_positive'],
                'vote_negative': tucao['vote_negative'],
                'comment_content': tucao['comment_content'].strip(),
            }
            update_comments(record, engine)
pics = pd.read_sql("select * from pics", engine)
comments = pd.read_sql("select * from comments", engine)
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), len(pics), len(comments))

