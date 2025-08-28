from bs4 import BeautifulSoup
import string
import requests
from pathlib import Path
import sqlite3 as sq
import time, random
import logging
from concurrent.futures import ThreadPoolExecutor



logging.basicConfig(
    filename="app.log",
    filemode="w",  # 'w' overwrites, 'a' appends
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
) 


db_path = (Path(__file__).parent).parent / "data" / "testing.db"
# stats_url = "http://ufcstats.com/statistics/fighters?char=a&page=all"
def get_ufc_fighters():
    #list of all fighters
    fighters_list = []
    seen = set()

    session = requests.Session()
    for i in list(string.ascii_lowercase):
        page = session.get(f"http://ufcstats.com/statistics/fighters?char={i}&page=all")
        if page.status_code == 200:
            soup = BeautifulSoup(page.text, "html.parser")
            tbody = soup.find("tbody")
            tr = tbody.find_all('tr') 
           
            for x, tag in enumerate(tr):
                # this dictionary should be local so that all the fighters dont point to the same pointer in the dict
                fighters = {}
                if x == 0:
                    continue
                td = tag.find_all('td')
                #this part of the code is to check if the fighter has already been seen so that the database doesnt get filled with redundancy
                try:
                    link = td[0].find("a")["href"]
                except:
                    continue

                if link in seen:
                    continue
                seen.add(link)

                for index, field in enumerate(["first_name", "last_name", "nick_name"]):
                    try:
                        fighters[field] = td[index].find("a").text.strip()
                    except:
                        fighters[field] = "--"

                fighters["height"] = td[3].text.strip()
                fighters["weight"] = td[4].text.strip()
                fighters["reach"] = td[5].text.strip()
                fighters["stance"] = td[6].text.strip()
                fighters["wins"] = td[7].text.strip()
                fighters["losses"] = td[8].text.strip()
                fighters["draws"] = td[9].text.strip()
                fighters["url"] = link
                belt = td[10]
                
                for field in ["height", "weight", "reach", "stance"]:
                    if fighters[field] == "--":
                        fighters[field] = "Unknown"

                #if there is an image tag then that means the fighter is a champion
                if belt.find('img'):
                    fighters["belt"] = "Champ"
                else:
                    fighters["belt"] = "--"
                fighters_list.append(fighters)
            #time.sleep(random.uniform(1, 3))

    #[{first_name, last_name, nick_name, height, weight, reach, stance, wins, losses, draws, belt, url}, ...]
    return fighters_list

def get_fighter_records():
    fighters_records = []
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        urls = cursor.execute("select url from fighters;").fetchall()
    session = requests.Session()
    for i in urls:
        try:
            page = session.get(i[0])
            page.raise_for_status()
            logging.info(f"Fetched {i[0]} successfuly")
        except Exception:
            logging.warning(f"Warning: could not connect to {i[0]}")
            continue
        if page.status_code == 200:
            soup = BeautifulSoup(page.text, 'html.parser')
            tbody = soup.find('tbody')
            tr_list = tbody.find_all('tr')
            
            for x, tr in enumerate(tr_list):
                fighter = {}
                if x == 0:
                    continue

                td_list = tr.find_all('td')
                win_loss = td_list[0].find_all('i')
    
                for tag in win_loss:
                    i_tag = "--"
                    if tag.text.strip() in ['win', 'loss', 'nc', 'draw', 'next']:
                        #I named it i_tag because in the website the information is inside <i>
                        i_tag = tag.text.strip()
                        break
                    
                if i_tag == 'next':
                    continue
                fighter['url'] = i[0]
                fighter['win_loss'] = i_tag
                opponents = td_list[1].find_all('p')
                fighter['fighter_1'] = opponents[0].text.strip()
                fighter['fighter_2'] = opponents[1].text.strip()
                method = td_list[7].find('p')
                fighter['method'] = method.text.strip()
                round_ended = td_list[8].find('p')
                fighter['round'] = round_ended.text.strip()
                time_ended = td_list[9].find('p')
                fighter['time'] = time_ended.text.strip()
                
                fighters_records.append(fighter)
            #time.sleep(random.uniform(1, 3))
        else:
            continue

    return fighters_records
        









                



