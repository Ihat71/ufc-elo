from bs4 import BeautifulSoup
import string
import requests
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from utilities import *



logging.basicConfig(
    filename="scraper.log",
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
                        fighters[field] = ""

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
                    if fighters[field] == "--" or fighters[field] == "''":
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

def get_events():
    all_events = []
    url = "http://ufcstats.com/statistics/events/completed?page=all"
  

    session = requests.Session()

    page = session.get(url)
    if page.status_code == 200:
        logging.info("Successfully fethed event data! ")
        soup = BeautifulSoup(page.text, "html.parser")
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
        for x, tr in enumerate(tr_list):
            event_data = {}
            if x == 0:
                continue
            elif tr.find('img'):
                continue
            td_list = tr.find_all('td')
            event_data['event_url'] = td_list[0].find('a')['href'].strip()
            event_data['event_name'] = td_list[0].find('a').text.strip()
            event_data['event_date'] = td_list[0].find('span').text.strip()
            event_data['event_location'] = td_list[1].text.strip()
            all_events.append(event_data)

    return all_events
    
        
def get_fighter_records(url):
    fighters_records = []
    session = requests.Session()
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        # urls = cursor.execute("select url from fighters;").fetchall()
        try:
            page = session.get(url)
            page.raise_for_status()
            logging.info(f"Fetched (Records) {url} successfuly")
        except Exception:
            logging.warning(f"Warning (Records): could not connect to {url}")
            return None
        
        soup = BeautifulSoup(page.text, 'html.parser')
        tbody = soup.find('tbody')
        tr_list = tbody.find_all('tr')
            
        for x, tr in enumerate(tr_list):
            fighter = {}
            if x == 0:
                continue

            td_list = tr.find_all('td')
            win_loss = td_list[0].find_all('i')

            i_tag = "--"
            for tag in win_loss:
                if tag.text.strip().lower() in ['win', 'loss', 'nc', 'draw', 'next']:
                    #I named it i_tag because in the website the information is inside <i>
                    i_tag = tag.text.strip()
                    break
                    
            if i_tag == 'next':
                continue

            fighter['url'] = url
            fighter['win_loss'] = i_tag
            opponents = td_list[1].find_all('p')
            fighter['fighter_1'] = opponents[0].text.strip()
            fighter['fighter_2'] = opponents[1].text.strip()
            fighter['event'] = td_list[6].find('a').text.strip()
            fighter['event_date'] = (td_list[6].find_all('p'))[1].text.strip()
            fighter['weight_class'] = None
            title = td_list[6].find("img", {"src": "http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/belt.png"})
            if title:
                is_title_fight = 'yes'
            else:
                is_title_fight = 'no'
            fighter['is_title_fight'] = is_title_fight
            method = td_list[7].find_all('p')
            fighter['method'] = f"{method[0].text.strip()} ({method[1].text.strip()})" if method[1].text.strip() != '' else method[0].text.strip()
            round_ended = td_list[8].find('p')
            fighter['round'] = round_ended.text.strip()
            time_ended = td_list[9].find('p')
            fighter['time'] = time_ended.text.strip()

            dt = datetime.strptime(fighter.get('event_date'), "%b. %d, %Y")
            usable_date = dt.strftime("%B %d, %Y")
            event_url = cursor.execute('select event_url from events where event_date = ?', (usable_date,)).fetchone()
            if event_url:
                try:    
                    event_page = session.get(event_url[0])
                    event_page.raise_for_status()
                    event_soup = BeautifulSoup(event_page.text, 'html.parser')
                    tbody = event_soup.find('tbody')
                    tr_list = tbody.find_all('tr')
                    for tr in tr_list:
                        td_list = tr.find_all('td')
                        opponents = [p.text.strip() for p in td_list[1].find_all('p')]
                        if fighter.get('fighter_1') in opponents and fighter.get('fighter_2') in opponents:
                            weight_class = td_list[6].find('p').text.strip()
                            fighter['weight_class'] = weight_class
                    logging.info(f'successfully got the event url and weight class {fighter.get("weight_class")} of the fight of fighter: {fighter.get("fighter_1")} vs opponent: {fighter.get("fighter_2")} result: {fighter.get("win_loss")}, method: {fighter.get("method")}, round and time ended: ( {fighter.get("round")} | {fighter.get("time")} ), in event date {usable_date}')
                except Exception as e:
                    logging.warning(f'exception {e} happened when attempting to fetch url {event_url}')
            else:
                logging.warning(f'could not get event url {event_url[0] if event_url else event_url} in date {usable_date}')
            
            fighters_records.append(fighter)
            #time.sleep(random.uniform(1, 3))

    return fighters_records

def get_fighter_records_threaded(max_workers=4):
    fighters_records = []
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        urls = [u[0] for u in cursor.execute("SELECT url FROM fighters;").fetchall()]

    # run threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_fighter_records, url): url for url in urls}
        for future in as_completed(futures):
            try:
                records = future.result()
                fighters_records.extend(records)
            except Exception as e:
                logging.error(f"Error processing {futures[future]}, error: {e}")

    return fighters_records
        

def get_advanced_stats():
    advanced_fighters = []
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        url = cursor.execute("select url from fighters;").fetchall()

    session = requests.Session()
    for fighter_url in url:
        advanced_fighter = {}
        try:
            page = session.get(fighter_url[0])
            page.raise_for_status()
            logging.info(f"Fetched (Advanced_stats) {fighter_url[0]} successfuly")
        except Exception:
            logging.warning(f"Warning (Adcanced_stats): could not connect to {fighter_url[0]}")
            continue
   
        soup = BeautifulSoup(page.text, 'html.parser')
        left_stats_div = soup.find('div', class_='b-list__info-box-left')
        right_stats_div = soup.find('div', class_='b-list__info-box_style-margin-right')

        left_stats = left_stats_div.find_all('li')
        right_stats = right_stats_div.find_all('li')

        advanced_fighter['url'] = fighter_url[0]
        for i in left_stats:
            tag = i.find('i').text.strip()
            advanced_fighter[tag.replace(":", "").replace(".", "").replace(" ", "_").lower()] = i.text.strip().replace(tag, "")
        for j in right_stats:
            tag = j.find('i').text.strip()
            advanced_fighter[tag.replace(":", "").replace(".", "").replace(" ", "_").lower()] = j.text.strip().replace(tag, "")              
        advanced_fighters.append(advanced_fighter)
        
           

    return advanced_fighters

def get_tapology_stats(tapology_url, name, session):
    page = session.get(tapology_url)
    soup = BeautifulSoup(page.text, 'html.parser')
    #split it into 3 phases: striking clinch and ground
    thead = soup.find_all('thead', class_='Table_THEAD')
    tbody = soup.find_all('tbody', class_='Table_TBODY')
    striking_col = thead[0]
    striking = tbody[0]
    clinching_col = thead[1]
    clinching = tbody[1]
    ground_col = thead[2]
    ground = tbody[2]
    
    striking_dict = {}
    striking_fights_list = []
    for tr in striking.find_all('tr'):
        striking_data = {}
        td_list = tr.find_all('td')
        #but remember that %BODY %HEAD and %LEG stays the same in the dict 
        for index, col in enumerate([parse_espn_stats(i) for i in striking_col.find_all('th')]):
            if index == 2:
                continue
            if len(td_list) > 4 and td_list[4].text.strip() == '-':
                break
            striking_data[col] = td_list[index].text.strip()
        striking_fights_list.append(striking_data)
    striking_dict[name] = striking_fights_list

    clinching_dict = {}
    clinching_fights_list = []
    for tr in clinching.find_all('tr'):
        clinching_data = {}
        td_list = tr.find_all('td')
        for index, col in enumerate([parse_espn_stats(i) for i in clinching_col.find_all('th')]):
            if index == 2:
                continue
            if len(td_list) > 4 and td_list[4].text.strip() == '-':
                break
            clinching_data[col] = td_list[index].text.strip()
        clinching_fights_list.append(clinching_data)
    clinching_dict[name] = clinching_fights_list


    ground_dict = {}
    ground_fights_list = []
    for tr in ground.find_all('tr'):
        ground_data = {}
        td_list = tr.find_all('td')
        for index, col in enumerate([parse_espn_stats(i) for i in ground_col.find_all('th')]):
            if index == 2:
                continue
            if len(td_list) > 4 and td_list[4].text.strip() == '-':
                break
            ground_data[col] = td_list[index].text.strip()
        ground_fights_list.append(ground_data)
    ground_dict[name] = ground_fights_list
    
    return (striking_dict, clinching_dict, ground_dict)
        


def tapology_stats_threaded(max_workers=5):
    striking = {}
    clinching = {}
    ground_game = {}
    session = requests.Session()
    with sq.connect(db_path) as conn:
        url_1 = 'https://www.espn.com/search/_/q/'
        cursor = conn.cursor()
        rows = cursor.execute('select name from fighters;').fetchall()
        fighter_pairs = []
        for row in rows:
            name = row[0].lower()
            first_name, last_name = row[0].lower().split(' ')
            search_url = url_1 + f'{first_name}%20{last_name}'
            page = session.get(search_url)
            soup = BeautifulSoup(page.text, 'html.parser')
            sections = soup.find_all('section', class_='Card')
            for section in sections:
                if section.find('a'):
                    fighter_url = section.find('a')['href']
                    fighter_pairs.append((fighter_url, name))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_tapology_stats, url, name, session): (url, name) for url, name in fighter_pairs}
        for future in as_completed(futures):
            try:
                strike, clinch, ground = future.result()
                striking.update(strike)
                clinching.update(clinch)
                ground_game.update(ground)
            except Exception as e:
                logging.error(f"Error processing {futures[future]}, error: {e}")
    
    return (striking, clinching, ground_game)
            


