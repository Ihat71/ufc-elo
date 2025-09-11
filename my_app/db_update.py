from bs4 import BeautifulSoup
import string
import requests
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import logging
from elo import get_dates, to_table_date, elo_equation

db_path = (Path(__file__).parent).parent / "data" / "testing.db"
events_url = "http://ufcstats.com/statistics/events/completed?page=all"

#dont forget traceback.print_exc() using import traceback when you log bossman
logging.basicConfig(
    filename="update.log",
    filemode="a",  # 'w' overwrites, 'a' appends
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
) 

fighters_updated = []


#update events first then the rest become easy
def update_events():
    with sq.connect(db_path) as conn: 
        cursor = conn.cursor()
        #returns a sorted list of all the dates of the events in my events table
        all_ufc_dates = get_dates()
        up_to_date = all_ufc_dates[-1]
        event_list = []
        try:
            page = requests.get(events_url)
            soup = BeautifulSoup(page.text, 'html.parser')
            tbody = soup.find('tbody')
            tr_list = tbody.find_all('tr')
            for tr in tr_list:
                if not tr.text.strip() or tr.find('img'):
                    continue 
                td_list = tr.find_all('td')
                dt = datetime.strptime(td_list[0].find("span").text.strip(), "%B %d, %Y")
                if dt > up_to_date:
                    url = td_list[0].find('a')['href'].strip()
                    name = td_list[0].find('a').text.strip()
                    date = td_list[0].find('span').text.strip()
                    location = td_list[1].text.strip()
                    cursor.execute('insert into events (event_url, event_name, event_date, event_location) values (?, ?, ?, ?)', (url, name, date, location))
                    logging.info(f'successfully inserted into events the event {name}, url {url}')
                    conn.commit()
                    event_list.append((url, dt))
        except Exception as e:
            logging.error(f'error {e} in trying to make the events up to date')
        
        return event_list

def get_fighter(name):
    first_name, last_name = name.split(" ", 1)
    last_name_letter = last_name[0]
    url = f"http://ufcstats.com/statistics/fighters?char={last_name_letter}&page=all"
    page = requests.get(url)
    if page.status_code == 200:
        soup = BeautifulSoup(page.text, "html.parser")
        tbody = soup.find("tbody")
        tr_list = tbody.find_all('tr') 
    
        for x, tag in enumerate(tr_list):
            td = tag.find_all('td')
            if x == 0:
                continue
            if not (td[0].find("a").text.strip() == first_name and td[1].find("a").text.strip() == last_name):
                continue
            try:
                link = td[0].find("a")["href"]
            except:
                continue


            height = td[3].text.strip()
            weight = td[4].text.strip()
            reach = td[5].text.strip()
            stance = td[6].text.strip()
            wins = td[7].text.strip()
            losses = td[8].text.strip()
            draws = td[9].text.strip()
            fighter_url = link
            belt = td[10]
        
            if height == "--" or height == "''":
                height = "Unknown"
            if weight == "--" or height == "''":
                weight = "Unknown"
            if reach == "--" or height == "''":
                reach = "Unknown"
            if stance == "--" or height == "''":
                stance = "Unknown"

            #if there is an image tag then that means the fighter is a champion
            if belt.find('img'):
                belt = "Champ"
            else:
                belt = "--"
            break
        
        with sq.connect(db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("insert into fighters (name, height, weight, reach, stance, wins, losses, draws, champ_status, url) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                                (name, height, 
                                            weight, reach, stance, wins
                                                    , losses, draws, belt, fighter_url))
            conn.commit()

def put_elo(name):
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        fighter_id = cursor.execute('select fighter_id from fighters where name = ?', (name,)).fetchone()
        cursor.execute('insert into elo (fighter_id) values (?)', (fighter_id[0],))
        conn.commit()

    
        
def update_records_and_fights():
    event_list = update_events()
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for url, date in event_list:
            date = to_table_date(date)
            event_id = cursor.execute('select event_id from events where event_url = ?', (url,)).fetchone()[0]
            # try:
            session = requests.Session()
            page = session.get(url)
            page.raise_for_status()
            soup = BeautifulSoup(page.text, 'html.parser')
            tbody = soup.find('tbody')
            tr_list = tbody.find_all('tr')

            for tr in tr_list:
                td_list = tr.find_all('td')
                result = td_list[0].find('p').text.strip().lower()

                fighter_elements = td_list[1].find_all('p')
                if len(fighter_elements) < 2:
                    logging.error("Could not find both fighters, skipping fight")
                    continue

                fighter_a = fighter_elements[0].text.strip()
                fighter_b = fighter_elements[1].text.strip()
                logging.info(f"Processing fight: {fighter_a} vs {fighter_b}")

                if not cursor.execute('select * from fighters where name = ?', (fighter_a,)).fetchone():
                    get_fighter(fighter_a)
                    put_elo(fighter_a)
                    logging.info(f'put into fighters and elo table the fighter {fighter_a}')
                if not cursor.execute('select * from fighters where name = ?', (fighter_b,)).fetchone():
                    get_fighter(fighter_b)
                    put_elo(fighter_b)
                    logging.info(f'put into fighters and elo table the fighter {fighter_b}')

                row = cursor.execute('select fighter_id from fighters where name = ?', (fighter_a,)).fetchone()
                id_a = row[0] if row else None

                row = cursor.execute('select fighter_id from fighters where name = ?', (fighter_b,)).fetchone()
                id_b = row[0] if row else None

                if id_b < id_a:
                    id_a, id_b = id_b, id_a

                #note to self: recheck id swap logic

                weight_class = td_list[6].text.strip()
                is_title_fight = 'yes' if td_list[6].find('img', {"src":"http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/belt.png"}) else 'no'
                #a bit confusing but saves energy and time

                p_tags = td_list[7].find_all('p') if td_list[7].find_all('p') else None
                if len(p_tags) >= 2 and p_tags[1].text.strip() and p_tags:
                    method = f"{p_tags[0].text.strip()} ({p_tags[1].text.strip()})"
                elif len(p_tags) >= 1 and p_tags:
                    method = p_tags[0].text.strip()
                else:
                    method = "Unknown"

                row = cursor.execute('select fighter_id from fighters where name = ?', (fighter_a,)).fetchone()
                # winner = row[0] if row  else None
                if 'draw' in result:
                    winner = 0
                elif 'nc' in result or 'DQ' in method:
                    winner = None
                elif 'win' in result:
                    winner = row[0]
                else:
                    winner = 'Unknown'

                round_ended = int(td_list[8].text.strip())
                time_ended = td_list[9].text.strip()
                #inserting the fights for fighter_a and fighter_b
                cursor.execute('insert into fights (event_id, date, fighter_a, fighter_b, winner, weight_class, method, round_ended, time_ended, is_title_fight) values (?,?,?,?,?,?,?,?,?,?)', (event_id, date, id_a, id_b, winner, weight_class, method, round_ended, time_ended, is_title_fight))
                conn.commit()
                logging.info(f'successfully inserted into fights the fight of {id_a} vs {id_b} winner {winner}')
                #inserting records for fighter_a and fighter_b
                #for fighter_a
                if id_a == winner:
                    win_loss_a = 'win'
                    win_loss_b = 'loss'
                elif id_b == winner:
                    win_loss_b = 'win'
                    win_loss_a = 'loss'
                elif winner == 0:
                    win_loss_a = 'draw'
                    win_loss_b = 'draw'
                elif winner == None or 'DQ' in method:
                    win_loss_a = 'nc'
                    win_loss_b = 'nc'

                url_a = cursor.execute('select url from fighters where fighter_id = ?', (id_a,)).fetchone()[0]
                cursor.execute('insert into records (url, event_id, date, fighter_1, fighter_2, result, weight_class, method, round_num, fight_time, is_title_fight) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (url_a, event_id, date, id_a, id_b, win_loss_a, weight_class, method, round_ended, time_ended, is_title_fight))
                logging.info(f'inserted into records table the record for fighter {id_a}')
                #for fighter_b
                url_b = cursor.execute('select url from fighters where fighter_id = ?', (id_b,)).fetchone()[0]
                cursor.execute('insert into records (url, event_id, date, fighter_1, fighter_2, result, weight_class, method, round_num, fight_time, is_title_fight) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (url_b, event_id, date, id_b, id_a, win_loss_b, weight_class, method, round_ended, time_ended, is_title_fight))
                conn.commit()
                logging.info(f'inserted into records table the record for fighter {id_b}')

                #getting the new elos:
                rA = cursor.execute('select elo from elo where fighter_id = ?', (id_a,)).fetchone()[0]
                rB = cursor.execute('select elo from elo where fighter_id = ?', (id_b,)).fetchone()[0]
                elo_winner = 'A' if id_a == winner else 'B'
                is_draw = True if winner == 0 else False
                is_nc = True if winner == None else False
                new_rA, new_rB = elo_equation(rA, rB, elo_winner, is_draw, is_nc, method, round_ended, is_title_fight)
                cursor.execute('update elo set elo = ? where fighter_id = ?', (new_rA, id_a))
                cursor.execute('update elo set elo = ? where fighter_id = ?', (new_rB, id_b))
                conn.commit()
                logging.info(f'changed elo of fighter {id_a} from {rA} to {new_rA}')
                logging.info(f'changed elo of fighter {id_b} from {rB} to {new_rB}')

                fighters_updated.append(id_a)
                fighters_updated.append(id_b)



                #note to self: also update the draws and no contests, also add some logging
                #note to self: add elo history then everything is finally complete


            # except Exception as e:
            #     logging.error(f"exception {e} happened when trying to access the urls for records and fights for url {url} in date {date}")

# def update_elo_and_history():
#     ...

#yes I plugged in my previous update advanced stats function to cloud sonnet and copied its reviewd version, however
#I ONLY did this to learn the correct handling and logging practices later down the line from this code
def update_advanced_stats():
    # Remove potential duplicates
    unique_fighters = set(fighters_updated)
    logging.info(f"Updating advanced stats for {len(unique_fighters)} fighters")
    
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        
        for fighter in unique_fighters:
            try:
                logging.info(f"Processing advanced stats for fighter ID: {fighter}")
                
                # Get fighter URL with error handling
                url_result = cursor.execute('select url from fighters where fighter_id = ?', (fighter,)).fetchone()
                if not url_result or not url_result[0]:
                    logging.error(f"No URL found for fighter {fighter}")
                    continue
                    
                url = url_result[0]
                logging.info(f"Fetching stats from: {url}")
                
                # Make web request with error handling
                try:
                    page = requests.get(url, timeout=30)
                    page.raise_for_status()  # Raises exception for bad status codes
                except requests.RequestException as e:
                    logging.error(f"Failed to fetch page for fighter {fighter}: {e}")
                    continue
                
                soup = BeautifulSoup(page.text, 'html.parser')
                advanced_stat = {}
                
                # Find all li elements
                li_list = soup.find_all('li')
                logging.info(f"Found {len(li_list)} li elements for fighter {fighter}")
                
                stats_found = 0
                for li in li_list:
                    tag = li.find('i')
                    logging.info(f"tag html: \n {tag}")
                    if not tag:
                        continue
                        
                    stat_name = tag.text.strip().replace('.', '').replace(' ', '_').replace(':', '').lower()
                    
                    if stat_name in ['slpm', 'str_acc', 'sapm', 'str_def', 'td_avg', 'td_acc', 'td_def', 'sub_avg']:
                        # Extract the stat value more safely
                        try:
                            full_text = li.text.strip()
                            tag_text = tag.text.strip()
                            stat = full_text.replace(tag_text, '').strip()
                            
                            # Basic validation - make sure we got something
                            if stat and stat != '--' and stat != 'N/A':
                                advanced_stat[stat_name] = stat
                                stats_found += 1
                                logging.debug(f"Fighter {fighter}: {stat_name} = {stat}")
                            else:
                                advanced_stat[stat_name] = None
                                
                        except Exception as e:
                            logging.warning(f"Error parsing stat {stat_name} for fighter {fighter}: {e}")
                            advanced_stat[stat_name] = None
                
                logging.info(f"Fighter {fighter}: Found {stats_found} valid stats")
                
                # Check if fighter already exists in advanced_stats - FIXED
                existing = cursor.execute('select fighter_id from advanced_stats where fighter_id = ?', (fighter,)).fetchone()
                
                if existing:
                    # Update existing record
                    cursor.execute('''
                        update advanced_stats 
                        set SLpM=?, str_acc=?, SApM=?, str_def=?, td_avg=?, td_acc=?, td_def=?, sub_avg=? 
                        where fighter_id=?
                    ''', (
                        advanced_stat.get('slpm'), 
                        advanced_stat.get('str_acc'), 
                        advanced_stat.get('sapm'), 
                        advanced_stat.get('str_def'), 
                        advanced_stat.get('td_avg'), 
                        advanced_stat.get('td_acc'), 
                        advanced_stat.get('td_def'), 
                        advanced_stat.get('sub_avg'), 
                        fighter
                    ))
                    logging.info(f'Successfully updated advanced stats for fighter: {fighter}')
                    
                else:
                    # Insert new record - make sure column count matches your table schema
                    cursor.execute('''
                        insert into advanced_stats 
                        (fighter_id, url, SLpM, str_acc, SApM, str_def, td_avg, td_acc, td_def, sub_avg) 
                        values (?,?,?,?,?,?,?,?,?,?)
                    ''', (
                        fighter, 
                        url, 
                        advanced_stat.get('slpm'), 
                        advanced_stat.get('str_acc'), 
                        advanced_stat.get('sapm'), 
                        advanced_stat.get('str_def'), 
                        advanced_stat.get('td_avg'), 
                        advanced_stat.get('td_acc'), 
                        advanced_stat.get('td_def'), 
                        advanced_stat.get('sub_avg')
                    ))
                    logging.info(f'Successfully inserted advanced stats for fighter: {fighter}')
                
            except Exception as e:
                logging.error(f"Error processing fighter {fighter}: {e}")
                import traceback
                logging.error(f"Traceback: {traceback.format_exc()}")
                continue  # Continue with next fighter
        
        try:
            conn.commit()
            logging.info("Successfully committed all advanced stats updates")
        except Exception as e:
            logging.error(f"Error committing advanced stats: {e}")
            conn.rollback()

def main():
    #two things to fix: fetching fighters in update records and also the winner detection
    update_records_and_fights()
    update_advanced_stats()

if __name__ == "__main__":
    main()
