import os, sys
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import sqlite3 as sq
import pandas as pd
from my_app.scraper import get_ufc_fighters, get_events, get_fighter_records_threaded, get_advanced_stats
import logging

logging.basicConfig(
    filename="setup.log",
    filemode="w",  #w overwrites, a appends
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
) 


db_path = (Path(__file__).parent).parent / "data" / "testing.db"

def db_tables_setup():
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE if not exists fighters(
                    fighter_id integer primary key, 
                    name varchar(100) 
                    not null, 
                    height varchar(100), 
                    weight integer, 
                    reach integer, 
                    stance varchar(100), 
                    wins integer default 0, 
                    losses integer default 0, 
                    draws integer default 0, 
                    champ_status varchar(100),
                    url varchar(100)
                    );
                """)
        # the "url" column is the url of the fighter's record page
        cursor.execute("""CREATE TABLE if not exists records(
                       url varchar(100),
                       event_id integer,
                       date varchar(100),
                       fight_id integer primary key,
                       fighter_1 integer,
                       fighter_2 integer,
                       result varchar(100),
                       weight_class varchar(100),
                       method varchar(100),
                       round_num integer,
                       fight_time varchar(100),
                       is_title_fight varchar(100),
                       foreign key (fighter_1) references fighters(fighter_id),
                       foreign key (fighter_2) references fighters(fighter_id),
                       foreign key (event_id) references events(event_id)
                       );
                """)
        
        cursor.execute("""CREATE TABLE if not exists advanced_stats(
                       fighter_id integer,
                       url varchar(100),
                       SLpM float,
                       str_acc varchar(100),
                       SApM float,
                       str_def varchar(100),
                       td_avg float,
                       td_acc varchar(100),
                       td_def varchar(100),
                       sub_avg float,
                       foreign key (fighter_id) references fighters(fighter_id)                    
                       );""")
        
        cursor.execute("""CREATE TABLE if not exists events(
                       event_id integer primary key,
                       event_url varchar(100),
                       event_name varchar(100),
                       event_date varchar(100),
                       event_location varchar(100)         
                       );""")
        
        cursor.execute("""CREATE TABLE if not exists fights(
                       fight_id integer primary key,
                       event_id integer,
                       date varchar(100),
                       fighter_a integer,
                       fighter_b integer,
                       winner integer,
                       weight_class varchar(100),
                       method varchar(100),
                       round_ended integer,
                       time_ended varchar(100),
                       is_title_fight varchar(100),
                       foreign key (event_id) references events(event_id),
                       foreign key (fighter_a) references fighters(fighter_id),
                       foreign key (fighter_b) references fighters(fighter_id),
                       foreign key (winner) references fighters(fighter_id)
                       );""")
                
        conn.commit()
def fighters_table_setup():
    fighters = get_ufc_fighters()

    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for fighter in fighters:
            name = (fighter["first_name"] + " " + fighter["last_name"]).strip()
            height = fighter["height"]
            weight = fighter["weight"]        
            reach = fighter["reach"].replace("''", "")
            stance = fighter["stance"]
            wins = fighter["wins"]
            losses = fighter["losses"]
            draws = fighter["draws"]
            belt = fighter["belt"]
            url = fighter["url"]

            cursor.execute("insert into fighters (name, height, weight, reach, stance, wins, losses, draws, champ_status, url) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                            (name, height, 
                                        weight, reach, stance, wins
                                                , losses, draws, belt, url))
        conn.commit()


def events_table_setup():
    events = get_events()
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for event in events:
            url = event['event_url']
            name = event['event_name']
            date = event['event_date']
            location = event['event_location']
            cursor.execute('insert into events (event_url, event_name, event_date, event_location) values (?, ?, ?, ?)', (url, name, date, location))
        conn.commit()


def records_table_setup():
    records = get_fighter_records_threaded()
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for record in records:
            win_loss = record['win_loss']
            url = record['url']
            fighter_1_name = record['fighter_1']
            fighter_2_name  = record['fighter_2']
            event = record['event']
            date = record['event_date']
            weight_class = record['weight_class']
            method = record['method']
            round_ended = int(record['round']) if record['round'].isdigit() else None
            time = record['time']
            is_title_fight = record['is_title_fight']
            id_1 = cursor.execute('SELECT fighter_id FROM fighters WHERE name = ?', (fighter_1_name.strip(),)).fetchone()
            id_2 = cursor.execute('SELECT fighter_id FROM fighters WHERE name = ?', (fighter_2_name.strip(),)).fetchone()

            if not id_1 or not id_2:
                logging.warning(f"Could not find id of both fighters: {fighter_1_name}, {fighter_2_name}")
                continue

            event_id_row = cursor.execute('select event_id from events where event_name = ?', (event,)).fetchone()
            event_id = event_id_row[0] if event_id_row else None
            
            fighter_1_id, fighter_2_id = id_1[0], id_2[0]
            try:
                cursor.execute('insert into records (url, event_id, date, fighter_1, fighter_2, result, weight_class, method, round_num, fight_time, is_title_fight) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (url, event_id, date, fighter_1_id, fighter_2_id, win_loss, weight_class, method, round_ended, time, is_title_fight))
                logging.info(f"Successfully inserted into the records table the url: {url}, ids: {fighter_1_id} vs {fighter_2_id}")
            except Exception:
                logging.error(f"failed to insert record for url: {url}, {fighter_1_name} vs {fighter_2_name}")
            

        conn.commit()

def advanced_table_setup():
    advanced_fighters = get_advanced_stats()
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for advanced_fighter in advanced_fighters:
            url = advanced_fighter.get('url')
            fighter_id = cursor.execute('select fighter_id from fighters where url = ?', (url,)).fetchone()
            slpm = advanced_fighter.get('slpm')
            str_acc = advanced_fighter.get('str_acc')
            sapm = advanced_fighter.get('sapm')
            str_def = advanced_fighter.get('str_def')
            td_avg = advanced_fighter.get('td_avg')
            td_acc = advanced_fighter.get('td_acc')
            td_def = advanced_fighter.get('td_def')
            sub_avg = advanced_fighter.get('sub_avg')
            try:
                cursor.execute("""insert into advanced_stats (url, fighter_id, SLpM, str_acc, SApM, 
                        str_def, td_avg, td_acc, td_def, sub_avg) values (?,?,?,?,?,?,?,?,?,?)""", 
                        (url, fighter_id[0], slpm, str_acc, sapm, 
                        str_def, td_avg, td_acc, td_def, sub_avg))
                logging.info(f"Successfully inserted into the advanced_stats table the url: f{url}, id: {fighter_id[0]}")
            except Exception:
                logging.error(f"Failed to insert into advanced_stats table the url: f{url}, id: {fighter_id[0]}")
            
            
        conn.commit()

def fights_table_setup():
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        query = cursor.execute('''SELECT 
                                event_id, 
                                date,
                                CASE WHEN fighter_1 < fighter_2 THEN fighter_1 ELSE fighter_2 END AS fighter1,
                                CASE WHEN fighter_1 < fighter_2 THEN fighter_2 ELSE fighter_1 END AS fighter2,
                                CASE
                                    WHEN result = 'win'  THEN fighter_1
                                    WHEN result = 'loss' THEN fighter_2
                                    WHEN result = 'draw' THEN 0
                                    WHEN result = 'nc' THEN NULL
                                END AS winner,
                                weight_class,
                                method,
                                round_num,
                                fight_time,
                                is_title_fight
                            FROM records
                            GROUP BY date, fighter1, fighter2;
                ''').fetchall()
        for fight in query:
            event_id, date, fighter_1, fighter_2, winner, weight_class, method, round_ended, round_time, is_title_fight = fight
            try:
                cursor.execute('insert into fights (event_id, date, fighter_a, fighter_b, winner, weight_class, method, round_ended, time_ended, is_title_fight) values (?,?,?,?,?,?,?,?,?,?)', (event_id, date, fighter_1, fighter_2, winner, weight_class, method, round_ended, round_time, is_title_fight))
                logging.info("successfully inserted into fights")
            except Exception:
                logging.error("failed to insert fight into fights table")
        conn.commit()




def main():
    db_tables_setup()
    # fighters_table_setup()
    #events_table_setup()
    #records_table_setup()
    # advanced_table_setup()
    fights_table_setup()
    


if __name__ == "__main__":
    main()