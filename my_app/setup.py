import os, sys
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import sqlite3 as sq
import pandas as pd
from my_app.scraper import get_ufc_fighters, get_fighter_records
import logging

logging.basicConfig(
    filename="app.log",
    filemode="w",  # 'w' overwrites, 'a' appends
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
                       fight_id integer primary key,
                       fighter_1 integer,
                       fighter_2 integer,
                       w_l varcahr(100),
                       method varchar(100),
                       round_num integer,
                       fight_time varchar(100),
                       foreign key (fighter_1) references fighters(fighter_id)
                       foreign key (fighter_2) references fighters(fighter_id)
                       );
                """)
        conn.commit()
def fighters_table_setup():
    fighters = get_ufc_fighters()

    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for fighter in fighters:
            name = fighter["first_name"] + " " + fighter["last_name"]
            height = fighter["height"]
            weight = fighter["weight"]        
            reach = fighter["reach"].replace(r'""', '')
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

def records_table_setup():
    records = get_fighter_records()
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        for record in records:
            win_loss = record['win_loss']
            url = record['url']
            fighter_1_name= record['fighter_1']
            fighter_2_name  = record['fighter_2']
            method = record['method']
            round_ended = int(record['round']) if record['round'].isdigit() else None
            time = record['time']
            id_1 = cursor.execute('SELECT id FROM fighters WHERE name = ?', (fighter_1_name,)).fetchone()
            id_2 = cursor.execute('SELECT id FROM fighters WHERE name = ?', (fighter_2_name,)).fetchone()

            if not id_1 or not id_2:
                logging.warning(f"Could not find id of both fighters: {fighter_1_name}, {fighter_2_name}")
                continue

            fighter_1_id, fighter_2_id = id_1[0], id_2[0]
            try:
                cursor.execute('insert into records (url, fighter_1, fighter_2, w_l, method, round_num, fight_time) values (?, ?, ?, ?, ?, ?, ?)', (url, fighter_1_id, fighter_2_id, win_loss, method, round_ended, time))
            except Exception:
                logging.error(f"failed to insert record for url: {url}, {fighter_1_name} vs {fighter_2_name}")

        conn.commit()






def main():
    db_tables_setup()
    fighters_table_setup()
    records_table_setup()


if __name__ == "__main__":
    main()