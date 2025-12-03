import logging
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
db_path = (Path(__file__).parent).parent / "data" / "testing.db"

def career_analysis(db, id):
    '''analyses career data from the fighter tab in my_app'''
    fighter = db.execute('select * from fighters where fighter_id = ?;', (id,)).fetchall()
    fighter = dict(fighter[0])
    records = db.execute('select * from records where fighter_1 = ?', (id,)).fetchall()
    sorted_records = sorted(records, key=lambda x: datetime.strptime(x['date'], '%b. %d, %Y'), reverse=True)

    last_5 = sorted_records[0:5]
    win_streak = 0
    rounds=0
    last_round_time_seconds=0
    finishes=0
    round_end_times = []
    title_fights = 0
    decisions = 0
    subs = 0
    ko_tko = 0

    for i in sorted_records:
        i = dict(i)
        if i['result'] == 'win':
            win_streak += 1
        if i['method']:
            if i['method'].strip() not in ['U-DEC', 'M-DEC', 'S-DEC', 'CNC', "Overturned"]:
                finishes += 1
            else:
                decisions += 1
            if 'SUB' in i['method']:
                subs += 1
            elif 'KO' in i['method']:
                ko_tko += 1
        if i['is_title_fight'] == 'yes':
            title_fights += 1
        rounds += int(i['round_num'])
        minute, second = map(int, i['fight_time'].split(':'))
        last_round_time_seconds += int(minute) * 60 + int(second)
        round_end_times.append(f'{rounds * 5 + minute}:{second}')
    
    total_seconds = (rounds * 5 * 60) * 60 + last_round_time_seconds
    h = (total_seconds // 3600) 
    m = (total_seconds % 3600) // 60 
    s = total_seconds % 60 

    row = sorted_records[-1]
    debut = dict(row)['date']

    row = sorted_records[0]
    last_fight = dict(row)['date']

    seconds = []
    for i in round_end_times:
        m, s = i.split(':')
        seconds.append(int(m) * 60 + int(s))
    average_s = sum(seconds) / len(seconds)
    avg_m = average_s // 60
    avg_s = average_s % 60 

    career_hash = {
        'ufc_fights':len(sorted_records),
        'win_streak':win_streak,
        'wins':fighter['wins'],
        'losses':fighter['losses'],
        'draws':fighter['draws'],
        'last_5':last_5,
        'finishes':finishes,
        'debut':debut,
        'last_fight':last_fight,
        'cage_time':f'{h}:{m}:{s}',
        'win_rate': int(fighter['wins'])/len(sorted_records),
        'finish_rate':finishes/len(sorted_records),
        'average_fight_time':f"{avg_m}:{avg_s}",
        'title_fights':title_fights,
        'subs':subs,
        'ko/tko':ko_tko,
        'decisions':decisions,
    }

    return career_hash



def elo_analysis(id):
    '''analyses elo data from the fighter tab in my_app'''
    with sq.connect(db_path) as conn:
        conn.row_factory = sq.Row
        db = conn.cursor()
        row = db.execute('select * from elo where fighter_id = ?', (id,)).fetchall()
        elo = row[0]['elo']
        peak_elo = elo
        elo_history = db.execute('select * from elo_history where fighter_1 = ? or fighter_2 = ?', (id, id)).fetchall()
        columns = [desc[0] for desc in db.execute('select * from elo_history').description]

        #the df turns the columns into numbers so i have to manually put in columns lol
        df = pd.DataFrame(elo_history, columns=columns)
        #to be safe
        df.columns = [c.strip() for c in df.columns] 

        for row in df.itertuples(index=False):
            if getattr(row, 'fighter_1') == id:
                if row.elo_1 > peak_elo:
                    peak_elo = row.elo_1
            elif getattr(row, 'fighter_2') == id:
                if row.elo_2 > peak_elo:
                    peak_elo = row.elo_2


        latest_date = elo_history[-1]['date'] if elo_history else None

        elo_hash = {
            'elo':elo,
            'peak_elo':peak_elo,
            'latest_fight':latest_date
        }

    return elo_hash




