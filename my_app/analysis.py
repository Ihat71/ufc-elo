import sqlite3 as sq
import logging
from pathlib import Path
import sqlite3 as sq
from datetime import datetime
import numpy as np
import pandas as pd
# from sklearn.preprocessing import StandardScaler, MinMaxScaler

logger = logging.getLogger(__name__)
db_path = (Path(__file__).parent).parent / "data" / "testing.db"

LEAGUE_AVGS = {
    'ts_acc':0.53,
    'ss_acc':0.45,
    'sdh_acc':0.32,
    'sdb_acc':0.64,
    'sdl_acc':0.8,
    'ko_pm':0.012,
    'kd_pm':0.011,
    'scb_acc':0.70,
    'scl_acc':0.42,
    'sch_acc':0.48,
    'control_time':14
}

PRIOR_ATTEMPTS = 50
PRIOR_MINS = 20

def career_analysis(db, id):
    '''analyses career data from the fighter tab in my_app'''
    fighter = db.execute('select * from fighters where fighter_id = ?;', (id,)).fetchall()
    fighter = dict(fighter[0])
    records = db.execute('select * from records where fighter_1 = ?', (id,)).fetchall()
    sorted_records = sorted(records, key=lambda x: datetime.strptime(x['date'], '%b. %d, %Y'), reverse=True)

    last_5 = sorted_records[0:5] if len(sorted_records) >= 5 else sorted_records
    win_streak = 0
    rounds=0
    last_round_time_seconds=0
    finishes=0
    round_end_times = []
    title_fights = 0
    decisions = 0
    subs = 0
    ko_tko = 0

    winner = True
    for i in sorted_records:
        i = dict(i)
        if i['result'] == 'win' and winner:
            win_streak += 1
        elif i['result'] != 'win':
            winner = False
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
        'win_rate': round(int(fighter['wins'])/(int(fighter['wins']) + int(fighter['losses']) + int(fighter['draws'])), 2),
        'finish_rate': round(finishes/len(sorted_records), 2),
        'average_fight_time':f"{avg_m}:{avg_s}",
        'title_fights':title_fights,
        'subs':subs,
        'ko_tko':ko_tko,
        'decisions':decisions,
    }

    return career_hash



def elo_analysis(id):
    '''analyses elo data from the fighter tab in my_app'''
    with sq.connect(db_path) as conn:
        conn.row_factory = sq.Row
        db = conn.cursor()
        row = db.execute('select * from elo where fighter_id = ?', (id,)).fetchone()
        elo = row['elo']
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


def fight_analysis(db, fight):
    #incomplete
    fighter_1 = {}
    fighter_2 = {}
    if fight['result'] == 'win':
        fighter_1['result'] = 'winner'
        fighter_2['result'] = 'loser'
    else:
        fighter_1['result'] = 'loser'
        fighter_2['result'] = 'winner'

    fighter_1['name'] = db.execute('select name from fighters where fighter_id = ?', (fight['fighter_1'],)).fetchone()[0]
    fighter_2['name'] = db.execute('select name from fighters where fighter_id = ?', (fight['fighter_2'],)).fetchone()[0]

    fighter_1['id'] = fight['fighter_1']
    fighter_2['id'] = fight['fighter_2']

    return (fighter_1, fighter_2)

def s_analysis(db, id):
    '''this function is responsible for making a hash to make striking data very easy to get'''
    striking_data = db.execute('select * from aggregate_striking where fighter_id = ?', (id,)).fetchall()


    ...

'''-------- here starts the functions that are responsible for creating the aggregate analysis tables in the database'''

def fighter_striking_analysis(fighter_id, conn=None):
    '''what is important: knockdowns (KD), sig distance body/head/leg strikes attempts (sdbl/A or sdhl/A or sdll/A), sig strikes attempts SSA, sig strikes landed ssl, 
    total strike attempts TSA and landed TSL, strikes absorbed per min, strike accuracy, strikes landed per min, str def precentag'''
    conn.row_factory = sq.Row
    db = conn.cursor()
    query = f'select s.SLpM, s.SApM, s.str_def, a.* from advanced_stats s join advanced_striking a on s.fighter_id = a.fighter_id where s.fighter_id = {fighter_id}'
    # query_2 = 'select * from advanced_striking'
    rows = db.execute('select * from records where fighter_1 = ?', (fighter_id,)).fetchall()
    num_of_fights = len(rows)
    num_of_mins = get_fighter_minutes(rows)
    # print(num_of_mins)

    df = pd.read_sql_query(query, conn)
    
    df = df.drop_duplicates()
    df = df.replace({'-': None})
    df = df.map(lambda x: x.replace("\n", "") if isinstance(x, str) else x)
    df = df.dropna(subset=['sdhl_a', 'sdbl_a', 'sdll_a', 'tsl', 'tsa', 'ssl', 'ssa'])   
    df[['sdhl', 'sdha']] = df['sdhl_a'].str.split('/', expand=True)
    df[['sdbl', 'sdba']] = df['sdbl_a'].str.split('/', expand=True)
    df[['sdll', 'sdla']] = df['sdll_a'].str.split('/', expand=True)
    
    df = non_ufc_fight_remover(fighter_id, df, db)



    df[['sdhl', 'sdha', 'sdbl', 'sdba', 'sdll', 'sdla', 'tsl', 'tsa', 'ssl', 'ssa']] = (
        df[['sdhl', 'sdha', 'sdbl', 'sdba', 'sdll', 'sdla', 'tsl', 'tsa', 'ssl', 'ssa']].astype(int)
    )


    df = df.drop(columns=['sdhl_a', 'sdbl_a', 'sdll_a', 'tsl_tsa', 'date', 'opponent', 'espn_url', 'res'])

    percent_cols = ['body_percentage', 'head_percentage', 'leg_percentage', 'str_def']

    df[percent_cols] = df[percent_cols].apply(lambda col: col.str.rstrip('%').astype(float) / 100)

    #---- more accruate defense -----
    total_elo = 0
    total_opponents = 0
    for opp in rows:
        x = db.execute('select elo from elo where fighter_id = ?', (opp['fighter_2'],)).fetchone()[0]
        total_elo += x
        total_opponents += 1
    all_avg_opp_elo = total_elo/total_opponents if total_opponents > 0 else 1100
    df['str_def'] = df['str_def'] * (all_avg_opp_elo/1200)
    #---------------------------------

    df = (df.groupby(['fighter_id', 'SLpM', 'SApM', 'str_def']).agg({
        'tsl':'sum',
        'tsa':'sum',
        'ssl':'sum',
        'ssa':'sum',
        'kd':'sum',
        'body_percentage':'mean',
        'head_percentage':'mean',
        'leg_percentage':'mean',
        'sdhl':'sum',
        'sdha':'sum',
        'sdbl':'sum',
        'sdba':'sum',
        'sdll':'sum',
        'sdla':'sum'
    })).round({
        'body_percentage':3,
        'head_percentage':3,
        'leg_percentage':3
    })

    
    # df['ts_acc'] = df['tsl'] / df['tsa'] #total strikes
    # df['ss_acc'] = df['ssl'] / df['ssa'] #significant strikes
    # df['sdh_acc'] = df['sdhl'] / df['sdha'] #significant distance head strikes
    # df['sdb_acc'] = df['sdbl'] / df['sdba'] #significant distance body strikes
    # df['sdl_acc'] = df['sdll'] / df['sdla'] #significant distance leg strikes

    def bayes_rate(successes, attempts, league_avg, prior_attempts):
        return (successes + league_avg * prior_attempts) / (attempts + prior_attempts)

    df['ts_acc']  = bayes_rate(df['tsl'],  df['tsa'],  LEAGUE_AVGS['ts_acc'], PRIOR_ATTEMPTS)
    df['ss_acc']  = bayes_rate(df['ssl'],  df['ssa'],  LEAGUE_AVGS['ss_acc'], PRIOR_ATTEMPTS)
    df['sdh_acc'] = bayes_rate(df['sdhl'], df['sdha'], LEAGUE_AVGS['sdh_acc'], PRIOR_ATTEMPTS)
    df['sdb_acc'] = bayes_rate(df['sdbl'], df['sdba'], LEAGUE_AVGS['sdb_acc'], PRIOR_ATTEMPTS)
    df['sdl_acc'] = bayes_rate(df['sdll'], df['sdla'], LEAGUE_AVGS['sdl_acc'], PRIOR_ATTEMPTS)


    df['total_ssl_acc'] = df['ssl'] / df['tsa'] #accuracy of significant strikes landed compared to total strikes attempted
    df['total_ssa_percentage'] = df['ssl'] / df['tsl'] #how many of the fighter's landed strikes are significant
    df['tsl_pm'] = df['tsl'] / num_of_mins #measures the true volume of a fighter
    df['kd_pm'] = df['kd'] / num_of_mins

    opp_avg_elo = 0
    true_ko_power = 0
    avg_opp_durability = 0

    if len(rows) > 0:
        ko_hash = ko_power(db, rows, num_of_mins)
        ko_list = ko_hash['ko_list']
        ko_pm = ko_hash['ko_pm']
        df['ko_pm'] = ko_pm
        opp_avg_elo = ko_hash['opponent_average_elo']
        opp_str_def = 0
        opp_sapm = 0

        ko_types = []
        num_of_opps = len(ko_list)
        if num_of_opps > 0:
            for opp in ko_list:
                opp_str_def += opp['opponent_str_def']
                opp_sapm += opp['opponent_sapm']
                ko_types.append(opp['ko_type'])
        print(ko_list, ko_types)

        avg_opp_durability = opp_avg_elo * 0.6 + (opp_str_def/num_of_opps) * 0.3 + ((1/(opp_sapm/num_of_opps)) * 0.1) if (num_of_opps != 0 and opp_sapm != 0) else 0

        def bayes_rate_pm(count, mins, league_avg, prior_mins):
            return (count + league_avg * prior_mins) / (mins + prior_mins)

        ko_pm_adj = bayes_rate_pm(len(ko_list), num_of_mins, LEAGUE_AVGS['ko_pm'], PRIOR_MINS)
        kd_pm_adj = bayes_rate_pm(df['kd'], num_of_mins, LEAGUE_AVGS['kd_pm'], PRIOR_MINS)

        true_ko_power = (0.75 * ko_pm_adj + 0.25 * kd_pm_adj) * avg_opp_durability

    


    df['true_ko_power'] = true_ko_power
    df['avg_ko_opp_durability'] = avg_opp_durability
    df['avg_ko_opp_elo'] = opp_avg_elo

    #------------------- late additions lol
    df['effective_volume'] = df['tsl_pm'] * df['ts_acc']
    df['leg_kicks'] = (df['sdll'] / num_of_mins) * df['sdl_acc']
    df['sig_acc'] = df['ss_acc'] * ((0.5*df['sdh_acc']) + (0.25*df['sdb_acc']) + (0.25*df['sdl_acc']))

    df = df.round({
        'ts_acc':2,
        'ss_acc':2,
        'sdh_acc':2,
        'sdb_acc':2,
        'sdl_acc':2,
        'total_ssl_acc':2,
        'total_ssa_percentage':2,
        'kd_pm':2,
        'tsl_pm':2,
        'true_ko_power':2,
        'avg_ko_opp_durability': 2,
        'avg_ko_opp_elo': 2,
        'effective_volume':2,
        'leg_kicks':2,
        'sig_acc':2
    })

    print(fighter_id, '\n', df.to_string())
    return df


def fighter_clinch_analysis(fighter_id, conn=None):
    conn.row_factory = sq.Row
    db = conn.cursor()
    # query_2 = 'select * from advanced_striking'
    rows = db.execute('select * from records where fighter_1 = ?', (fighter_id,)).fetchall()
    num_of_fights = len(rows)
    num_of_mins = get_fighter_minutes(rows)
    # print(num_of_mins)

    df = pd.read_sql_query(
    'select * from advanced_clinch where fighter_id = ?',
    conn,
    params=(fighter_id,)
    )
    
    df = df.drop_duplicates()
    df = df.replace({'-': None})
    df = df.map(lambda x: x.replace("\n", "") if isinstance(x, str) else x)
    # sc stands for significant clinch strikes. h: head, b:body, l:leg
    df = df.dropna(subset=['scbl', 'scba', 'schl', 'scha', 'scll', 'scla'])   

    
    df = non_ufc_fight_remover(fighter_id, df, db)



    df[['scbl', 'scba', 'schl', 'scha', 'scll', 'scla']] = (
        df[['scbl', 'scba', 'schl', 'scha', 'scll', 'scla']].astype(int)
    )

    df = df.drop(columns=['rv', 'sr', 'tdl', 'tda', 'tds', 'tk_acc', 'date', 'opponent', 'espn_url', 'res'])

    if df.empty:
        return pd.DataFrame()


    df = (df.groupby(['fighter_id']).agg({
        'scbl':'sum',
        'scba':'sum',
        'schl':'sum',
        'scha':'sum',
        'scll':'sum',
        'scla':'sum'
    }))

    
    # df['ts_acc'] = df['tsl'] / df['tsa'] #total strikes
    # df['ss_acc'] = df['ssl'] / df['ssa'] #significant strikes
    # df['sdh_acc'] = df['sdhl'] / df['sdha'] #significant distance head strikes
    # df['sdb_acc'] = df['sdbl'] / df['sdba'] #significant distance body strikes
    # df['sdl_acc'] = df['sdll'] / df['sdla'] #significant distance leg strikes

    def bayes_rate(successes, attempts, league_avg, prior_attempts):
        return (successes + league_avg * prior_attempts) / (attempts + prior_attempts)

    # df['ts_acc']  = bayes_rate(df['tsl'],  df['tsa'],  LEAGUE_AVGS['ts_acc'], PRIOR_ATTEMPTS)
    # df['ss_acc']  = bayes_rate(df['ssl'],  df['ssa'],  LEAGUE_AVGS['ss_acc'], PRIOR_ATTEMPTS)
    # df['sdh_acc'] = bayes_rate(df['sdhl'], df['sdha'], LEAGUE_AVGS['sdh_acc'], PRIOR_ATTEMPTS)
    # df['sdb_acc'] = bayes_rate(df['sdbl'], df['sdba'], LEAGUE_AVGS['sdb_acc'], PRIOR_ATTEMPTS)
    # df['sdl_acc'] = bayes_rate(df['sdll'], df['sdla'], LEAGUE_AVGS['sdl_acc'], PRIOR_ATTEMPTS)

    prior_attempts = 15

    df['scb_acc'] = df['scbl'] / df['scba'].replace(0, pd.NA)
    df['scl_acc'] = df['scll'] / df['scla'].replace(0, pd.NA)
    df['sch_acc'] = df['schl'] / df['scha'].replace(0, pd.NA)

    df['scb_acc'] = bayes_rate(df['scbl'], df['scba'], LEAGUE_AVGS['scb_acc'], prior_attempts)
    df['scl_acc'] = bayes_rate(df['scll'], df['scla'], LEAGUE_AVGS['scl_acc'], 5)
    df['sch_acc'] = bayes_rate(df['schl'], df['scha'], LEAGUE_AVGS['sch_acc'], prior_attempts)

    df = df.fillna(0)

    df['total_attempted'] = df['scba'] + df['scha'] + df['scla']
    df['total_landed'] = df['scbl'] + df['schl'] + df['scll']

    df['clinch_strikes_pm'] = df['total_landed'] / num_of_mins if num_of_mins != 0 else 0
    df['effective_accuracy'] = 0.45 * df['scb_acc'] + 0.10 * df['scl_acc'] + 0.45 * df['sch_acc'] 
    
    df = df.round({
        'effective_accuracy':4
    })

    print(fighter_id, '\n', df.to_string())
    return df

def fighter_grappling_analysis(name, fighter_id, conn=None):
    conn.row_factory = sq.Row
    db = conn.cursor()
    # query_2 = 'select * from advanced_striking'
    record_rows = db.execute('select * from records where fighter_1 = ?', (fighter_id,)).fetchall()

    elo_rows = db.execute(
        '''
        SELECT
            CASE
                WHEN fighter_1 = ? THEN elo_2
                WHEN fighter_2 = ? THEN elo_1
            END AS opp_elo
        FROM elo_history
        WHERE fighter_1 = ? OR fighter_2 = ?
        ''',
        (fighter_id, fighter_id, fighter_id, fighter_id)
    ).fetchall()

    num_of_fights = len(record_rows)
    num_of_mins = get_fighter_minutes(record_rows)
    opp_avg_elo = sum([int(i['opp_elo']) for i in elo_rows]) / len(elo_rows) if len(elo_rows) > 0 else 0
    num_of_rounds = sum([int(i['round_num']) for i in record_rows])

    opp_ids = [row['fighter_2'] for row in record_rows]

    # print(num_of_mins)

    sub_count = 0
    subs_conceded = 0
    for row in record_rows:
        if 'sub' in row['method'].lower() and 'win' in row['result'].lower():
            sub_count += 1
        elif 'sub' in row['method'].lower() and 'loss' in row['result'].lower():
            subs_conceded += 1

    sub_attempts_faced = 0
    total_opp_control_time = 0
    for opp in opp_ids:
        opp_attempted_subs = db.execute('select sm from advanced_ground where fighter_id = ? and LOWER(opponent) like LOWER(?)', (opp, name)).fetchone()
        ctrl_time = db.execute('select control_time from advanced_stats where fighter_id = ?', (opp,)).fetchone()
        if ctrl_time and ctrl_time['control_time'] is not None:
            total_opp_control_time += ctrl_time['control_time']

        if opp_attempted_subs and opp_attempted_subs['sm'] is not None:
            sub_attempts_faced += opp_attempted_subs['sm']


    opp_avg_ctrl_time = total_opp_control_time / len(opp_ids) if opp_ids else 0

    # Step 1: Only join fight-level tables first
    df = pd.read_sql_query(
        f'''
        select g.*, c.tds, c.tdl, c.tda, c.tk_acc, c.rv, s.control_time, s.sub_avg, s.td_def
        from advanced_ground g 
        join advanced_clinch c on g.fighter_id = c.fighter_id and g.date = c.date
        join advanced_stats s on g.fighter_id = s.fighter_id
        where g.fighter_id = {fighter_id}
        ''',
        conn
    )

    # Step 2: Remove non-UFC fights
    df = non_ufc_fight_remover(fighter_id, df, db)

    # Step 3: Now join career stats if needed
    # stats_df = pd.read_sql_query(
    #     'select * from advanced_stats where fighter_id = ?',
    #     conn,
    #     params=(fighter_id,)
    # )
    # # Add career stats to every fight row (broadcast merge)
    # for col in stats_df.columns:
    #     if col != 'fighter_id':
    #         df[col] = stats_df.iloc[0][col]  # same career stats for all rows


    
    df = df.drop_duplicates()
    df = df.replace({'-': None})
    df = df.map(lambda x: x.replace("\n", "") if isinstance(x, str) else x)
    # sc stands for significant clinch strikes. h: head, b:body, l:leg
    df = df.dropna(subset=['tdl', 'tda', 'control_time', 'tds', 'tk_acc', 'rv', 'sgbl', 'sgba', 'sghl', 'sgha', 'sgll', 'sgla', 'sm', 'ad', 'adhg', 'adtb', 'adtm', 'adts'])   


    # df = non_ufc_fight_remover(fighter_id, df, db)


    df[['tdl', 'tda', 'tds', 'rv', 'sgbl', 'control_time', 'sgba', 'sghl', 'sgha', 'sgll', 'sgla', 'sm', 'ad', 'adhg', 'adtb', 'adtm', 'adts']] = (
        df[['tdl', 'tda', 'tds', 'rv', 'sgbl', 'control_time', 'sgba', 'sghl', 'sgha', 'sgll', 'sgla', 'sm', 'ad', 'adhg', 'adtb', 'adtm', 'adts']].astype(float)
    )



    if df.empty:
        return pd.DataFrame()

    df['tk_acc'] = df['tk_acc'].str.replace('%', '').astype(float) / 100
    df['td_def'] = df['td_def'].str.replace('%', '').astype(float) / 100

    # print(df)

    # Step 1: Group only fight-level stats
    fight_cols = ['tdl','tda','tds','tk_acc','rv','sgbl','sgba','sghl','sgha','sgll','sgla','sm','ad','adhg','adtb','adtm','adts']
    df_fight = df.groupby(['fighter_id']).agg({col:'sum' if col != 'tk_acc' else 'mean' for col in fight_cols}).reset_index()

    # Step 2: Add career-level stats back
    career_cols = ['sub_avg','td_def','control_time']
    for col in career_cols:
        if col in df.columns:
            df_fight[col] = df[col].iloc[0]

    df = df_fight


    df['td_pm'] = df['tdl'] / num_of_mins if num_of_mins != 0 else 0

    # ------------- Ground and Pound ----------------
    df['sgh_acc'] = df['sghl'] / df['sgha'].replace(0, np.nan)
    df['sgb_acc'] = df['sgbl'] / df['sgba'].replace(0, np.nan)
    df['sgl_acc'] = df['sgll'] / df['sgla'].replace(0, np.nan)

    df['effective_gnp'] = ( 0.45 * df['sgh_acc'] + 0.45 * df['sgb_acc'] + 0.10 * df['sgl_acc'] ) * (1 + (df['tds'] / df['tda']) * 0.1)
    df['effective_gnp'] = df['effective_gnp'] * df['tdl'] / num_of_mins

    # this one might be useless
    df['bjj_advances'] = (0.15*df['adhg'] + 0.35*df['adtb'] + 0.35*df['adtm'] + 0.15*df['adts']) / num_of_mins

    df['effective_takedowns'] = (
        (df['tdl'] / num_of_mins) *            # actual successful takedowns per minute
        (0.7 + 0.3 * df['tk_acc'])             # efficiency bonus, but not dominant
    )

    # ------ Effective Subs ------
    df['sub_attempts_pm'] = df['sm'] / num_of_mins if num_of_mins != 0 else 0
    df['subs_pm'] = sub_count / num_of_mins

    df['sub_success_rate'] = df['subs_pm'] / df['sub_attempts_pm'].replace(0, np.nan)
    df['sub_success_rate'] = df['sub_success_rate'].fillna(0)

    df['opp_avg_elo'] = opp_avg_elo


    df['effective_sub_threat'] = df['sub_attempts_pm'] * (df['sub_success_rate'] ** 1.5) * ((df['opp_avg_elo'] + 1200) / 1200) ** 1.25

    df['control_time_per_round'] = df['control_time'] / num_of_rounds if num_of_rounds != 0 else 0


    df['effective_control'] = (df['control_time_per_round'] ** 1.5) * (((df['opp_avg_elo'] + 1200 )/ 1200)) ** 0.75

    # ---------- BJJ defence ---------------
    #eps = 0.1

    df['subs_conceded_pm'] = (subs_conceded) / num_of_mins
    df['sub_attempts_faced_pm'] = (sub_attempts_faced) / num_of_mins

    sub_term = np.exp(-1.5 * df['subs_conceded_pm'])
    attempt_term = np.exp(-0.7 * df['sub_attempts_faced_pm'])

    df['opp_avg_control'] = opp_avg_ctrl_time / LEAGUE_AVGS['control_time']
    df['bjj_defence'] = np.log1p(
        sub_term * attempt_term * ((df['opp_avg_elo'] + 1200) / 1200) * df['opp_avg_control'] ** 0.75
    )

    # print(fighter_id, '\n', df.to_string())
    
    df = df.fillna(0)

    print(fighter_id, '\n', df.to_string())
    return df

def total_fighting_analysis(art_style):
    with sq.connect(db_path) as conn:
        conn.row_factory = sq.Row
        db = conn.cursor()
        fighters = db.execute('select DISTINCT a.fighter_id, f.name from advanced_clinch a join fighters f on a.fighter_id = f.fighter_id').fetchall()

        if art_style == 'striking':
            fighters_striking = []
            for fighter in fighters:
                strike_df = fighter_striking_analysis(fighter['fighter_id'], conn=conn)
                fighters_striking.append(strike_df)      
            
            fighters_df = pd.concat(fighters_striking).reset_index()

            # league_avgs = {
            #     'ts_acc': fighters_df['tsl'].sum() / fighters_df['tsa'].sum(),
            #     'ss_acc': fighters_df['ssl'].sum() / fighters_df['ssa'].sum(),
            #     'sdh_acc': fighters_df['sdhl'].sum() / fighters_df['sdha'].sum(),
            #     'sdb_acc': fighters_df['sdbl'].sum() / fighters_df['sdba'].sum(),
            #     'sdl_acc': fighters_df['sdll'].sum() / fighters_df['sdla'].sum(),
            # }

            # print(league_avgs)


            features_to_scale = [
                'ts_acc', 'ss_acc', 'sdh_acc', 'sdb_acc', 'sdl_acc',
                'total_ssl_acc', 'total_ssa_percentage', 'kd_pm', 'tsl_pm',
                'true_ko_power', 'avg_ko_opp_durability', 'avg_ko_opp_elo',
                'SLpM', 'SApM', 'str_def',
                'body_percentage', 'head_percentage', 'leg_percentage', 'effective_volume', 'leg_kicks', 'sig_acc'
            ]


            fighters_df = get_z_score(features_to_scale=features_to_scale, df=fighters_df, low=0.04, up=0.96)

                    
            fighters_df['SApM_scaled'] = 100 - fighters_df['SApM_scaled']

            fighters_df['Boxing'] = 0.35 * fighters_df['true_ko_power_scaled'] + 0.15 * fighters_df['effective_volume_scaled'] + 0.25 * fighters_df['str_def_scaled'] + 0.25 * fighters_df['SApM_scaled']
            fighters_df['KickBoxing'] = 0.25 * fighters_df['true_ko_power_scaled'] + 0.15 * fighters_df['effective_volume_scaled'] + 0.20 * fighters_df['str_def_scaled'] + 0.10 * fighters_df['SApM_scaled'] + 0.30 * fighters_df['leg_kicks_scaled']

        elif art_style == 'clinching':
            fighters_clinching = []
            for fighter in fighters:
                clinch_df = fighter_clinch_analysis(fighter['fighter_id'], conn=conn)
                fighters_clinching.append(clinch_df)      
            
            fighters_df = pd.concat(fighters_clinching).reset_index()

            features_to_scale = [
                'scbl', 'schl', 'scll', 'scb_acc', 'sch_acc', 'scl_acc', 'clinch_strikes_pm', 'effective_accuracy'
            ]

            fighters_df = get_z_score(features_to_scale=features_to_scale, df=fighters_df, low=0.04, up=0.96)

            fighters_df["Muay"] = 0.2 * fighters_df['clinch_strikes_pm_scaled'] + 0.8 * fighters_df['effective_accuracy_scaled']

        elif art_style == 'grappling':
            fighters_clinching = []
            for fighter in fighters:
                grappling_df = fighter_grappling_analysis(fighter['name'], fighter['fighter_id'], conn=conn)
                fighters_clinching.append(grappling_df)      
            
            fighters_df = pd.concat(fighters_clinching).reset_index()

            features_to_scale = [
                'tdl', 'tk_acc', 'sm', 'td_def', 'sub_avg', 'control_time', 'effective_takedowns',
                'td_pm', 'effective_gnp', 'bjj_advances', 'effective_sub_threat', 'control_time_per_round',
                'effective_control', 'opp_avg_elo', 'subs_pm', 'sub_attempts_pm', 'bjj_defence'
            ]

            fighters_df = get_z_score(features_to_scale=features_to_scale, df=fighters_df, low=0.04, up=0.96)

            fighters_df["Wrestling"] = 0.34 * fighters_df['effective_takedowns_scaled'] + 0.33 * fighters_df['effective_control_scaled'] + 0.33 * fighters_df['td_def_scaled']
            fighters_df["BJJ"] = 0.7 * fighters_df['effective_sub_threat_scaled'] + 0.3 * fighters_df['bjj_defence_scaled']
            fighters_df['GNP'] = fighters_df['effective_gnp_scaled']

        
        # fighters_df.head()
        # fighters_df.info()
        # fighters_df.shape
        # fighters_df.describe(include='all')

        fighters_df.to_sql(
            name=f"aggregate_{art_style}",
            con=conn,
            if_exists="replace",
            index=False
        )

        # print(striking_fighters_dict)

# def update_aggregate_tables():
#     ...
# def create_aggregate_tables():
#     ...

'''Here starts the analysis utitlity functions'''

def ko_power(db, fights, mins):
    '''KO power is the most abstract so it needs more effort to get an accurate measure of power
        How hard somebody hits relative to time, opportunity and the opponent's durability
    '''
    ko_num = 0
    opponents_average_elo = 0
    ko_list = []
    for fight in fights:
        if 'KO' in fight['method'] and 'win' in fight['result']:
            ko_dict = {}
            # ko_type = fight['method'].split(' ', 1)[1] if len(fight['method'].split(' ', 1)) > 1 else ''
            ko_type = fight['method']
            ko_dict['weight_class'] = (fight['weight_class'])
            ko_dict['ko_type'] = ko_type.replace('(', '').replace(')', '')
            ko_num += 1
            ko_dict['opponent'] = fight['fighter_2']
            ko_dict['date'] = fight['date']
            opponents_average_elo += int(db.execute(f"select elo from elo where fighter_id = {fight['fighter_2']}").fetchone()[0])
            ko_dict['opponent_str_def'] = float(db.execute(f"select str_def from advanced_stats where fighter_id = {fight['fighter_2']}").fetchone()[0].replace('%', '')) / 100
            ko_dict['opponent_sapm'] = db.execute(f"select SApM from advanced_stats where fighter_id = {fight['fighter_2']}").fetchone()[0]
            ko_list.append(ko_dict)

    opponents_average_elo = opponents_average_elo/ko_num if ko_num > 0 else 1100
    ko_rate = ko_num / len(fights)
    ko_pm = ko_num / mins

    ko_hash = {
        'ko_list':ko_list, #this is for contextualizing the KOs
        'ko_rate':ko_rate, #average of KOs per fight
        'ko_pm':ko_pm, #average of KO per cage minute time
        'opponent_average_elo':opponents_average_elo,
    }

    return ko_hash
    

def non_ufc_fight_remover(id, df, db):
    espn_dates = df['date'].apply(parse_date)

    rows = db.execute(
        'SELECT date FROM records WHERE fighter_1 = ?',
        (id,)
    ).fetchall()

    ufc_dates = [parse_date(r['date']) for r in rows]

    return df[espn_dates.isin(ufc_dates)]


def parse_date(d):
    # If it's a Series or tuple, take the first element
    if isinstance(d, (tuple, pd.Series)):
        d = d[0] if isinstance(d, tuple) else str(d)[1:]
    
    d = str(d).strip()  # ensure it's a string

    # known formats
    formats = ["%b. %d, %Y", "%b %d, %Y"]

    for fmt in formats:
        try:
            return datetime.strptime(d, fmt)
        except:
            continue

    raise ValueError(f"Unknown date format: {d}")



def get_fighter_minutes(fights):
    seconds = 0
    for row in fights:
        seconds += int(row['round_num']) * 5 * 60 + (int((row['fight_time'].split(':'))[0])) * 60
    return seconds // 60

def safe_split(col):
    return col.fillna("0/0").replace("-", "0/0").str.split("/", expand=True)

def get_hash_data(db, art_type, fighter_id):
    """ get the data of a specific fighter in a specific art type when given the db (sq.Row), art type (striking, clinching, grappling) and fighter id. Returns -1 if fail"""
    if art_type not in ['striking', 'clinching', 'grappling']:
        return -1
    row = db.execute(f"select * from aggregate_{art_type.lower()} where fighter_id = ?", (fighter_id,)).fetchone()
    if row:
        return row
    else:
        return -1

def get_z_score(features_to_scale=None, df=None, low=None, up=None):
    ''' features_to_scale: put the columns you want to change, fighters_df: the pandas df, low: the lower end of the z_score you want to clip, up: upper end of the clip'''
    '''low + up should be 1.0'''
    for col in features_to_scale:
        series = df[col]

        #z score
        median = series.median()
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1

        if iqr == 0:
            df[col + '_scaled'] = 50
            continue

        z = (series - median) / iqr

        # --- Clip extreme z-scores (prevents outliers from dominating) ---
        z_clipped = z.clip(lower=z.quantile(low), upper=z.quantile(up))

        # --- Map to 1–100 ---
        scaled = ((z_clipped - z_clipped.min()) / (z_clipped.max() - z_clipped.min())) * 99 + 1

        df[col + '_scaled'] = scaled.round(2)

    return df


# total_fighting_analysis('clinching')
# with sq.connect(db_path) as conn:
#     fighter_striking_analysis(2373, conn)

