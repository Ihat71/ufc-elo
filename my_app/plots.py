import sqlite3 as sq
import pandas as pd
import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt
from pathlib import Path

db_path = Path(__file__).parent.parent / 'data' / 'testing.db'

#this python script is for parsing and then visualizing fight data
def elo_history_plot(fighter_id):
    with sq.connect(db_path) as conn:
        cursor = conn.cursor()
        query = f'''select 
          f.name as fighter,
          o.name as opponent, 
          h.winner, 
          CASE 
            WHEN h.fighter_1 = {fighter_id} THEN h.elo_1
            WHEN h.fighter_2 = {fighter_id} THEN h.elo_2
          END AS initial_elo,

          CASE 
            WHEN h.fighter_1 = {fighter_id} THEN h.new_elo_1
            WHEN h.fighter_2 = {fighter_id} THEN h.new_elo_2
          END AS new_elo,

          CASE 
            WHEN h.fighter_1 != {fighter_id} THEN h.new_elo_1
            WHEN h.fighter_2 != {fighter_id} THEN h.new_elo_2
          END AS opponent_new_elo,

          h.date
          from elo_history h 
          join fighters f on f.fighter_id = {fighter_id} 
          join fighters o on (o.fighter_id = h.fighter_1 or o.fighter_id = h.fighter_2) and o.fighter_id != {fighter_id}
          where h.fighter_1 = {fighter_id} or h.fighter_2 = {fighter_id}
          ;
            '''
        name = cursor.execute('select name from fighters where fighter_id = ?', (fighter_id,))
        df = pd.read_sql_query(query, conn)

    df['date'] = pd.to_datetime(df['date'], format = '%b. %d, %Y')
    df = df.sort_values('date')
    #print(df)

    fig = px.line(df, x="date", y="new_elo", markers=True, title="Elo History", hover_data=['new_elo', 'date', 'opponent', 'opponent_new_elo'])
    fig.update_traces(line_color="black", marker_color="red")
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    fig.update_layout(
        dragmode=False
    )
    return fig

def striking_analysis_plot(fighter_id, db):

    stats = db.execute('select true_ko_power_scaled as KO_power, str_def_scaled as defence, effective_volume_scaled as volume, sig_acc_scaled as sig_acc, SApM_scaled as sapm, ' \
    'leg_kicks_scaled as leg_kicks from aggregate_striking where fighter_id = ? ', (fighter_id,)).fetchone()

    df = pd.DataFrame(dict(
    r=[stats['KO_power'], stats['volume'], stats['defence'], stats['sig_acc'], stats['leg_kicks'], stats['sapm']],
    theta=['KO power','Volume','Defense', 'Significant Accuracy', 'Leg Kicks', 'Damage Asborbtion']
      )
    )
    

    fig = px.line_polar(df, r='r', theta='theta', line_close=True)
    # fig.update_traces(fill='toself')
    # fig.show()


    return fig





