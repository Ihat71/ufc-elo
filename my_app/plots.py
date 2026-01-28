import sqlite3 as sq
import pandas as pd
# import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt
from pathlib import Path
from PIL import Image
import numpy as np

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
    fig.update_layout(
          polar=dict(
              radialaxis=dict(range=[0, 100], tick0=0, dtick=20)
          )
      )
    
    return fig

def clinching_analysis_plot(fighter_id, db):
    
  stats = db.execute('select clinch_strikes_pm_scaled as clinch_volume, effective_accuracy_scaled as clinch_accuracy, schl as head_hunter, \
            scbl as body_hunter from aggregate_clinching where fighter_id = ?', (fighter_id,)).fetchone()
  
  df = pd.DataFrame(dict(
    r=[stats['clinch_volume'], stats['clinch_accuracy'], stats['head_hunter'], stats['body_hunter']],
    theta=['Clinch Strike Volume', 'Clinch Strike Accuracy', 'Head Hunting', 'Body Hunting']
      )
    )
    

  fig = px.line_polar(df, r='r', theta='theta', line_close=True)
  fig.update_layout(
          polar=dict(
              radialaxis=dict(range=[0, 100], tick0=0, dtick=20)
          )
      )

  return fig

def grappling_analysis_plot(fighter_id, db):
    
  stats = db.execute('''select bjj_defence_scaled as bjj_def, effective_takedowns_scaled as takedowns, td_def_scaled as td_def, 
                    effective_control_scaled as control,  effective_gnp_scaled as gnp, 
                    effective_sub_threat_scaled as sub_threat from aggregate_grappling where fighter_id = ?''', (fighter_id,)).fetchone()
  
  df = pd.DataFrame(dict(
    r=[stats['takedowns'], stats['td_def'], stats['control'], stats['gnp'], stats['sub_threat'], stats['bjj_def']],
    theta=['Takedowns', 'Takedown Defence', 'Ground Control', 'Ground and Pound', 'Sub Threat', 'Sub Defence']
      )
    )
    

  fig = px.line_polar(df, r='r', theta='theta', line_close=True)
  fig.update_layout(
          polar=dict(
              radialaxis=dict(range=[0, 100], tick0=0, dtick=20)
          )
      )

  return fig

# def body_heat_map(db, fighter_id):
#   stats = db.execute('select sdbl, sdll, sdhl, tsl from aggregate_striking where fighter_id = ?', (fighter_id,)).fetchone()

# # Example strike data
#   head = stats['sdhl']
#   body = stats['sdbl']
#   leg = stats['sdll']

#   values = [head, body, leg]
#   labels = ["Head", "Body", "Leg"]

#   # Normalize for color intensity
#   max_val = max(values)
#   colors = [v/max_val for v in values]

#   fig, ax = plt.subplots()

#   # Draw simple body
#   head_circle = plt.Circle((0, 3), 0.3, color=plt.cm.hot(colors[0]))
#   body_rect = plt.Rectangle((-0.3, 1.5), 0.6, 1.2, color=plt.cm.hot(colors[1]))
#   leg_rect = plt.Rectangle((-0.3, 0), 0.6, 1.5, color=plt.cm.hot(colors[2]))

#   ax.add_patch(head_circle)
#   ax.add_patch(body_rect)
#   ax.add_patch(leg_rect)

#   ax.set_xlim(-1, 1)
#   ax.set_ylim(0, 4)
#   ax.set_aspect("equal")
#   plt.title("Strike Distribution Heatmap")
#   plt.axis("off")
#   plt.show()


# conn = sq.connect(db_path)
# conn.row_factory = sq.Row
# db = conn.cursor()
# body_heat_map(db, 2373)





