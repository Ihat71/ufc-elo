


def get_fighter_id(conn, name):
    '''this function is for the db_setup to modularize the getting of fighter ids'''
    cursor = conn.cursor()
    row = cursor.execute('select fighter_id from fighters where name = ?', (name,)).fetchone()
    return row[0]

def parse_espn_stats(i):
    '''this function is for the scraper where it is needed to parse the stats strings'''
    return i.text.lower().strip().replace(' ', '_').replace('/', '_').replace('-', '_') 


def get_column_query(fight_dict):
    '''makes a custom query so i dont have to keep writing queries im so done with that'''
    column_query = '(fighter_id,'
    values = '(?,'
    for key in fight_dict:
        if key.lower() in ['%body', '%leg', '%head']:
            key = f"{key.lower().replace('%', '')}_percentage"
        column_query += f"{key},"
        values += '?,'
    column_query = replace_last(column_query, ",", ")")
    values = replace_last(values, ",", ")")
    return column_query, values

def replace_last(text, old, new, count=1):
    """Replace the last occurence with a new word"""
    parts = text.rsplit(old, count)
    return new.join(parts)
