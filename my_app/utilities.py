
def get_fighter_id(conn, name):
    '''this function is for the db_setup to modularize the getting of fighter ids'''
    cursor = conn.cursor()
    row = cursor.execute('select fighter_id from fighters where LOWER(name) = ?', (name.lower(),)).fetchone()
    if row:
        return row[0]
    else:
        return None

def parse_espn_stats(i):
    '''this function is for the scraper where it is needed to parse the stats strings'''
    return i.text.lower().strip().replace(' ', '_').replace('/', '_').replace('-', '_').replace('.', '')


def replace_last(text, old, new, count=1):
    """Replace the last occurence with a new word"""
    parts = text.rsplit(old, count)
    return new.join(parts)

def get_fighter_pair_url(fighter_pairs, fighter_name):
    for url, name in fighter_pairs:
        if name == fighter_name:
            return url
        