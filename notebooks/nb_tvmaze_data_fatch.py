# Databricks notebook source
# MAGIC %run ./nb_common_function

# COMMAND ----------

# DBTITLE 1,Cell 2
def fetch_shows(max_pages=5):
    all_shows = []
    for page in range(max_pages):
        url = f'{BASE_URL}/shows?page={page}'
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:            
            break # no more pages
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_shows.extend(data)
        time.sleep(0.5) # be polite to API
        print(f'Fetched {len(all_shows)} shows')
    return all_shows
shows_data = fetch_shows(max_pages=3)

# COMMAND ----------

# DBTITLE 1,Cell 3
def fetch_episodes(show_id):
    url = f'{BASE_URL}/shows/{show_id}/episodes'
    resp = requests.get(url, timeout=30)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    eps = resp.json()
    for ep in eps:
        ep['show_id'] = show_id # add FK
    return eps

def fetch_cast(show_id):
    url = f'{BASE_URL}/shows/{show_id}/cast'
    resp = requests.get(url, timeout=30)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    cast = resp.json()
    for c in cast:
        c['show_id'] = show_id
    return cast
# Sample: use first 20 show IDs
show_ids = [s['id'] for s in shows_data[:20]]
all_episodes, all_cast = [], []
for sid in show_ids:
    all_episodes.extend(fetch_episodes(sid))
    all_cast.extend(fetch_cast(sid))
    time.sleep(0.3)
print(f'Episodes: {len(all_episodes)} | Cast: {len(all_cast)}')


# COMMAND ----------

# DBTITLE 1,Cell 4

# Raw JSON paths
shows_raw_path = f'{RAW_PATH}/shows/shows_{RUN_DATE}.json'
episodes_raw_path = f'{RAW_PATH}/episodes/episodes_{RUN_DATE}.json'
cast_raw_path = f'{RAW_PATH}/cast/cast_{RUN_DATE}.json'

save_json_to_adls(shows_data, shows_raw_path)
save_json_to_adls(all_episodes, episodes_raw_path)
save_json_to_adls(all_cast, cast_raw_path)
