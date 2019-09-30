import pandas as pd
import json
import requests
import time
from datetime import datetime, timedelta
import calendar

# returns df of cleaned onstreet parking data
def get_df():
    response = requests.get('https://opendata.arcgis.com/datasets/0060469c57864becb76a036d23236143_0.geojson').json()
    df = pd.io.json.json_normalize(response['features'])
    new_columns = [column.rsplit('.')[-1].lower() for column in df.columns.tolist()]
    df.columns = new_columns
    df = df[['gisobjid', 'obj_code', 'address', 'aorb', 'street', 'suffix', 'prefix', 'evenodd', 'timelimit', 'pkgtype', 'aorp', 'permitarea', 'maxrate', 'evtarea', 'pkgenday', 'enbegin', 'enend', 'pkgsday', 'pkgswbeg', 'pkgswend', 'parkmob', 'tmstrcn', 'noparkdays', 'noparktime', 'asset_id', 'p_address', 'beat_num', 'objectid', 'coordinates']]
    df.rename(columns={'timelimit':'time_limit', 'pkgtype': 'pkg_type', 'evtarea': 'event_area', 'enbegin': 'parking_begin', 'enend': 'parking_end', 'pkgswbeg': 'sweeping_begin', 'pkgswend': 'sweeping_end', 'parkmob': 'park_mobile', 'tmstrcn': 'time_restriction'}, inplace=True)
    df[['lng','lat']] = pd.DataFrame(df['coordinates'].values.tolist(), index=df.index)
    df['parking_days'] = df['pkgenday'].apply(get_day_range)
    df[['sweeping_weeks', 'sweeping_days']] = pd.DataFrame(dict(zip(df.index, df['pkgsday'].apply(get_sweeping_schedule)))).T
    df[['nopark_days1', 'nopark_days2']] = pd.DataFrame(dict(zip(df.index, df['noparkdays'].apply(get_nopark_schedule)))).T
    df[['nopark_begin1', 'nopark_end1', 'nopark_begin2', 'nopark_end2']] = pd.DataFrame(dict(zip(df.index, df['noparktime'].apply(get_nopark_time)))).T
    for col in ['parking_begin', 'parking_end', 'sweeping_begin', 'sweeping_end']:
        df[col] = df[col].apply(format_time)
    df['timeLimit'] = df.time_limit.apply(get_time_limit)
    df['parkingType'] = df.pkg_type.apply(get_parking_type)
    return df

# returns weekday range ('MON-FRI') as an array of ints ([0, 1, 2, 3, 4])
def get_day_range(weekdays):
    if weekdays is None:
        return None
    day_range = list(map(lambda x: day_to_int(x), (weekdays).split('-')))
    days = []
    c = calendar.Calendar(day_range[0])
    for day in c.iterweekdays():
        days.append(day)
        if day == day_range[1]:
            return days

# returns [[weeks], [weekday int]] for sweeping from sweeping day string
def get_sweeping_schedule(schedule):
    if schedule is None:
        return [None, None]
    elif schedule[0].isdigit():
        return [[int(schedule[0])], [day_to_int(schedule.split()[1])]]
    else:
        return [[1,2,3,4,5], [day_to_int(schedule)]]

# returns weekday int from weekday string (e.g., 'MON')
def day_to_int(weekday):
    return None if weekday is None else time.strptime(weekday[:3],'%a').tm_wday

# returns time object from string (e.g., '8 am')
def format_time(t):
    if t is None:
        return None
    elif ':' in t:
        return datetime.strptime(t, '%I:%M %p').time()
    else:
        return datetime.strptime(t, '%I %p').time()

# returns [weekday range 1, weekday range 2] from string
def get_nopark_schedule(schedule):
    if schedule is None:
        return [None, None]
    else:
        schedule_list = schedule.split(" & ")
    if len(schedule_list) < 2:
        schedule_list.append(None)
    return [get_day_range(schedule_list[0]), get_day_range(schedule_list[1])]

# returns [nopark time start 1, end 1, start 2, end 2] from string
def get_nopark_time(times):
    if times is None:
        return [None, None, None, None]
    else:
        times = times.replace('Midnight', '12am').replace('.', ':').replace('and', '&').replace('am', ' am').replace('pm', ' pm')
    times_list = times.split(" & ")
    all_times = [t.split('-') for t in times_list]
    flat_list = [format_time(item) for sublist in all_times for item in sublist]
    if len(flat_list) < 3:
        flat_list.extend([None, None])
    return flat_list

"""
timeLimit
- NoParking
- None
- Hours / Minutes

parkingType
- Metered -> if it costs to park
- NoParking -> if it's illegal to park
- Free -> if it's free for anyone to park
- Restricted -> if it's dedicated parking
- Residential -> if it requires a residential permit

"""

def get_time_limit(limit):
    if limit in [None, 'No Limit']:
        return None
    else:
        return limit

def get_parking_type(pkg_type):
    if pkg_type in ['Red Zone', 'Driveway', 'Red Zone (Fire Hydrant)', 'Alley', 'White Zone', 'Yellow Zone', 'No Parking AnyTime', 'No Parking Any Time', 'Yellow Zone', 'Fire Hydrant', 'Crosswalk', 'Crosswalk (No Markings)', 'No Parking Passenger Loading', 'R X R', 'Cross Hatching', 'Taxi Zone', 'Bus Only', 'Official Vehicle Only', 'Fire Lane', 'Construction', 'Passenger Loading Zone', 'Bike Parking']:
        return 'NoParking'
    elif pkg_type in ['Single Space Meter', 'Pay & Display Space', 'Pay & Display (Meter In Space)', 'Single Space Meter (Double Space)', 'Pay-by-plate', 'Pay-by-plate meter in space']:
        return 'Metered'
    elif pkg_type in ['RT', 'Green Zone', 'Time Zone', 'No Restrictions', None]:
        return 'Free'
    elif pkg_type in ['Motorcycle Parking', 'Blue Zone', 'Dedicated Car Share']:
        return 'Restricted'
    elif pkg_type in ['Residential Zone']:
        return 'Residential'
    else:
        return 'Unclassified'

# refreshes table in bigquery
def refresh_bq(df):
    df.to_gbq('sacramento_parking.onstreet', project_id='projects-247703', if_exists='replace')

# function to trigger
def main(self):
    refresh_bq(get_df())
