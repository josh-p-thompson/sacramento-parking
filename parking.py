import pandas as pd
from datetime import datetime, timedelta
import os
import json
from google.cloud import bigquery

# formats datetime to string
def format_datetime(date_time):
    return date_time.strftime('%Y-%m-%d %H:%M:%S')

# returns week # from datetime
def get_week_count(day):
    return (day.day-1) // 7+1

# returns string as time
def parse_time(datetime_string):
    return datetime.strptime(datetime_string, '%H:%M:%S').time()

# returns a schedule
def get_schedule(name, days, begin, end, weeks = [1,2,3,4,5]):
    date_list = pd.date_range(datetime.now().date(), periods=100).to_pydatetime().tolist()
    begin = parse_time(begin)
    end = parse_time(end)
    schedules = []
    for day in date_list:
        if day.weekday() in days and get_week_count(day) in weeks:
            begin_date = datetime.combine(day, begin)
            if begin > end:
                day += timedelta(days=1)
            end_date = datetime.combine(day, end)
            schedules.append({'type': name, 'begin': format_datetime(begin_date), 'end': format_datetime(end_date)})
    return schedules

def string_to_list(string_list):
    if string_list:
        return json.loads(string_list)
    else:
        return None

# returns list of all schedules
def get_all_schedules(spot):
    # get different schedules
    if all(schedule is None for schedule in [spot['parking_days'], spot['sweeping_days'],spot['nopark_days1']]):
        if spot['time_limit'] == 'No Parking Anytime':
            return [{'type': 'noparking', 'begin': None, 'end': None}]
        else:
            return [{'type': 'unknown', 'begin': None, 'end': None}]
    if spot['parking_days'] is not None:
        full_schedule = get_schedule('active', spot['parking_days'], spot['parking_begin'], spot['parking_end'])
    if spot['sweeping_days'] is not None:
        full_schedule.extend(get_schedule('sweeping', spot['sweeping_days'], spot['sweeping_begin'], spot['sweeping_end'], spot['sweeping_weeks']))
    if spot['nopark_days1'] is not None:
        full_schedule.extend(get_schedule('noparking', spot['nopark_days1'], spot['nopark_begin1'], spot['nopark_end1']))
    if spot['nopark_begin2'] is not None:
        if spot['nopark_days2'] is None:
            full_schedule.extend(get_schedule('noparking', spot['nopark_days1'], spot['nopark_begin2'], spot['nopark_end2']))
        else:
            full_schedule.extend(get_schedule('noparking', spot['nopark_days2'], spot['nopark_begin2'], spot['nopark_end2']))

    # sort schedules by time
    full_schedule.sort(key=lambda x: x['begin'])
    full_schedule.sort(key=lambda x: x['end'])
    # correct overlapping periods
    for index, value in enumerate(full_schedule):
        if value['type'] is 'active':
            # this happens to everything but the first schedule
            if index != 0:
                if full_schedule[index-1]['end'] > value['begin']:
                    full_schedule[index]['begin'] = full_schedule[index-1]['end']
            # this happens to everything but the last schedule
            if index != len(full_schedule)-1:
                if full_schedule[index+1]['begin'] < value['end']:
                    full_schedule[index]['end'] = full_schedule[index+1]['begin']
    for index, value in enumerate(full_schedule):
        # if last end does not equal current begin then create open period
        if full_schedule[index-1]['end'] != value['begin'] and index !=0:
            full_schedule.insert(index, {'periodType': 'open', 'begin': full_schedule[index-1]['end'], 'end': value['begin']})
    return full_schedule

def closest(lng, lat):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='/Users/Josh/Desktop/sacramento-parking/config/bqkey.json'
    bigquery_client = bigquery.Client(project='projects-247703')
    QUERY = '''
    SELECT *, ROUND(ST_DISTANCE(ST_GEOGPOINT(lng, lat), ST_GEOGPOINT({}, {})), 3) AS distMeters
    FROM `projects-247703.sacramento_parking.onstreet`
    ORDER BY distMeters
    LIMIT 1
    '''.format(lng, lat)
    query_job = bigquery_client.query(QUERY)
    result = query_job.result().to_dataframe().to_dict(orient='records')[0]
    for key in ['parking_days', 'sweeping_weeks', 'sweeping_days', 'nopark_days1', 'nopark_days2']:
        result[key] = string_to_list(result[key])
    return result

def spot(lng, lat):
    result = closest(lng, lat)
    result['schedule'] = get_all_schedules(result)
    spot_dict = {}
    for key in ['address', 'aorb', 'street', 'suffix', 'prefix', 'evenodd', 'aorp', 'permitarea', 'maxrate', 'event_area', 'park_mobile', 'lng', 'lat', 'timeLimit', 'parkingType', 'distMeters', 'schedule']:
        spot_dict[key] = result[key]
    return [spot_dict]
