import pandas as pd
import requests
import datetime
import time
from math import cos, asin, sqrt
import numpy as np

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def get_parking_data():
    response = requests.get('https://opendata.arcgis.com/datasets/0060469c57864becb76a036d23236143_0.geojson').json()
    df = pd.io.json.json_normalize(response['features'])
    df = df.drop(df.columns[0], axis=1)
    new_columns = [col.rsplit('.')[1].upper() for col in df.columns.tolist()]
    df.columns = new_columns
    df[['LAT','LNG']] = pd.DataFrame(df['COORDINATES'].values.tolist(), index=df.index)
    df['TIMELIMIT_VIOLATION'] = df['TIMELIMIT'].apply(timelimit_violation)
    df['PKGTYPE_VIOLATION'] = df['PKGTYPE'].apply(pkgtype_violation)
    df['NEXT_SWEEPING_PERIOD'] = df.apply(lambda x: next_sweeping_period(x['PKGSDAY'], x['PKGSWBEG'], x['PKGSWEND']), axis=1)
    df['NEXT_SWEEPING_START'] = df['NEXT_SWEEPING_PERIOD'].apply(lambda x: None if x is None else x[0])
    df['NEXT_SWEEPING_END'] = df['NEXT_SWEEPING_PERIOD'].apply(lambda x: None if x is None else x[1])
    df['NEXT_SWEEPING_HOURS'] = df['NEXT_SWEEPING_START'].apply(time_to_sweeping)
    return df

# gets distance between two points, from -> https://stackoverflow.com/questions/41336756/find-the-closest-latitude-and-longitude
def distance(lat1, lng1, lat2, lng2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lng2-lng1)*p)) / 2
    return 12742 * asin(sqrt(a))

# returns the closest point
def closest(coords, v):
    return min(coords, key=lambda p: distance(v[0],v[1],p[0],p[1]))

# returns date of next street sweeping datetime
def next_sweeping_date(clean_date):
    if clean_date is None:
        return None
    digit = clean_date[0]
    wkdy = time.strptime(clean_date.split()[-1][:3],"%a").tm_wday
    now = datetime.datetime.today()
    next_day = now.date() + datetime.timedelta(days=(wkdy-now.weekday())%7)
    if not digit.isdigit() or next_day.day < 7:
        return next_day
    next_month = pd.datetime(year=now.year, month=now.month + 1, day=1).date()
    if next_month.weekday() == wkdy:
        return next_month
    return next_month.replace(day=(1 + (wkdy - next_month.weekday()) % 7))

# returns start and end datetimes of next street cleaning
def next_sweeping_period(clean_day, begin, end):
    clean_date = next_sweeping_date(clean_day)
    if None in [clean_date, begin, end]:
        return None
    start_datetime = datetime.datetime.combine(clean_date, datetime.datetime.strptime(begin, '%I %p').time())
    end_datetime = datetime.datetime.combine(clean_date, datetime.datetime.strptime(end, '%I %p').time())
    now = datetime.datetime.today()
    if now > end_datetime:
        start_datetime += datetime.timedelta(days=7)
        end_datetime += datetime.timedelta(days=7)
    return [start_datetime, end_datetime]

# returns hours to next street sweeping
def time_to_sweeping(start_datetime):
    td = start_datetime - datetime.datetime.today()
    if td < datetime.timedelta(hours=0):
        return 0
    return td // pd.Timedelta(hours=1)

# returns True for timelimit violations
def timelimit_violation(limit):
    if limit in ['No Parking Anytime', '30 Minutes', '5 Minutes', '1 Hour', '15 Minutes', '90 Minutes']:
        return True
    return False

# returns True for parking type violations
def pkgtype_violation(pkgtype):
    if pkgtype in ['Red Zone', 'Red Zone (Fire Hydrant)', 'Alley', 'White Zone',
                   'No Parking Any Time', 'Yellow Zone', 'Fire Hydrant', 'Crosswalk', 'Residential Zone',
                   'Single Space Meter (Double Space)', 'Crosswalk (No Markings)', 'No Parking Passenger Loading',
                   'Motorcycle Parking', 'R X R', 'Blue Zone', 'Cross Hatching', 'Taxi Zone', 'Bus Only',
                   'Official Vehicle Only', 'Construction', 'Fire Lane', 'Passenger Loading Zone', 'Bike Parking']:
        return True
    return False

class ParkingSpot:
    def __init__(self, coord):
        self.originalCoord = coord
        self.coordinates = closest(df['COORDINATES'].tolist(), coord)
        self.all = df[(df['LAT'] == self.coordinates[0]) & (df['LNG'] == self.coordinates[1])].to_dict(orient='records')[0]
        self.distMeters = distance(coord[0], coord[1], self.coordinates[0], self.coordinates[1]) * 1000
        self.fullAddress = f"{self.all['ADDRESS']} {self.all['STREET']}{self.all['SUFFIX']} {self.all['PREFIX']}"
        self.timeLimit = self.all['TIMELIMIT']
        self.parkingType = self.all['PKGTYPE']
        self.permitArea = self.all['PERMITAREA']
        self.eventArea = self.all['EVTAREA']
        self.timeViolation = self.all['TIMELIMIT_VIOLATION']
        self.parkingViolation = self.all['PKGTYPE_VIOLATION']
