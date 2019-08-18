import pandas as pd
import requests
import datetime
from math import cos, asin, sqrt

# creates df of parking data
def get_parking_data():
    response = requests.get('https://opendata.arcgis.com/datasets/0060469c57864becb76a036d23236143_0.geojson').json()
    df = pd.io.json.json_normalize(response['features'])
    df = df.drop(df.columns[0], axis=1)
    new_columns = [col.rsplit('.')[1].upper() for col in df.columns.tolist()]
    df.columns = new_columns
    df[['LAT','LNG']] = pd.DataFrame(df['COORDINATES'].values.tolist(), index=df.index)
    return df

# gets distance between two points; source - https://stackoverflow.com/questions/41336756/find-the-closest-latitude-and-longitude
def distance(lat1, lng1, lat2, lng2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lng2-lng1)*p)) / 2
    return 12742 * asin(sqrt(a))

# returns the closest point
def closest(coords, v):
    return min(coords, key=lambda p: distance(v[0],v[1],p[0],p[1]))

# returns date of next street sweeping
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
def next_sweeping_period(clean_date, begin, end):
    clean_date = next_sweeping_date(clean_date)
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
    if pkgtype in ['Red Zone', 'Single Space Meter', 'Red Zone (Fire Hydrant)', 'Alley', 'White Zone',
                   'No Parking Any Time', 'Yellow Zone', 'Fire Hydrant', 'Crosswalk', 'Residential Zone',
                   'Single Space Meter (Double Space)', 'Crosswalk (No Markings)', 'No Parking Passenger Loading',
                   'Motorcycle Parking', 'R X R', 'Blue Zone', 'Cross Hatching', 'Taxi Zone', 'Bus Only',
                   'Official Vehicle Only', 'Construction', 'Fire Lane', 'Passenger Loading Zone', 'Bike Parking']:
        return True
    return False

# returns a dictionary for the coordinate
def get_parking(coord):
    coord_dict = df[(df['LAT'] == coord[0]) & (df['LNG'] == coord[1])].to_dict(orient='records')[0]
    coord_dict['PARKING_VIOLATION'] = timelimit_violation(coord_dict['TIMELIMIT']) or pkgtype_violation(coord_dict['PKGTYPE'])
    next_sweep = next_sweeping_period(coord_dict['PKGSDAY'], coord_dict['PKGSWBEG'], coord_dict['PKGSWEND'])
    if next_sweep is None:
        for key in ['NEXT_SWEEPING_START', 'NEXT_SWEEPING_END', 'NEXT_SWEEPING_HRS']:
            coord_dict[key] = None
    else:
        coord_dict['NEXT_SWEEPING_START'] = next_sweep[0].strftime("%Y-%m-%d %H:%M:%S")
        coord_dict['NEXT_SWEEPING_END'] = next_sweep[1].strftime("%Y-%m-%d %H:%M:%S")
        coord_dict['NEXT_SWEEPING_HRS'] = time_to_sweeping(next_sweep[0])
    return coord_dict

class ParkingSpot:
    def __init__(self, coord):
        self.originalCoord = coord
        self.coordinates = closest(df['COORDINATES'].tolist(), coord)
        self.all = get_parking(self.coordinates)
        self.distMeters = distance(coord[0], coord[1], self.coordinates[0], self.coordinates[1]) * 1000
        self.fullAddress = f"{self.all['ADDRESS']} {self.all['STREET']}{self.all['SUFFIX']} {self.all['PREFIX']}"
        self.timeLimit = self.all['TIMELIMIT']
        self.parkingType = self.all['PKGTYPE']
        self.permitArea = self.all['PERMITAREA']
        self.eventArea = self.all['EVTAREA']
        self.violation = self.all['PARKING_VIOLATION']
        self.sweepStart = self.all['NEXT_SWEEPING_START']
        self.sweepEnd = self.all['NEXT_SWEEPING_END']
        self.sweepHrs = self.all['NEXT_SWEEPING_HRS']

current = ParkingSpot([-121.50350012231698, 38.57408831516617])
print(current.coordinates)
