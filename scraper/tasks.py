import requests
import json
import datetime
import pandas as pd

from bs4 import BeautifulSoup
from celery.task.schedules import crontab
from celery.decorators import periodic_task
from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser

from scraper.models import Snapshot, Location, Stat
from scraper.serializers import (LocationSerializer,
                                SnapshotSerializer, StatSerializer)

def get_location_keys(url):
    """
    Function uses GET to fetch locations from the UI app database.
    It returns a dictionary with location names as keys and primary keys as
    values.
    """
    location_ui = requests.get(url)
    location_list = location_ui.json()
    location_keys = {}
    for locations in location_list:
        location_keys[locations['name']] = locations['pk']
    return location_keys

def get_snapshot_list(url):
    snapshots_ui = requests.get(url)
    return snapshots_ui.json()

def scrape(url='www.veturilo.waw.pl/mapa-stacji/'):
    """
    This function will extract the table from Veturilo website and create a
    Pandas dataframe from it.
    """
    req = requests.get('https://' + url)
    table = BeautifulSoup(req.text).table
    dat=[]
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        dat.append([ele for ele in cols if ele])

    cols = ['Location', 'Bikes', 'Stands', 'Free stands', 'Coords']
    df = pd.DataFrame(dat, columns=cols)
    df.dropna(inplace=True)
    return df


@periodic_task(run_every=crontab(minute='*/10'))
def take_snapshot(location_url = 'http://127.0.0.1:8000/scraper/api/locations/',
                snapshot_url = 'http://127.0.0.1:8000/scraper/api/snapshots/'
                ):
    """
    Function that scrapes the Veturilo website every 10 minutes,
    places the locations in the local database, and uses POST to inject
    data to the UI app database.
    """
    df = scrape()
    for i in df.index:
        single = df.loc[i]
        # create or get locations
        loc, created = Location.objects.get_or_create(
                                name=single['Location'],
                                all_stands=single['Stands'],
                                coordinates=single['Coords']
                                )

        # POST new location if it doesn't exist in the databases.
        if created:
            location_serializer = LocationSerializer(loc)
            location_json = JSONRenderer().render(location_serializer.data)
            r = requests.post(location_url, location_json,
                    headers={'Content-type': 'application/json'})

    location_keys = get_location_keys(location_url)
    # print(location_keys)
    # create Snapshot dicts. The will not be stored in the gatherer database.
    for i in df.index:
        single = df.loc[i]
        snapshot = dict(
            location = location_keys[single['Location']],
            avail_bikes = single['Bikes'],
            free_stands = single['Free stands'],
            timestamp = datetime.datetime.now()
        )

        snapshot_json = JSONRenderer().render(snapshot)
        r = requests.post(snapshot_url, snapshot_json,
                headers={'Content-type': 'application/json'})



@periodic_task(run_every=crontab(0, 0, day_of_month='1'))
def reduce_data(
            location_url = 'http://127.0.0.1:8000/scraper/api/locations/',
            snapshot_url = 'http://127.0.0.1:8000/scraper/api/snapshots/',
            stat_url = 'http://127.0.0.1:8000/scraper/api/stats/',
            snapshot_delete_url = 'http://127.0.0.1:8000/scraper/api/snapshot/',
            old_days = 10
            ):
    """
    Function averages data from every month and places it in a separate
    table. Data is derived from the UI app API.
    """
    ##### get the data from UI app API
    location_keys = get_location_keys(location_url)
    snapshot_list = get_snapshot_list(snapshot_url)

    cols = ['pk', 'location', 'avail_bikes', 'free_stands', 'timestamp', 'weekend']
    df = pd.DataFrame(snapshot_list, columns=cols)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # round time to 10min
    df['time'] = df['timestamp'].dt.round('10min').dt.strftime('%H:%M')

    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    cutoff_date = today - datetime.timedelta(days=old_days)

    # data for removal
    df_old = df[df['timestamp'] < cutoff_date]
    # data for statistics
    df_forstat = df[df['timestamp'].dt.month == last_month]

    group = df_forstat.groupby(['location', 'time', 'weekend'])
    # calculate means and SDs
    means = group.mean()
    sd = group.std()

    # Creating Stat dicts, but not storing them in the gatherer database.
    for location, time, weekend in means.index:
        subset_mean = means.xs((location, time, weekend), level=(0,1,2), axis=0)
        subset_sd = sd.xs((location, time, weekend), level=(0,1,2), axis=0)
        stat = dict(
            location = location,
            avail_bikes_mean = subset_mean['avail_bikes'][0],
            free_stands_mean = subset_mean['free_stands'][0],
            avail_bikes_sd = subset_sd['avail_bikes'][0],
            free_stands_sd = subset_sd['free_stands'][0],
            time = time,
            month = last_month,
            weekend = weekend
        )
        # serialize the data
        stat_json = JSONRenderer().render(stat)
        r = requests.post(stat_url, stat_json,
                headers={'Content-type': 'application/json'})

    # delete old data
    print (df_old)
    for pk in df_old['pk']:
        r = requests.delete(snapshot_delete_url + str(pk))
