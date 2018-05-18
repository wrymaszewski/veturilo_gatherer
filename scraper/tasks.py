import requests
import pandas as pd

from bs4 import BeautifulSoup
from celery.task.schedules import crontab
from celery.decorators import periodic_task
from datetime import datetime, timedelta, date
from django.db.models import Avg

from scraper.models import Snapshot, Location, Stat
import datetime

from scraper.serializers import (LocationSerializer,
                                SnapshotSerializer, StatSerializer)
from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser
from django.utils.six import BytesIO

def scrape(url='www.veturilo.waw.pl/mapa-stacji/'):
    """
    This function will extract the table from Veturilo website and create a
    pandas dataframe from it.
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
def take_snapshot():
    """
    Function that scrapes the veturilo website every 30 minutes and places
    the raw data in the DB.
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
        print('Location: ' + loc.name)
        # add a new snapshot
        obj = Snapshot(
            location = loc,
            avail_bikes = single['Bikes'],
            free_stands = single['Free stands'],
            timestamp = datetime.now(tz = timezone('Europe/Warsaw'))
        )
        obj.save()

@periodic_task(run_every=crontab(hour='*/4'))
def send_data():
        """
        Serialization and sending the data to the UI app once in 4h.
        Then the the data is deleted.
        """
        # Locations
        locations = Location.objects.all()
        location_serializer = LocationSerializer(locations, many=True)
        location_json = JSONRenderer().render(location_serializer.data)
        ######code for API connection

        # Snapshots
        snapshots = Snapshot.objects.all()
        snapshot_serializer = SnapshotSerializer(snapshots, many=True)
        snapshot_json = JSONRenderer().render(snapshot_serializer.data)
        ######code for API connection

        # Clearing the databases
        snapshots.delete()

@periodic_task(run_every=crontab(0, 0, day_of_month='1'))
def reduce_data():
    """
    Function averages data from every month and places it in a separate
    table. Data is derived from the UI app API.
    """
    ##### get the data from API
    locations = Location.objects.all()
    # test
    snapshots = Snapshot.objects.filter(pk__lte=60)
    snapshot_serializer = SnapshotSerializer(snapshots, many=True)
    content = JSONRenderer().render(snapshot_serializer.data)

    stream = BytesIO(content)
    snapshots = JSONParser().parse(stream)

    lst=[]
    for snapshot in snapshots:
        lst.append([snapshot['location'], snapshot['avail_bikes'],
                    snapshot['free_stands'], snapshot['timestamp'],
                    snapshot['weekend']])
    cols = ['location', 'avail_bikes', 'free_stands', 'timestamp', 'weekend']
    df = pd.DataFrame(lst, columns=cols)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['time'] = df['timestamp'].dt.round('10min').dt.strftime('%H:%M')

    group = df.groupby(['location', 'time', 'weekend'])
    means = group.mean()
    sd = group.std()
    today = date.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)

    # Serialization
    obj_list = []
    for location, time, weekend in means.index:
        subset_mean = means.xs((location, time, weekend), level=(0,1,2), axis=0)
        subset_sd = sd.xs((location, time, weekend), level=(0,1,2), axis=0)
        stat = Stat(
            location = locations.get(pk=location),
            avail_bikes_mean = subset_mean['avail_bikes'],
            free_stands_mean = subset_mean['free_stands'],
            # avail_bikes_sd = subset_sd['avail_bikes'],
            avail_bikes_sd = 1,
            # free_stands_sd = subset_sd['free_stands'],
            free_stands_sd = 1,
            time = time,
            month = last_month,
            weekend = weekend
        )
        # serialize the data
        stat_serializer = StatSerializer(stat)
        obj_list.append(stat_serializer.data)

    # convert into json
    stat_json = JSONRenderer().render(obj_list)
    # print(stat_json)
    # ######code for API connection
