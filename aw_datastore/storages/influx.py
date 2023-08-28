import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone, timedelta
import json
import os
import copy

from aw_core.models import Event

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from . import logger
from .abstract import AbstractStorage


# Time combined with measure (bucket_id) and tag style make it unique this times series database
# The time as it is stored in the database becomes key to updates

# JSON structure for InfluxDB for events
# app/status in tags because it will allow easy filtering and grouping
# Rules of thumb: Tags are parameters you run WHERE against, Fields are
# what you SELECT but if you SELECT * you get both but you can't use WHERE
# on field all because performance first
"""
[
    {
        "mesasurement": "bucket_id_window",
        "tags": {
            "app": "app_name"
        },
        "time": "2009-11-10T23:00:00Z",
        "fields": {
            "id": 0
            "duration": 20
            "title": "webstie - chrome"
        }
    },
    {
        "measurement": "bucket_id_afk",
        "tags": {
            "status": "not-afk" // status as a tag there are only two groups
        }
        "time": "2009-11-10T23:00:00Z",
        "fields": {
            "id": 0
            "duration": 20
        }
    }
]
"""


VERSION = 1
ACTIVITY = "activity"

class InfluxDBStorage(AbstractStorage):
    """Uses a InfluxDB server as backend"""

    sid = "influxdb"

    def __init__(self, testing):
        # TODO put config in correct place
        # hardcoded for testing
        # host = "localhost" # defaults localhost
        # port = 8086 # defaults 8086
        # user = "admin" # defaults root
        # password = "admin123" # defaults root

        # token = os.environ.get("INFLUXDB_TOKEN")
        token = "AgMz1OhmvJfGEjh-L93uCbye7Xvc-9O6mQxYSrgBAHMD6RiHQnVnV1FSYNHwosVUiJObPv8S204j88BY_H-eDQ=="
        org = "Test"
        url = "http://localhost:8086"

        dbname = "activitywatch" + ("-testing" if testing else "")

        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.read_api = self.client.query_api()

        # self.client = InfluxDBClient(host, port, user, password, dbname)

        # self.db = InfluxDBClient(host, port, user, password, dbname)
        # Tags are parameters that can be used with a where or a group by
        # FIXME any parameter for an event can become a tag except time and bucket_id
        # This will be set across all bucket_ids so may need a way to set custom tags by bucket id
        # but this is very database specific
        # Made tags duplicate of fields for now
        # self.tags = ("app", "status")
        self.time_precision = WritePrecision.MS

    def _event_to_point(self, event: Event) -> Point:
        return (
            Point(ACTIVITY)
            .tag("app", event.client)
            .field("metadata", json.dumps(event.metadata))
        )

    # def _result_to_event(self, table: FluxTable/FluxRecord?) -> Event:
    #     return Event(

    #     )

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> None:
        # Need a sample to create a bucket. Not possible othewise
        # TODO: Think if more of these params should be tags

        # print("Creating bucket", bucket_id)
        # "bucket_id=aw-watcher-window_phoenix type_id=currentwindow client=aw-watcher-window hostname=phoenix created=2023-08-26T18:06:03.754327 name=None data=None"
        print(bucket_id, type_id, client, hostname, created, name, data)

        b = self.client.buckets_api().create_bucket(
            bucket_name=bucket_id,
            description=json.dumps(
                {
                    "id": bucket_id,
                    # TODO: Add timezone to "created". PeeWee like 2023-08-17T10:46:22.005100+00:00
                    # while influx is like 2023-08-27T03:54:56.254125
                    "created": created,
                    "name": name,
                    "type": type_id,
                    "client": client,
                    "hostname": hostname,
                    "data": data or {},
                }
            ),
        )

    def update_bucket(
        self,
        bucket_id: str,
        type_id: Optional[str] = None,
        client: Optional[str] = None,
        hostname: Optional[str] = None,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> None:
        # InfluxDB doesn't support updates
        # Need to create a duplicate bucket and then move data
        # So better to leave it for now
        raise Exception("Not support by DB")

    def delete_bucket(self, bucket_id: str) -> None:
        self.client.buckets_api().delete_bucket(bucket_id)

    def buckets(self) -> Dict[str, Dict[str, Any]]:
        # print("GET buckets API called")
        buckets = dict()
        for bucket in self.client.buckets_api().find_buckets().buckets:
            bucket_id = bucket.name

            if not bucket_id.startswith("aw-watcher"):
                continue

            metadata = self.get_metadata(bucket_id)
            buckets[bucket_id] = metadata
        return buckets

    def get_event(self, bucket_id: str, event_id: int) -> Optional[Event]:
        event = self.read_api.query(
            'from(bucket: "{}") |> filter(fn: (r) => r._measurement == "{}" and r.id == {})'.format(
                bucket_id, ACTIVITY, event_id
            )
        )[0]
        return Event(
            id=event.id,
            timestamp=event.time,
            duration=event.duration,
            data=event.fields,
        )

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> List[Event]:
        # print("______GET___EVENTS___WAS___CALLED_______")
        # print(bucket_id, limit, starttime, endtime)

        # _starttime = starttime.isoformat() if starttime else starttime
        # _endtime = endtime.isoformat() if endtime else endtime

        # query = f"from(bucket: '{bucket_id}') |> filter(fn: (r) => r._measurement == 'events'"

        # if endtime or starttime:
        #     query += f" and time > '{_starttime}' and time < '{_endtime}'"

        # if limit and limit > -1:
        #     query += f") |> limit(n: {limit})"
        # else:
        #     query += ")"

        query = f'from(bucket: "{bucket_id}") |> range(start: -1h) |> filter(fn: (r) => r["_measurement"] == "{ACTIVITY}")'

        # print("final query", query)
        # return [
        #     Event(id='test1', timestamp='2023-08-26T20:33:44.625000+00:00', duration=123.0, data={"app": "boomer"}),
        #     Event(id='test2', timestamp='2023-09-26T20:33:44.625000+00:00', duration=567.0, data={"url": "coder.com"}),
        # ]
        entries = self.read_api.query(query)
        print("GET_EVENTS_CALLED")

        events = []
        for entry in entries:
            # Passing timestamp value to avoid warning. but it doesn't really add any value here!
            for r in entry.records:
                # print("Record", r)

                if VERSION == 1:
                    info = {}
                    if r["_field"] == "info":
                        info = json.loads(r["_value"])
                        event = Event(
                            id=info["id"],
                            timestamp=r["_time"],
                            duration=info["duration"],
                            data=info["data"],
                        )
                        events.append(event)
                    else:
                        # print("Found other fields", r["_field"])
                        pass
                else:
                    pass

                # print("Record", r)
                # if r["_field"] == "id":
                #     event.id = r["_value"]
                #     event.timestamp = r["_time"]
                # if r["_field"] == "duration":
                #     event.duration = r["_value"]

                # event.data[r["_field"]] = r["_value"]
                # print("Event from record", event)

                # try: data = json.loads(r["_value"])
                # except: data = r["_value"]

                # duration = data.get("duration", 0)

                # if "timestamp" in data:
                #     del data["timestamp"]
                # if "duration" in data:
                #     del data["duration"]

                # events.append(
                #     Event(
                #         id=counter, # r.id, # FIXME: counter isn't reliable at all. it's on per API call basis.
                #         # it will get reset easily.
                #         timestamp=r["_time"], # r.timestamp,
                #         duration=duration, # r.duration,
                #         data=data.get("data", {}),
                #     )
                # )

        return events

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> int:
        # print("__GET__EVENT_COUNT__WAS_CALLED")
        # if starttime:
        #     _starttime = starttime.isoformat() if starttime else starttime
        # if endtime:
        #     _endtime = endtime.isoformat() if endtime else endtime

        # query = f"from(bucket: '{bucket_id}') |> filter(fn: (r) => r._measurement == 'events')"
        # if endtime or starttime:
        #     query += f" and time > '{_starttime}' and time < '{_endtime}'"

        query = f'from(bucket: "{bucket_id}") |> range(start: -1h) |> count()'
        count = self.read_api.query(query)[0].records[0]
        print("CCOUTN", count)
        return int(count["_value"])

    def get_metadata(self, bucket_id: str) -> dict:
        b = self.client.buckets_api().find_bucket_by_name(bucket_id)
        try: return json.loads(b.description)
        except: return {}


    def insert_one(self, bucket_id: str, event: Event) -> Event:
        # print("Trying to insert with _INSERT_ONE_")
        # print(bucket_id, event)
        # event = {'id': None, 'timestamp': datetime.datetime(2023, 8, 26, 12, 56, 22, 25000, tzinfo=datetime.timezone.utc), 'duration': datetime.timedelta(seconds=10, microseconds=56000), 'data': {'app': 'Code', 'title': 'influx.py - activitywatch - Visual Studio Code'}}

        if event.id is None:
            # TODO: Should update original event?
            pass
        else:
            # This probably comes only from self.replace_event
            # print("FOUND_EVENT_WITH_ID", event.id)
            pass

        if VERSION == 1:
            event_id = event.id or int(event.timestamp.timestamp())
            point = Point("activity").time(event.timestamp, self.time_precision).field("info", json.dumps({
                "id": event_id,
                "timestamp": event.timestamp.isoformat(),
                "duration": event.duration.total_seconds(),
                "data": event.data
            }))
            # Note: Adding anything as a field creates multiple entries for each point.
            # Don't add "timestamp" tag because it instantly bloats number of series in grafana graphs
            # will have to do too many adjustments to fix that!
            # .tag("timestamp", event_id) # .tag("id", event_id) # .field("event_id", event_id)
            # Note: Looks like "id" isn't allowed as a tag. It doesn't appear in the results

            for tag in ["app", "status"]:
                if tag in event.data:
                    point = point.tag(tag, event.data[tag])

            self.write_api.write(bucket=bucket_id, record=point)
        else:
            # This complicates the results a lot. I get multiple tables which is super hard to handle
            point = Point("activity").time(event.timestamp, WritePrecision.MS) # .field("id", event.id)
            point = point.field("duration", event.duration.total_seconds())

            for key, value in event.data.items():
                if key in ("status", "app"):
                    point = point.tag(key, value)

                point = point.field(key, value)

            point = point.field("id", int(event.timestamp.timestamp())) #

            self.write_api.write(bucket=bucket_id, record=point)
            return event


        # event_copy = copy.deepcopy(event)
        # timestamp_value = event.timestamp.isoformat()
        # duration_value = event.duration.total_seconds() # seconds

        # event_copy["timestamp"] = timestamp_value
        # event_copy["duration"] = duration_value

        # print("EVENT___AFTER_TRANSFORMATION")
        # print(event_copy)

        # {'id': None, 'timestamp': '2023-08-26T20:33:44.625000+00:00', 'duration': 3008.0, 'data': {'app': 'Code', 'title': 'influx.py - activitywatch - Visual Studio Code'}}

        # point = (
        #     Point("activity")
        #     .tag("bucket_id", bucket_id)
        #     .tag("event_id", event.id) # Mostly none
        #     # .tag("timestamp", timestamp_value)
        #     # .tag("duration", duration_value)
        #     .field("data", json.dumps({
        #         "id": event.id,
        #         # "timestamp": timestamp_value,
        #         # "duration": duration_value, # The actual APIs might be expected as ms. Might have multiple by 1000 later
        #         "data": event.data,
        #     }))
        #     .field("duration", duration_value)
        #     .time(event.timestamp, WritePrecision.MS)
        # )

    def insert_many(self, bucket_id: str, events: List[Event]) -> None:
        print("Trying to insert with _INSERT_BULK_")
        raise NotImplementedError()
        # points = [
        #     Point("events")
        #     .tag("bucket_id", bucket_id)
        #     .field("id", event.id)
        #     .field("timestamp", event.timestamp)
        #     .field("duration", event.duration)
        #     .field("data", event.data)
        #     for event in events
        # ]

        # self.write_api.write(bucket=bucket_id, records=points)

    def delete(self, bucket_id: str, event_id: int) -> bool:
        query = f"from(bucket: '{bucket_id}') |> filter(fn: (r) => r._measurement == 'events' and r.id == {event_id})"
        self.write_api.delete(query=query)

    def replace(self, bucket_id: str, event_id: int, event: Event) -> Event:
        print("CALLING_DELETE_FUNCTION")
        self.delete(bucket_id, event_id)
        return self.insert_one(bucket_id, event)

    def replace_last(self, bucket_id: str, event: Event) -> None:

        def _get_last_event(bucket_id: str) -> Event:
            query = f'from(bucket: "{bucket_id}") |> range(start: 0) |> last()'
            # res = self.read_api.query(query)[0].records[0]

            entries = self.read_api.query(query)

            # timestamp = 0
            # duration = 0
            # data = {}

            for e in entries:
                for r in e.records:

                    if VERSION == 1:
                        info = {}
                        if r["_field"] == "info":
                            info = json.loads(r["_value"])
                            event = Event(
                                id=info["id"],
                                timestamp=r["_time"],
                                duration=info["duration"],
                                data=info["data"],
                            )
                            return event
                        else:
                            pass
                            # print("Found other fields", r["_field"])
                    else:
                        pass

                    # print(r)
                    # if r["_field"] == "_time":
                    #     timestamp = r["_value"]
                    # if r["_field"] == "duration":
                    #     duration = r["_value"]
                    # if r["_field"] == "data":
                    #     data = r["_value"]

                    # try: data = json.loads(data)
                    # except: pass

            # event = Event(
            #     id=timestamp, # r.id, # FIXME: counter isn't reliable at all. it's on per API call basis.
            #     # it will get reset easily.
            #     timestamp=datetime.fromtimestamp(timestamp).replace(tzinfo=timezone.utc),
            #       # r.timestamp,
            #     duration=duration, # r.duration,
            #     data=data.get("data", {}),
            # )

            # return event

                    # duration = data.get("duration", 0)

                    # if "timestamp" in data:
                    #     del data["timestamp"]
                    # if "duration" in data:
                    #     del data["duration"]


                # try: value = json.loads(res["_value"])
                # except: value = {}

                # return Event(
                #     id=value.get("id"),
                #     timestamp=value.get("timestamp"),
                #     duration=value.get("duration", 0),
                #     data=value.get("data")
                # )

        last_event = _get_last_event(bucket_id)
        # print("Last event", last_event)
        last_event.id = event.id or int(event.timestamp.timestamp()) # int will truncate decimals. Is it a good idea??
        # print("Last event after change", last_event)
        self.insert_one(bucket_id, last_event)
