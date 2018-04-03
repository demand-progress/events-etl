# encoding=utf8

import os
import requests
import json
import datetime
import usaddress
import urllib
import dateutil
from dateutil.tz import *
import redis

#const
UNNECESSARY_ELEMENTS = ['campaign', 'confirmed_at', 'created_at', 'creator', 'directions',  \
                        'ends_at', 'ends_at_utc', 'host_is_confirmed', \
                        'note_to_attendees', 'notes', 'phone', 'plus4', 'updated_at'\
                        ]
SUPER_GROUP = 'TeamInternet'
EVENT_TYPE = 'Action'

#Headers
_TITLE = 'title'
_URL = 'browser_url'
_STARTDATE = 'starts_at'

_PREURL = "https://act.demandprogress.org/event/action/"
_LIMIT = 20

# Town Hall Project
TOWN_HALL_URL = "https://townhallproject-86312.firebaseio.com/townHalls.json"

def grab_data():
    cleaned_data = retrieve_and_clean_data()

    translated_data = translate_data(cleaned_data)

    # retrieve_town_hall_events(cleaned_data)

    return translated_data

def retrieve_town_hall_events(current_ak_events):
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    redis_conn = redis.from_url(redis_url)
    last_updated = redis_conn.get("town_hall_last_updated")

    if last_updated:
        print "Got last updated " + str(last_updated)
        req = requests.get(TOWN_HALL_URL + '?print=pretty&orderBy=%22lastUpdated%22&startAt=' + str(last_updated))
    else:
        req = requests.get(TOWN_HALL_URL + '?print=pretty')
        print "Set last updated = " + str(int((datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000))
        redis_conn.set("town_hall_last_updated", int((datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000))

    if req.status_code != 200:
        raise ValueError("Error in retrieving data from the Town Hall Project ", req.status_code, req.text)
    else:
        existing_town_hall_events_in_ak = {}
        for e in current_ak_events:
            for field in e['fields']:
                if field['name'] == "town_hall_project_event_id" and field['value']:
                    existing_town_hall_events_in_ak[field['value']] = e['id']

        events = json.loads(req.text)
        print "Total num events to import from the Town Hall Project = " + str(len(events))

        for event_id in events:
            event = events[event_id]

            if "meetingType" not in event:
                print "Missing meetingType for event: "
                print event
                print "\n"
                continue

            if event['meetingType'] not in ['Office Hours', "Town Hall", "Tele-Town Hall", "Empty Chair Town Hall"]:
                continue

            if "address" not in event:
                print "Missing address for event: "
                print event
                print "\n"
                continue

            try:
                parsed_address = usaddress.tag(event['address'])[0]
            except usaddress.RepeatedLabelError as error:
                print('Error parsing address: ' + repr(error))
                print event
                print "\n"
                continue

            # XXX: skipping tele town halls and other places without a street name for now because we require an address
            if "ZipCode" not in parsed_address or "StreetName" not in parsed_address:
                print "Bad address: "
                print event
                print "\n"
                continue
            street_address = (parsed_address['AddressNumber'] if "AddressNumber" in parsed_address else "") + " " + (parsed_address["StreetNamePreDirectional"] if "StreetNamePreDirectional" in parsed_address else "") + " " + (parsed_address['StreetName'] if "StreetName" in parsed_address else "") + " " + (parsed_address['StreetNamePostType'] if "StreetNamePostType" in parsed_address else "")

            try :
                event_start = dateutil.parser.parse(event['Date'] + " " + event["Time"] + (" " + event['timeZone'] if 'timeZone' in event else ""))
            except ValueError as error:
                print ("Error parsing datetime: " + repr(error))
                print event
                print "\n"
                continue


            if event_start < datetime.datetime.now(event_start.tzinfo): # ignore past events
                continue

            event_title = (event['eventName'] if ('eventName' in event) else event['meetingType']) + (" - " + event['Member'] if ("Member" in event) else "") + (" - " + event['District'] if ("District" in event) else "")
            event_description = event_title + "\n"
            if "Notes" in event:
                event_description += event["Notes"]
            if "timeEnd" in event and event['timeEnd']:
                event_description += "\n\nEvent ends at " + event['timeEnd']
            if "linkName" in event:
                event_description += "\n\n" + event['linkName'] + ": " + event['link']

            if event['eventId'] in existing_town_hall_events_in_ak:
                # Update existing event in AK
                akId = str(existing_town_hall_events_in_ak[event['eventId']])
                ak_event = {
                    "address1": street_address,
                    "postal": parsed_address['ZipCode'],
                    "public_description": event_description,
                    "starts_at": event_start.strftime("%m/%d/%Y %H:%M"),
                    'title': event_title,
                    "venue": event['Location'] if "Location" in event else ""
                }
                action_endpoint = os.environ.get('ACTION_KIT_REST_URL') + "event/" + akId + "/"
                print "Updating event  " + action_endpoint + " in AK: " + json.dumps(ak_event)
                req = requests.patch(action_endpoint, data=json.dumps(ak_event), headers = {"Content-type": "application/json", "Access": 'application/json'})
                if req.status_code > 299:
                    raise ValueError("Error updating town hall event in Action Kit ", req.status_code, req.text, ak_event)
                else:
                    print "Successfully updated event! "
            else:
                # Add new event to AK
                ak_event = {
                    "page": "team-internet_create",
                    "email": "support@demandprogress.org",
                    "event_address1": street_address,
                    "event_postal": parsed_address['ZipCode'] if "ZipCode" in parsed_address else "",
                    "event_host_ground_rules": "1",
                    "event_host_requirements": "1",
                    "event_max_attendees": "5000",
                    "event_public_description": event_description,
                    "event_starts_at_ampm": event_start.strftime("%p"),
                    "event_starts_at_date": event_start.strftime("%m/%d/%Y"),
                    "event_starts_at_time": event_start.strftime("%I:%M"),
                    "event_title": event_title,
                    "event_venue": event['Location'] if ("Location" in event and event['Location']) else event['meetingType'],
                    "event_is_approved": "1",
                    "event_host_is_confirmed": "1",
                    "name": "Team Internet",
                    "phone": "000-000-0000",
                    "zip": parsed_address['ZipCode'] if "ZipCode" in parsed_address else "",
                    "action_town_hall_project_event_id": event['eventId'],
                    "action_categories": event['meetingType'].lower().replace(" ", "").replace("-", "")
                }
                print "Adding new event to AK: " + str(ak_event)
                action_endpoint = os.environ.get('ACTION_KIT_REST_URL') + "action/"
                req = requests.post(action_endpoint, data = ak_event, headers = {"Access": 'application/json'})
                if req.status_code != 201:
                    raise ValueError("Error creating town hall event in Action Kit ", req.status_code, req.text, ak_event, event)
                else:
                    print "Successfully created event! "
            # EO add new event
        # EO for event_id in events
    return 1

def retrieve_and_clean_data():
    """
    We retrieve data through the API and URL given to us by the
    partner organization. We remove the unnecessary elements as
    defined in UNNECESSARY_ELEMENTS
    """

    print(" -- Retrieving Team Internet Action")
    # start at page 1
    page = 0
    has_more_content = True
    event_endpoint = os.environ.get('EVENT_CAMPAIGN_URL')

    cleaned_data = []

    total_signups = 0

    # XXX: for some reason AK is returning some duplicate events, so track and dont double include them
    event_ids = []

    while has_more_content:
        offset = page * _LIMIT
        req = requests.get(event_endpoint + ("&_offset=%d" % offset), data={'_limit': _LIMIT}, headers={"Access": 'application/json'})
        print ("---- Going to Page", page, offset, req.status_code)

        page = page + 1
        print (req)
        if req.status_code != 200:
            raise ValueError("Error in retrieving ", req.status_code)
        else:
            json_data = json.loads(req.text)
            events = json_data['objects']
            has_more_content = len(events) == _LIMIT

            for event in events:
                # remove private data

                if not event["is_approved"]:
                    continue

                if not event["status"] == "active":
                    continue

                # Skip events we already have
                if event['id'] in event_ids:
                    continue

                event_ids.append(event['id'])

                for unneeded_key in UNNECESSARY_ELEMENTS:
                    if unneeded_key in event:
                        del event[unneeded_key]
                # print("\n\n")
                total_signups = total_signups + event['attendee_count']
                cleaned_data.append(event)

            # will continue to traverse if has more content
    #endof while has content

    return cleaned_data


def translate_data(cleaned_data):
    """
    This is where we translate the data to the necessary information for the map
    """
    print(" -- Translating Team Internet Event")
    translated_data = []

    for data in cleaned_data:

        address = clean_venue(data)

        group_name = data['title']
        has_coords = 'latitude' in data and 'longitude' in data

        if not has_coords:
            continue

        # ignore events older than 24 hours ago
        yesterday = datetime.date.today() - datetime.timedelta(1)
        if data['starts_at'][:10] < yesterday.strftime('%Y-%m-%d'):
            continue

        categories = []
        for field in data['fields']:
            if field['name'] == 'categories':
                categories.append(field['value'])
        
        event = {
            'id': data['id'],
            'title': data[_TITLE] if _TITLE in data else None,
            'url': _PREURL + ("%d" % data['id']),
            'supergroup' : SUPER_GROUP,
            'group': group_name,
            'event_type': EVENT_TYPE,
            'start_datetime': data[_STARTDATE] if _STARTDATE in data else None,
            'venue': address,
            'lat': data['latitude'] if has_coords else None,
            'lng': data['longitude'] if has_coords else None,
            'categories': ','.join(categories),
            'max_attendees': data['max_attendees'],
            'attendee_count': data['attendee_count']
        }

        if event['categories'] != 'officehours' and event['categories'] != 'emptychairtownhall':
            translated_data.append(event)
            continue
    
    return translated_data

def clean_venue(location):
    """
    We translate the venue information to a flat structure
    """
    venue = location['venue'] + '.' if 'venue' in location else None
    address = ''.join([location['address1'], location['address2']])
    locality = location['city'] if 'city' in location else None
    region = location['region'] if 'region' in location else None
    postal_code = location['postal'] if 'postal' in location else None

    return ' '.join(['' if i is None else i for i in [venue, address, locality, region, postal_code]])
