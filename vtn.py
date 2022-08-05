import asyncio
from datetime import datetime, timezone, timedelta
from openleadr.objects import Target, Interval
from openleadr import OpenADRServer, enable_default_logging
from functools import partial
from openleadr.utils import generate_id
from mysql.connector import connect, Error, cursor

try:
    connection = connect(user='root', password='',
                         host='localhost', database='vtn_db')
except Error:
    print(Error)
else:
    cursor = connection.cursor()
    print("Connection succeeded!")

enable_default_logging()


async def on_create_party_registration(registration_info):
    """
    Inspect the registration info and return a ven_id and registration_id.
    """
    ven_name = registration_info['ven_name']
    query = """SELECT COUNT(*) FROM vens WHERE ven_name=%s"""
    cursor.execute(query, (ven_name,))

    result = [item[0] for item in cursor.fetchall()]

    if result[0] > 0:
        ven_id = generate_id()
        registration_id = generate_id()

        insert_id_query = """UPDATE vens SET ven_id=%s, registration_id=%s WHERE ven_name=%s"""
        val = (ven_id, registration_id, ven_name)
        cursor.execute(insert_id_query, val)

        connection.commit()

        print(cursor.rowcount, "record(s) affected")
        return ven_id, registration_id
    else:
        return False


async def on_register_report(ven_id, resource_id, measurement, unit, scale,
                             min_sampling_interval, max_sampling_interval):
    """
    Inspect a report offering from the VEN and return a callback and sampling interval for receiving the reports.
    """
    callback = partial(on_update_report, ven_id=ven_id, resource_id=resource_id, measurement=measurement)
    sampling_interval = min_sampling_interval
    return callback, sampling_interval


async def on_update_report(data, ven_id, resource_id, measurement):
    """
    Callback that receives report data from the VEN and handles it.
    """
    for time, value in data:
        print(f"Ven {ven_id} reported {measurement} = {value} at time {time} for resource {resource_id}")


def lookup_fingerprint(ven_id):
    query_ven_info = """SELECT * FROM vens WHERE ven_id=%s"""
    cursor.execute(query_ven_info, (ven_id, ))
    results = cursor.fetchall()

    for row in results:
        id_ven = row[0]
        ven_name = row[1]
        registration_id = row[2]
        fingerprint = row[3]

    if ven_id == id_ven:
        return {'ven_id': id_ven,
                'ven_name': ven_name,
                'registration_id': registration_id,
                'fingerprint': fingerprint}


async def event_response_callback(ven_id, event_id, opt_type):
    """
     Callback that receives the response from a VEN to an Event.
     """
    print(f"VEN {ven_id} responded to Event {event_id} with: {opt_type}")


# Create the server object
server = OpenADRServer(vtn_id='myvtn',
                       cert='./certificates/vtn.crt',
                       key='./certificates/vtn.key',
                       http_cert='./certificates/vtn.crt',
                       http_key='./certificates/vtn.key',
                       http_ca_file='./certificates/CA.crt',
                       ven_lookup=lookup_fingerprint)

# Add the handler for client (VEN) registrations
server.add_handler('on_create_party_registration', on_create_party_registration)

# Add the handler for report registrations from the VEN
server.add_handler('on_register_report', on_register_report)

# Add a prepared event for a VEN that will be picked up when it polls for new messages
server.add_event(ven_id='ven_id_123',
                 signal_name='simple',
                 signal_type='level',
                 intervals=[{'dtstart': datetime(2021, 10, 8, 0, 25, 0, tzinfo=timezone.utc),
                             'duration': timedelta(minutes=10),
                             'signal_payload': 1}],
                 callback=event_response_callback)

# Run the server on the asyncio event loop
loop = asyncio.get_event_loop()
loop.create_task(server.run_async())
loop.run_forever()