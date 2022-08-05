import asyncio
import os
from datetime import timedelta
from functools import partial
from openleadr import OpenADRClient
from mysql.connector import connect, Error, cursor
import paho.mqtt.client as mqtt_client

# OpenleADR certs
CA_CERT = os.path.join(os.path.dirname(os.path.dirname(__file__)), '/Users/larissasoutodelrio/Documents/mestrado/openleadr_aws/certificates', 'CA.crt')
VEN_CERT = os.path.join(os.path.dirname(os.path.dirname(__file__)), '/Users/larissasoutodelrio/Documents/mestrado/openleadr_aws/certificates', 'ven.crt')
VEN_KEY = os.path.join(os.path.dirname(os.path.dirname(__file__)), '/Users/larissasoutodelrio/Documents/mestrado/openleadr_aws/certificates', 'ven.key')

#MYSQL config
con = connect(host='localhost', database='control_algorithm', user='root', password='')
if con.is_connected():
    db_info = con.get_server_info()
    print("Conectado ao servidor MySQL versão ", db_info)
    cursor = con.cursor()
    cursor.execute("select database();")
    linha = cursor.fetchone()
    print("Conectado ao banco de dados ", linha)
else:
   print("Erro na conexao")

# MQTT config
broker = 'lsinfo.tech'
port = 1883
topic = "#"
client_id = 'ven'

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client_mqtt = mqtt_client.Client(client_id)
    client_mqtt.on_connect = on_connect
    client_mqtt.connect(broker, port)
    return client_mqtt

client_mqtt = connect_mqtt()

def handle_publish(topic, payload):
    try:
        client_mqtt.publish(topic, payload)
        print('Published data on', topic)

    except Exception as e:
        print("[ERROR] Could not publish data, error: {}".format(e))


async def read_current_power():
    query = "SELECT current_power FROM consumption ORDER BY id DESC"
    cursor.execute(query)
    results = cursor.fetchall()

    for result in results:
        return result[0]

async def handle_event(event):
    signal = event['event_signals'][0]
    intervals = signal['intervals']
    message = str(intervals[0]['dtstart']) + " " + str(intervals[0]['signal_payload'])
    print(message)

    handle_publish('loads/signal', message)

    #checar se há apenas cargas prioritárias (zero) ligadas e recusar o evento
    query = """SELECT COUNT(*) FROM `status` WHERE `state`=1 AND `priority`>0"""
    cursor.execute(query)
    results = cursor.fetchall()

    for result in results:
        print(result[0])
        if result[0] > 0:
            return 'optOut'

    return 'optIn'


def control_device(status, time):
    print(status, time)

client = OpenADRClient(ven_name='ven_cloud',
                       vtn_url='https://140.238.187.16:8000/myvtn.com.br',
                       cert=VEN_CERT,
                       key=VEN_KEY,
                       ca_file=CA_CERT,
                       vtn_fingerprint='F6:79:38:1B:19:85:D2:8A:C5:77')

client.add_report(callback=partial(read_current_power),
                  report_specifier_id='VoltageReport',
                  resource_id='device123',
                  measurement='Voltage',
                  unit='V',
                  sampling_rate=timedelta(seconds=60))

client.add_handler('on_event', handle_event)

loop = asyncio.get_event_loop()
loop.create_task(client.run())
loop.run_forever()