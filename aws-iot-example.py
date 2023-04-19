from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import time
import asyncio
import json
import random
import argparse
import sys
from os.path import exists
from utils.config_loader import Config


def get_mac_address():
    """Get MAC address for unique ID of the device
    """
    with open("/sys/class/net/eth0/address") as file:
        mac = file.read()
        mac = mac.replace(":", "").replace("\n","")
        return mac

def get_cpu_temp():
    """Get CPU temperature of the device
    """
    with open("/sys/class/thermal/thermal_zone0/temp") as file:
        temp = file.read()
        temp = temp.replace("\n","")
        temperature = float(float(temp)/1000)
        return temperature


def on_connection_interrupted(connection, error, **kwargs):
    """Callback function for when the connection is interrupted
    """
    print('Connection interrupted with error {}'.format(error))


def on_connection_resumed(connection, return_code, session_present, **kwargs):
    """Callback function for when the connection is resumed
    """
    print('Connection resumed with return code {}, session present {}'.format(return_code, session_present))

def on_msg_callback(topic, payload, **kwargs):
    """Callback function for when the the message from the cloud is reached
    """
    print("Received message from topic '{}': {}".format(topic, payload))


def main():
    """Main function
    """

    # Parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default='/home/pi/.aws/config.ini')
    args = parser.parse_args()


    # Get config file path from arguments
    CONFIG_PATH = args.config

    # Check if the config file exists 
    if exists(CONFIG_PATH) == False:
        sys.exit("Configuration file {} does not exist. Aborting..".format(CONFIG_PATH))

    # Parse the configuration file and get the configs from the file
    config = Config(CONFIG_PATH)
    config_parameters = config.get_section('SETTINGS')
    secure_cert_path = config_parameters['PROD_CERT']
    secure_key_path = config_parameters['PROD_KEY']
    root_ca_path = config_parameters['ROOT_CERT']
    endpoint_url = config_parameters['IOT_ENDPOINT']
    cert_folder = config_parameters['SECURE_CERT_PATH']

    # Get MAC address from the device
    mac_address = get_mac_address()

    # Connect to the AWS IoT Core
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    MQTTClient = mqtt_connection_builder.mtls_from_path(
                    endpoint=endpoint_url,
                    cert_filepath="{}/{}".format(cert_folder, secure_cert_path),
                    pri_key_filepath="{}/{}".format(cert_folder, secure_key_path),
                    client_bootstrap=client_bootstrap,
                    ca_filepath="{}/{}".format(cert_folder, root_ca_path),
                    on_connection_interrupted=on_connection_interrupted,
                    on_connection_resumed=on_connection_resumed,
                    client_id=mac_address,
                    clean_session=False,
                    keep_alive_secs=6)

    connect_future = MQTTClient.connect()
    connect_future.result()
    print("Connected!")

    # Subscribe to a topic
    mqtt_topic1_subscribe_future, _ = MQTTClient.subscribe(
            topic="iot/data",
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_msg_callback)

    # Wait for subscription to succeed
    mqtt_topic1_subscribe_result = mqtt_topic1_subscribe_future.result()
    print("Subscribed with {}".format(str(mqtt_topic1_subscribe_result['qos'])))


    # Subscribe to the cloud to device topic
    mqtt_topic2_subscribe_future, _ = MQTTClient.subscribe(
            topic="iot/device/requests",
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_msg_callback)

    # Wait for subscription to succeed
    mqtt_topic2_subscribe_result = mqtt_topic2_subscribe_future.result()
    print("Subscribed with {}".format(str(mqtt_topic2_subscribe_result['qos'])))

    # Publish random values to the MQTT topic
    while True:
        value = random.randint(0, 100)
        temp_value = get_cpu_temp()
        payload = {"pressure": value, "temperature": temp_value}

        MQTTClient.publish(
                topic="iot/data",
                payload=json.dumps(payload),
                qos=mqtt.QoS.AT_LEAST_ONCE)
        time.sleep(1)

if __name__ == "__main__":
    main()
