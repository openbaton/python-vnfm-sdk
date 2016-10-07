# Copyright (c) 2015 Fraunhofer FOKUS. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import ConfigParser
import json
import logging
import threading

import abc
import os
import pika
from interfaces.exceptions import PyVnfmSdkException
from utils.Utilities import get_map

__author__ = 'lto'

log = logging.getLogger(__name__)

# TODO improve this
ENDPOINT_TYPES = ["RABBIT", "REST"]


class ManagerEndpoint(object):
    def __init__(self, type, endpoint, endpoint_type, description=None, enabled=True, active=True):
        self.type = type
        self.endpoint = endpoint
        self.endpoint_type = endpoint_type
        self.description = description
        self.enabled = enabled
        self.active = active

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    pass


def check_endpoint_type(endpoint_type):
    if endpoint_type not in ENDPOINT_TYPES:
        raise PyVnfmSdkException("The endpoint type must be in %s" % ENDPOINT_TYPES)


class AbstractVnfm(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def instantiate(self, virtualNetworkFunctionRecord, scripts, vimInstances):
        pass

    @abc.abstractmethod
    def query(self):
        """This operation allows retrieving VNF instance state and attributes."""
        pass

    @abc.abstractmethod
    def scale(self, scaleOut, virtualNetworkFunctionRecord, component, scripts, dependency):
        """This operation allows scaling (out / in, up / down) a VNF instance."""
        pass

    @abc.abstractmethod
    def checkInstantiationFeasibility(self):
        """This operation allows verifying if the VNF instantiation is possible."""
        pass

    @abc.abstractmethod
    def heal(self, virtualNetworkFunctionRecord, vnfcInstanceComponent, cause):
        pass

    @abc.abstractmethod
    def updateSoftware(self):
        """This operation allows applying a minor / limited software update(e.g.patch) to a VNF instance."""
        pass

    @abc.abstractmethod
    def modify(self, virtualNetworkFunctionRecord, dependency):
        """This  operation allows making structural changes (e.g.configuration, topology, behavior, redundancy model) to a VNF instance."""
        pass

    @abc.abstractmethod
    def upgradeSoftware(self):
        """This operation allows deploying a new software release to a VNF instance."""
        pass

    @abc.abstractmethod
    def terminate(self, virtualNetworkFunctionRecord):
        """This operation allows terminating gracefully or forcefully a previously created VNF instance."""
        pass

    @abc.abstractmethod
    def notifyChange(self):
        """This operation allows providing notifications on state changes of a VNF instance, related to the VNF Lifecycle."""
        pass

    def on_message(self, **kwargs):
        """This message is in charge of dispaching the message to the right method"""
        return ""

    def on_request(self, ch, method, props, body):
        n = body
        response = self.on_message(body)
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id= \
                                                             props.correlation_id, content_type='text/plain'),
                         body=str(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("Answer sent")

    def thread_function(self, ch, method, properties, body):
        threading.Thread(target=self.on_request, args=(ch, method, properties, body)).start()

    def __init__(self, type):
        self.type = type

    def run(self):
        config_file_name = "/etc/openbaton/%s/conf.ini" % self.type  # understand if it works
        log.debug("Config file location: %s" % config_file_name)
        config = ConfigParser.ConfigParser()
        config.read(config_file_name)  # read config file
        _map = get_map(section='vnfm', config=config)  # get the data from map
        log.debug("Map is: %s" % _map)
        logging_dir = _map.get('log_path')

        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)

            logging.basicConfig(filename=logging_dir + '/%s-vnfm.log' % _map.get('type'), level=logging.INFO)

        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)

        logging.basicConfig(filename=logging_dir + '/ems-receiver.log', level=logging.INFO)
        username = _map.get("username")
        password = _map.get("password")
        autodel = _map.get("autodelete")
        heartbeat = _map.get("heartbeat")
        exchange_name = _map.get("exchange")
        queuedel = True
        if autodel == 'false':
            queuedel = False
        if not heartbeat:
            heartbeat = '60'
        if not exchange_name:
            exchange_name = 'openbaton-exchange'

        rabbit_credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=_map.get("broker_ip"), credentials=rabbit_credentials,
                                      heartbeat_interval=int(heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.exchange_declare(exchange=exchange_name, type="topic", durable=True)

        channel.queue_declare(queue='vnfm.nfvo.actions', auto_delete=queuedel, durable=True)
        channel.queue_declare(queue='vnfm.nfvo.actions.reply', auto_delete=queuedel, durable=True)
        channel.queue_declare(queue='nfvo.%s.actions' % self.type, auto_delete=queuedel, durable=True)

        self.register(_map, channel, self.type)

        channel.basic_consume(self.thread_function, queue='nfvo.%s.actions' % self.type)

        log.info("Waiting for actions")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            self.unregister(_map, channel, self.type)

    def register(self, _map, channel, type):
        # Registration
        log.info("Registering VNFM of type %s" % type)
        endpoint_type = _map.get("endpoint_type")
        log.debug("Got endpoint type: %s" % endpoint_type)
        check_endpoint_type(endpoint_type)
        manager_endpoint = ManagerEndpoint(type=type, endpoint="nfvo.%s.actions" % type, endpoint_type=endpoint_type,
                                           description="First python vnfm")
        log.debug("Sending endpoint type: " + manager_endpoint.toJSON())
        channel.basic_publish(exchange='', routing_key='nfvo.vnfm.register',
                              properties=pika.BasicProperties(content_type='text/plain'),
                              body=manager_endpoint.toJSON())

    def unregister(self, _map, channel, type):
        # UnRegistration
        log.info("Unregistering VNFM of type %s" % type)
        endpoint_type = _map.get("endpoint_type")
        check_endpoint_type(endpoint_type)
        manager_endpoint = ManagerEndpoint(type=type, endpoint="nfvo.%s.actions" % type, endpoint_type=endpoint_type,
                                           description="First python vnfm")
        log.debug("Sending endpoint type: " + manager_endpoint.toJSON())
        channel.basic_publish(exchange='', routing_key='nfvo.vnfm.unregister',
                              properties=pika.BasicProperties(content_type='text/plain'),
                              body=manager_endpoint.toJSON())
