from __future__ import print_function

import abc
import json
import logging
import os
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
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor

import pika
import six

if six.PY3:
    import configparser as config_parser  # py3
else:
    import ConfigParser as config_parser  # py2

from openbaton.utils import get_action_and_vnfr, create_vnf_record, get_new_vnfc_instance, get_scripts_from_vnfp, \
    get_map, check_endpoint_type, get_nfv_message, str2bool, exec_rpc_call

from openbaton.exceptions import PyVnfmSdkException

log = logging.getLogger(__name__)

stop = False

__KNOWN_ACTIONS__ = [
    'INSTANTIATE',
    'MODIFY',
    'START',
    'STOP',
    'SCALE_OUT',
    'SCALE_IN',
    'RELEASE_RESOURCES',
    'HEAL',
    'ERROR'
]


class VnfmListener(object):
    def __init__(self, vnfm_klass, config_file_path, vnfm_init_params):
        super(VnfmListener, self).__init__()
        self.vnfm_klass = vnfm_klass
        self.vnfm_init_params = vnfm_init_params
        self.queuedel = True
        self._stop_running = False
        log.addHandler(logging.NullHandler())
        log.debug("Config file location: %s" % config_file_path)
        config = config_parser.ConfigParser()
        config.read(config_file_path)
        self.properties = get_map(section='vnfm', config=config)
        self.type = self.properties.get('type')
        if not self.type:
            raise PyVnfmSdkException("Missing type in config file!")
        username = self.properties.get("username", 'openbaton-manager-user')
        password = self.properties.get("password", 'openbaton')
        self.endpoint = self.properties.get("endpoint", self.type)
        self.endpoint_type = self.properties.get("endpoint_type", "RABBIT")
        log.debug("Configuration is: %s" % self.properties)
        logging_dir = self.properties.get('log_path', ".")

        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)

        file_handler = logging.FileHandler("{0}/{1}-vnfm.log".format(logging_dir, self.type))
        file_handler.setLevel(level=logging.DEBUG)
        log.addHandler(file_handler)

        self.heartbeat = self.properties.get("heartbeat", "60")
        self.exchange_name = self.properties.get("exchange", 'openbaton-exchange')
        self.durable = self.properties.get("exchange_durable", True)

        self.rabbit_mgmt_credentials = pika.PlainCredentials(username, password)

        self.username = None
        self.password = None
        self.rabbit_private_credentials = None
        self.threads = []
        self.executor = ThreadPoolExecutor(max_workers=100)

    def _start_listen(self):
        connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.properties.get("broker_ip"),
                                          credentials=self.rabbit_private_credentials,
                                          heartbeat_interval=int(self.heartbeat)))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)

        channel.queue_declare(queue=self.endpoint, auto_delete=self.queuedel, durable=self.durable)
        channel.queue_bind(queue=self.endpoint, exchange=self.exchange_name, routing_key=self.endpoint)
        channel.basic_consume(self._on_message, queue=self.endpoint)
        log.info("Waiting for actions")
        while channel._consumer_infos and not self._stop_running:
            channel.connection.process_data_events(time_limit=1)

    def _on_message(self, ch, method, props, body):
        vnfm = self.instantiate_vnfm()
        vnfm._setup(self.rabbit_private_credentials, self.properties, heartbeat=int(self.heartbeat))
        self.executor.submit(vnfm._execute_action, body, props.reply_to)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _set_stop(self):
        self._stop_running = True

    def register(self):
        """
        This method sends a message to the nfvo in order to register
        """

        log.info("Registering VNFM of type %s" % self.type)
        check_endpoint_type(self.endpoint_type)
        manager_endpoint = self.get_manager_endpoint()
        connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.properties.get("broker_ip"),
                                          credentials=self.rabbit_mgmt_credentials,
                                          heartbeat_interval=int(self.heartbeat)))
        response = exec_rpc_call(connection, {
            'action':              'register',
            'type':                manager_endpoint.get('type'),
            "vnfmManagerEndpoint": manager_endpoint,
        }, 'nfvo.manager.handling')
        self.username = response.get('rabbitUsername')
        self.password = response.get('rabbitPassword')
        self.rabbit_private_credentials = pika.PlainCredentials(self.username, self.password)
        log.debug("Got private temp credentials: usr:%s, pwd:%s" % (self.username, self.password))
        # self.connection.close()

    def get_manager_endpoint(self):
        manager_endpoint = dict(type=self.type,
                                endpoint=self.endpoint,
                                endpointType=self.endpoint_type,
                                description="First python vnfm",
                                enabled=True,
                                active=True)
        return manager_endpoint

    def unregister(self):
        """
        This method sends a message to the nfvo in order to unregister
        """
        connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.properties.get("broker_ip"),
                                          credentials=self.rabbit_mgmt_credentials,
                                          heartbeat_interval=int(self.heartbeat)))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        log.info("Unregistering VNFM of type %s" % self.type)
        manager_endpoint = self.get_manager_endpoint()
        channel.basic_publish(exchange='openbaton-exchange',
                              routing_key='nfvo.manager.handling',
                              properties=pika.BasicProperties(content_type='text/plain'),
                              body=json.dumps({
                                  'action':              'unregister',
                                  'type':                self.type,
                                  'username':            self.username,
                                  'password':            self.password,
                                  'vnfmManagerEndpoint': manager_endpoint
                              }))

    def start_listening(self, listeners):
        for i in range(0, listeners - 1):
            t = threading.Thread(target=self._start_listen)
            self.threads.append(t)
            t.start()
        self._start_listen()

    def instantiate_vnfm(self):
        return self.vnfm_klass(**self.vnfm_init_params)

    def stop(self):
        self._set_stop()
        for t in self.threads:
            t.join()


@six.add_metaclass(abc.ABCMeta)
class AbstractVnfm(object):
    @abc.abstractmethod
    def instantiate(self, vnf_record, scripts, vim_instances):
        """This operation allows the VNFM to instantiate the vnf_record."""
        pass

    @abc.abstractmethod
    def query(self):
        """This operation allows retrieving VNF instance state and attributes."""
        pass

    @abc.abstractmethod
    def scale_out(self, vnf_record, vnfc_instance, scripts, dependency):
        """
        This operation allows scaling out a VNF instance.
        :param vnf_record: the VNF Record to which the VNFCInstance belomgs
        :param vnfc_instance: the New VNFCInstance added
        :param scripts: the scripts if any that go along with this VNF
        :param dependency: the dependencies already satisfied
        :return: must return the VNFRecord
        """
        pass

    @abc.abstractmethod
    def scale_in(self, vnf_record, vnfc_instance):
        """
        This operation allows scaling in a VNF instance.
        :param vnf_record: the VNF Record to which the VNFCInstance belongs to
        :param vnfc_instance: the VNFCInstance to be removed
        :return: VNFRecord
        """
        pass

    @abc.abstractmethod
    def check_instantiation_feasibility(self):
        """This operation allows verifying if the VNF instantiation is possible."""
        pass

    @abc.abstractmethod
    def heal(self, vnf_record, vnfc_instance, cause):
        """
        This operation is meant to execute healing scripts on the VNFCInstance

        :param vnf_record: the VNFR to which the VNFCInstance belongs to
        :param vnfc_instance: the VNFCInstato to be healed
        :param cause: the cause of the failure
        :return: the VNFR
        """
        pass

    @abc.abstractmethod
    def update_software(self):
        """This operation allows applying a minor / limited software update(e.g.patch) to a VNF instance."""
        pass

    @abc.abstractmethod
    def modify(self, vnf_record, dependency):
        """
        This  operation allows making structural changes (e.g.configuration, topology, behavior, redundancy model)
        to a VNF instance. """
        pass

    @abc.abstractmethod
    def upgrade_software(self):
        """This operation allows deploying a new software release to a VNF instance."""
        pass

    @abc.abstractmethod
    def terminate(self, vnf_record):
        """This operation allows terminating gracefully or forcefully a previously created VNF instance."""
        pass

    @abc.abstractmethod
    def notify_change(self):
        """This operation allows providing notifications on state changes of a VNF instance, related to the VNF
        Lifecycle. """
        pass

    @abc.abstractmethod
    def start_vnfr(self, vnf_record, vnfc_instance=None):
        """
        This operation allows the VNFM to start the VNF record.

        :param vnf_record: the VNFRecord to start
        :param vnfc_instance: in case there is a start for a single VNFC this parameter will contain the VNCInstance
                to be started, if not will be None
        :return:
        """
        pass

    @abc.abstractmethod
    def stop_vnfr(self, vnf_record):
        """
        This operation allows the VNFM to stop the VNF record.
            :param vnf_record:
            :return:
            """
        pass

    @abc.abstractmethod
    def handle_error(self, vnf_record):
        """
        This operation is called when an error occurs and the VNFM needs to handle the error case in the VNFM.
        :param vnf_record
        :return:
        """
        pass

    def _execute_action(self, body, reply_to=None):
        """
        This message is in charge of dispaching the message to the right method
        :param body: the string message (in JSON format)
        :return: the NFV Message
        """

        # for python 2 and 3 compatibility
        try:
            msg = json.loads(body)
        except TypeError:
            msg = json.loads(body.decode('utf-8'))

        # log.debug("Message received: %s" % msg)

        action, virtual_network_function_record = get_action_and_vnfr(msg)
        try:
            nfv_message = None
            if action == "INSTANTIATE":
                extension = msg.get("extension")
                keys = msg.get("keys")
                log.debug("Got these keys: %s" % keys)
                vim_instances = msg.get("vimInstances")
                vnfd = msg.get("vnfd")
                vnf_package = msg.get("vnfPackage")
                vlrs = msg.get("vlrs")
                vnfdf = msg.get("vnfdf")
                scripts = None
                if vnf_package:
                    if vnf_package.get("scriptsLink") is None:
                        scripts = vnf_package.get("scripts")
                    else:
                        scripts = vnf_package.get("scriptsLink")
                virtual_network_function_record = create_vnf_record(vnfd,
                                                                    vnfdf.get("flavour_key"),
                                                                    vlrs,
                                                                    vim_instances,
                                                                    extension)
                log.debug("VNFR created is: %s" % virtual_network_function_record)

                grant_operation = self._grant_operation(virtual_network_function_record)
                virtual_network_function_record = grant_operation["virtualNetworkFunctionRecord"]
                vim_instances = grant_operation["vduVim"]

                if str2bool(self.properties.get("allocate", 'True')):
                    log.debug("Calling allocate resources")
                    try:
                        virtual_network_function_record = self._allocate_resources(
                                virtual_network_function_record,
                                vim_instances,
                                keys,
                                **extension).get("vnfr")
                        if not virtual_network_function_record:
                            return
                    except Exception as e:
                        if not isinstance(e, PyVnfmSdkException) or (isinstance(e, PyVnfmSdkException) and not e.vnfr):
                            traceback.print_exc()
                        log.error("Exception while allocating resources: %s" % e)
                        raise e

                virtual_network_function_record = self.instantiate(
                        vnf_record=virtual_network_function_record,
                        scripts=scripts,
                        vim_instances=vim_instances)

                nfv_message = get_nfv_message(action, virtual_network_function_record)

            if action == "MODIFY":
                virtual_network_function_record = self.modify(
                        vnf_record=virtual_network_function_record,
                        dependency=msg.get("vnfrd"))

                nfv_message = get_nfv_message(action, virtual_network_function_record)

            if action == "START":
                vnfc_instance = msg.get('vnfcInstance')
                virtual_network_function_record = self.start_vnfr(vnf_record=virtual_network_function_record,
                                                                  vnfc_instance=vnfc_instance)

                nfv_message = get_nfv_message(action, virtual_network_function_record)

            if action == "STOP":
                vnfc_instance = msg.get('vnfcInstance')
                virtual_network_function_record = self.stop_vnfr(vnf_record=virtual_network_function_record,
                                                                 vnfc_instance=vnfc_instance)
                nfv_message = get_nfv_message(action, virtual_network_function_record, vnfc_instance)

            if action == "ERROR":
                virtual_network_function_record = self.handle_error(vnf_record=virtual_network_function_record)
                nfv_message = None

            if action == "RELEASE_RESOURCES":
                virtual_network_function_record = self.terminate(vnf_record=virtual_network_function_record)
                nfv_message = get_nfv_message(action, virtual_network_function_record)

            if action == "HEAL":
                virtual_network_function_record = self.heal(vnf_record=virtual_network_function_record,
                                                            vnfc_instance=msg.get('vnfcInstance'),
                                                            cause=msg.get('cause'))
                nfv_message = get_nfv_message(action, virtual_network_function_record)

            if action == 'SCALE_OUT':
                component = msg.get('component')
                vnf_package = msg.get('vnfPackage')
                scripts = get_scripts_from_vnfp(vnf_package)
                dependency = msg.get('dependency')
                mode = msg.get('mode')
                extension = msg.get('extension')
                new_vnfc_instance = None
                if str2bool(self.properties.get("allocate", 'True')):
                    scaling_message = get_nfv_message('SCALING',
                                                      virtual_network_function_record,
                                                      user_data=self.get_user_data())
                    log.debug('The NFVO allocates resources. Send SCALING message.')
                    result = exec_rpc_call(self.connection, json.dumps(scaling_message), "vnfm.nfvo.actions.reply")
                    log.debug('Received {} message.'.format(result.get('action')))
                    virtual_network_function_record = result.get('vnfr')
                    new_vnfc_instance = get_new_vnfc_instance(virtual_network_function_record, component)
                    if mode is not None and mode.lower() == "standby":
                        new_vnfc_instance['state'] = "STANDBY"

                virtual_network_function_record = self.scale_out(
                        virtual_network_function_record,
                        new_vnfc_instance or component,
                        scripts,
                        dependency)

                if new_vnfc_instance is None:
                    new_vnfc_instance = get_new_vnfc_instance(virtual_network_function_record, component)

                nfv_message = get_nfv_message('SCALED', virtual_network_function_record, new_vnfc_instance)

            if action == 'SCALE_IN':
                virtual_network_function_record = self.scale_in(virtual_network_function_record,
                                                                msg.get('vnfcInstance'))
                nfv_message = None

            if action not in __KNOWN_ACTIONS__:
                raise PyVnfmSdkException("Unknown action!")

        except Exception as exception:
            traceback.print_exc()
            if isinstance(exception, PyVnfmSdkException) and exception.vnfr:
                virtual_network_function_record = exception.vnfr
            nfv_message = get_nfv_message('ERROR', virtual_network_function_record, exception=exception)
        self._handle_answer(nfv_message, reply_to)

    def _handle_answer(self, nfv_message, reply_to=None):
        ch = self.connection.channel()
        ch.basic_qos(prefetch_count=1)
        if nfv_message:
            if reply_to:
                ch.basic_publish(exchange='',
                                 routing_key=reply_to,
                                 properties=pika.BasicProperties(content_type='text/plain'),
                                 body=json.dumps(nfv_message))
            elif nfv_message.get("action") == "INSTANTIATE":
                ch.basic_publish(exchange='',
                                 routing_key="vnfm.nfvo.actions.reply",
                                 properties=pika.BasicProperties(content_type='text/plain'),
                                 body=json.dumps(nfv_message))
            else:
                ch.basic_publish(exchange='',
                                 routing_key="vnfm.nfvo.actions",
                                 properties=pika.BasicProperties(content_type='text/plain'),
                                 body=json.dumps(nfv_message))
            log.info("Answer sent")

    def _grant_operation(self, vnf_record):
        nfv_message = get_nfv_message("GRANT_OPERATION", vnf_record)
        log.info("Executing GRANT_OPERATION")
        result = exec_rpc_call(self.connection, json.dumps(nfv_message), "vnfm.nfvo.actions.reply")
        log.debug("grant_allowed: %s" % result.get("grantAllowed"))
        log.debug("vdu_vims: %s" % result.get("vduVim").keys())
        log.debug("vnf_record: %s" % result.get("virtualNetworkFunctionRecord").get("name"))

        return result

    def _allocate_resources(self, vnf_record, vim_instances, keys, **kwargs):
        user_data = self.get_user_data()
        if user_data is not None:
            monitoring_ip = kwargs.get("monitoringIp")
            log.debug("monitoring ip is: %s" % monitoring_ip)
            user_data = user_data.replace("export MONITORING_IP=", "export MONITORING_IP=%s" % monitoring_ip)
            log.debug("Sending userdata: \n%s" % user_data)

        nfv_message = get_nfv_message(action="ALLOCATE_RESOURCES", vnfr=vnf_record, vim_instances=vim_instances,
                                      user_data=user_data, keys=keys)
        log.debug("Executing ALLOCATE_RESOURCES")
        result = exec_rpc_call(self.connection, json.dumps(nfv_message), "vnfm.nfvo.actions.reply")
        if not result:
            raise PyVnfmSdkException("Got empty message from nfvo while allocating resource!")
        elif result.get('action') == 'ERROR':
            raise PyVnfmSdkException("Not able to allocate Resources because: %s" % result.get('message'),
                                     vnfr=result.get('vnfr'))
        else:
            log.debug("vnf_record: %s has allocated resources" % result.get("vnfr").get("name"))
            return result

    def get_user_data(self):
        """
        Looks for the user data content in the 'user_data' configuration property or if any,
        in the '/etc/openbaton/<type>/userdata.sh' file. if not found returns None :return: the userdata content or
        None.
        :return the userdata content or None if the file was not found
        """
        userdata_path = self.properties.get("userdata_path",
                                            "/etc/openbaton/%s/userdata.sh" % self.properties.get('type'))
        if os.path.isfile(userdata_path):
            with open(userdata_path, "r") as f:
                return f.read()
        else:
            return None

    def _setup(self, creds, properties, heartbeat=60):
        self.properties = properties
        self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.properties.get("broker_ip"),
                                          credentials=creds,
                                          heartbeat_interval=heartbeat))


def start_vnfm_instances(vnfm_klass, config_file_path, instances=1, **kwargs):
    """
    This utility method start :instances number of thread of class vnfm_klass.

    :param vnfm_klass: the Class of the VNFM
    :param config_file_path: the config file path for the vnfm
    :param instances: the number of instances
    :param kwargs: the instantiation arguments for the vnfm class

    """

    log.debug("VNFM Class: %s" % vnfm_klass)
    l = VnfmListener(vnfm_klass, config_file_path, kwargs)
    l.register()
    try:
        l.start_listening(instances)
    except KeyboardInterrupt:
        log.info("Ctrl-C pressed. Exiting")
        l.stop()
        l.unregister()
        sys.exit(0)
