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

try:
    import configparser as config_parser  # py3
except ImportError:
    import ConfigParser as config_parser  # py2

import abc
import copy
import json
import logging
import operator
import os
import threading
import uuid

import pika

try:
    # py 2
    from utils.Utilities \
        import get_map, get_nfv_message, check_endpoint_type, ManagerEndpoint, str2bool
except ImportError:
    # py 3
    from .utils.Utilities \
        import get_map, get_nfv_message, check_endpoint_type, ManagerEndpoint, str2bool

from vnfm.sdk.exceptions import PyVnfmSdkException

__author__ = 'lto'

log = logging.getLogger("org.openbaton.python.vnfm.sdk.%s" % __name__)

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


class AbstractVnfm(threading.Thread):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def instantiate(self, vnf_record, scripts, vim_instances, package=None):
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
        """This  operation allows making structural changes (e.g.configuration, topology, behavior, redundancy model) to a VNF instance."""
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
    def notifyChange(self):
        """This operation allows providing notifications on state changes of a VNF instance, related to the VNF Lifecycle."""
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

    @staticmethod
    def __get_action_and_vnfr__(msg):
        action = msg.get("action")
        log.debug("Action is %s" % action)
        virtual_network_function_record = msg.get('virtualNetworkFunctionRecord')
        if not virtual_network_function_record:
            virtual_network_function_record = msg.get('vnfr')

        # if not virtual_network_function_record:
        #     raise PyVnfmSdkException("No VNFR sent from the NFVO! i dunno what to do!:(")

        return action, virtual_network_function_record

    @staticmethod
    def create_vnf_record(vnfd, flavor_key, vlrs, vim_instances, extension):
        """
        This method provides a general implementation of the process for creating the VNFRecord starting from the VNFD
        and some parameters.

        :param vnfd: the VNFDescriptor from which to create the VNFRecord
        :param flavor_key: the flavor key name to be used
        :param vlrs: the list of Virtual Links
        :param vim_instances: the dict containing list of VimInstances per VDU
        :param extension: some key-value extensions

        :return: the VNFR ready to be instantiated
        """

        log.debug("Requires is: %s" % vnfd.get("requires"))
        log.debug("Provides is: %s" % vnfd.get("provides"))

        vnfr = dict(lifecycle_event_history=[],
                    parent_ns_id=extension.get("nsr-id"),
                    name=vnfd.get("name"),
                    type=vnfd.get("type"),
                    requires=vnfd.get("requires"),
                    provides=dict(),
                    endpoint=vnfd.get("endpoint"),
                    packageId=vnfd.get("vnfPackageLocation"),
                    monitoring_parameter=vnfd.get("monitoring_parameter"),
                    auto_scale_policy=vnfd.get("auto_scale_policy"),
                    cyclicDependency=vnfd.get("cyclicDependency"),
                    configurations=vnfd.get("configurations"),
                    vdu=vnfd.get("vdu"),
                    version=vnfd.get("version"),
                    connection_point=vnfd.get("connection_point"),
                    deployment_flavour_key=flavor_key,
                    vnf_address=[],
                    status="NULL",
                    descriptor_reference=vnfd.get("id"),
                    lifecycle_event=vnfd.get("lifecycle_event"),
                    virtual_link=vnfd.get("virtual_link"))

        if vnfr.get("requires") is not None:
            if vnfr.get("requires").get("configurationParameters") is None:
                vnfr["requires"]["configurationParameters"] = []
        if vnfr.get("provides") is not None:
            if vnfr.get("provides").get("configurationParameters") is None:
                vnfr["provides"]["configurationParameters"] = []

        vnfr['vdu'] = []
        vnfd_vdus = vnfd.get('vdu')
        for vnfd_vdu in vnfd_vdus:
            vdu_new = copy.deepcopy(vnfd_vdu)
            vdu_new['parent_vdu'] = vnfd_vdu.get('id')
            vdu_new['id'] = str(uuid.uuid4())
            passed_vim_instances = vim_instances.get(vnfd_vdu.get('id'))
            if not passed_vim_instances:
                passed_vim_instances = vim_instances.get(vnfd_vdu.get('name'))
            vim_instance_names = []
            for vi in passed_vim_instances:
                vim_instance_names.append(vi.get('name'))
            vdu_new['vimInstanceName'] = vim_instance_names
            vnfr.get('vdu').append(vdu_new)

        for vlr in vlrs:
            for internal_vlr in vnfr["virtual_link"]:
                if vlr.get("name") == internal_vlr.get("name"):
                    internal_vlr["exId"] = vlr.get("extId")

        return vnfr

    def __on_message__(self, body):
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

        log.debug("Message received: %s" % msg)

        try:
            action, virtual_network_function_record = self.__get_action_and_vnfr__(msg)
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
                virtual_network_function_record = self.create_vnf_record(vnfd,
                                                                         vnfdf.get("flavour_key"),
                                                                         vlrs,
                                                                         vim_instances,
                                                                         extension)
                log.debug("VNFR created is: %s" % virtual_network_function_record)

                grant_operation = self.__grant_operation__(virtual_network_function_record)
                virtual_network_function_record = grant_operation["virtualNetworkFunctionRecord"]
                vim_instances = grant_operation["vduVim"]

                if str2bool(self.properties.get("allocate", 'True')):
                    virtual_network_function_record = self.__allocate_resources__(
                        virtual_network_function_record,
                        vnf_package,
                        vim_instances,
                        keys,
                        **extension).get("vnfr")

                virtual_network_function_record = self.instantiate(
                    vnf_record=virtual_network_function_record,
                    scripts=scripts,
                    vim_instances=vim_instances,
                    package=vnf_package)

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
                scripts = self.__get_scripts_from_vnfp__(vnf_package)
                dependency = msg.get('dependency')
                mode = msg.get('mode')
                extension = msg.get('extension')
                new_vnfc_instance = None
                if str2bool(self.properties.get("allocate", 'True')):
                    scaling_message = get_nfv_message('SCALING',
                                                      virtual_network_function_record,
                                                      user_data=self.get_user_data())
                    log.debug('The NFVO allocates resources. Send SCALING message.')
                    result = self.__exec_rpc_call__(json.dumps(scaling_message))
                    log.debug('Received {} message.'.format(result.get('action')))
                    virtual_network_function_record = result.get('vnfr')
                    new_vnfc_instance = self.__get_new_vnfc_instance__(virtual_network_function_record, component)
                    if mode is not None and mode.lower() == "standby":
                        new_vnfc_instance['state'] = "STANDBY"

                virtual_network_function_record = self.scale_out(
                    virtual_network_function_record,
                    new_vnfc_instance or component,
                    scripts,
                    dependency)

                if new_vnfc_instance is None:
                    new_vnfc_instance = self.__get_new_vnfc_instance__(virtual_network_function_record, component)

                nfv_message = get_nfv_message('SCALED', virtual_network_function_record, new_vnfc_instance)

            if action == 'SCALE_IN':
                virtual_network_function_record = self.scale_in(virtual_network_function_record,
                                                                msg.get('vnfcInstance'))
                nfv_message = None

            if action not in __KNOWN_ACTIONS__:
                raise PyVnfmSdkException("Unknown action!")

            return nfv_message

        except PyVnfmSdkException as exception:
            nfv_message = get_nfv_message('ERROR', virtual_network_function_record, exception=exception)
            return nfv_message

    @staticmethod
    def __get_scripts_from_vnfp__(vnf_package):
        if vnf_package:
            scripts = vnf_package.get("scripts")
            if not scripts:
                scripts = vnf_package.get('scriptsLink')
        else:
            scripts = {}
        return scripts

    @staticmethod
    def __get_new_vnfc_instance__(virtual_network_function_record, component):
        for vdu in virtual_network_function_record.get('vdu'):
            for vnfc_instance in vdu.get('vnfc_instance'):
                if vnfc_instance.get('vnfComponent').get('id') == component.get('id'):
                    return vnfc_instance
        raise PyVnfmSdkException('New VNFCInstance not found when scaling out!')

    def __on_request__(self, ch, method, props, body):
        log.info("Waiting for actions")
        response = self.__on_message__(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if response:
            if response.get("action") == "INSTANTIATE":
                ch.basic_publish(exchange='',
                                 routing_key="vnfm.nfvo.actions.reply",
                                 properties=pika.BasicProperties(content_type='text/plain'),
                                 body=json.dumps(response))
            else:
                ch.basic_publish(exchange='',
                                 routing_key="vnfm.nfvo.actions",
                                 properties=pika.BasicProperties(content_type='text/plain'),
                                 body=json.dumps(response))
            log.info("Answer sent")

    def __thread_function__(self, ch, method, properties, body):
        log.info("here")
        threading.Thread(target=self.__on_request__, args=(ch, method, properties, body)).start()

    def __init__(self, _type, config_file):
        super(AbstractVnfm, self).__init__()

        self.queuedel = True
        self._stop_running = False
        log.addHandler(logging.NullHandler())
        self.type = _type

        # Configuration file initialisation
        log.debug("Config file location: %s" % config_file)
        config = config_parser.ConfigParser()
        config.read(config_file)
        self.properties = get_map(section='vnfm', config=config)
        guest_username = self.properties.get("username")
        guest_password = self.properties.get("password")

        log.debug("Configuration is: %s" % self.properties)
        logging_dir = self.properties.get('log_path')

        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)

        file_handler = logging.FileHandler("{0}/{1}-vnfm.log".format(logging_dir, self.type))
        file_handler.setLevel(level=logging.DEBUG)
        log.addHandler(file_handler)

        self.heartbeat = self.properties.get("heartbeat", "60")
        self.exchange_name = self.properties.get("exchange", 'openbaton-exchange')
        self.durable = self.properties.get("exchange_durable", True)
        self.guest_rabbit_credentials = pika.PlainCredentials(guest_username, guest_password)

        self.manager_endpoint = ManagerEndpoint(type=self.type,
                                                endpoint=self.type,
                                                endpointType='RABBIT',
                                                description="First python vnfm")
        _body = {
            "type": self.type,
            "action": "register",
            "vnfmManagerEndpoint": self.manager_endpoint.toJSON()
        }
        self.corr_id = str(uuid.uuid4())
        self.response = None
        log.info("Registering VNFM of type %s" % self.type)
        ## First register to the service agent queue

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.properties.get("broker_ip"),
                                      credentials=self.guest_rabbit_credentials))

        self.channel = self.connection.channel()
        # self.channel.basic_qos(prefetch_count=1)
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(self._on_response, no_ack=True,
                                   queue=self.callback_queue)

        body = json.dumps(_body)
        log.debug("Sending body: %s" % body)
        self.channel.basic_publish(exchange='openbaton-exchange', routing_key='nfvo.manager.handling',
                                   properties=pika.BasicProperties(reply_to=self.callback_queue,
                                                                   correlation_id=self.corr_id),
                                   body=str(body))
        while self.response is None:
            self.connection.process_data_events()
        self.connection.close()

    def _on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            log.debug("Received :%s" % type(body))
            self.response = json.loads(body.decode("utf-8"))
            self.rabbit_credentials = pika.PlainCredentials(self.response.get("rabbitUsername"), self.response.get('rabbitPassword'))

    def run(self):
        log.debug("Connecting to %s" % self.properties.get("broker_ip"))
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.properties.get("broker_ip"), credentials=self.rabbit_credentials,
                                      heartbeat_interval=int(self.heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        # channel.exchange_declare(exchange=self.exchange_name, type="topic", durable=self.durable)

        # channel.queue_declare(queue='vnfm.nfvo.actions', auto_delete=self.queuedel, durable=self.durable)
        # channel.queue_declare(queue='vnfm.nfvo.actions.reply', auto_delete=self.queuedel, durable=self.durable)
        channel.queue_declare(queue='%s' % self.type, auto_delete=self.queuedel, durable=self.durable)
        channel.basic_consume(self.__thread_function__, queue='%s' % self.type)
        log.info("Waiting for actions")
        while channel._consumer_infos and not self._stop_running:
            channel.connection.process_data_events(time_limit=1)

    def _set_stop(self):
        self._stop_running = True

    def __grant_operation__(self, vnf_record):
        nfv_message = get_nfv_message("GRANT_OPERATION", vnf_record)
        log.info("Executing GRANT_OPERATION")
        result = self.__exec_rpc_call__(json.dumps(nfv_message))
        log.debug("grant_allowed: %s" % result.get("grantAllowed"))
        log.debug("vdu_vims: %s" % result.get("vduVim").keys())
        log.debug("vnf_record: %s" % result.get("virtualNetworkFunctionRecord").get("name"))

        return result

    def __allocate_resources__(self, vnf_record, vnf_package, vim_instances, keys, **kwargs):
        user_data = self.get_user_data()
        if user_data is not None:
            monitoring_ip = kwargs.get("monitoringIp")
            log.debug("monitoring ip is: %s" % monitoring_ip)
            user_data = user_data.replace("export MONITORING_IP=", "export MONITORING_IP=%s" % monitoring_ip)
            log.debug("Sending userdata: \n%s" % user_data)
        nfv_message = get_nfv_message(action="ALLOCATE_RESOURCES", vnfr=vnf_record, vim_instances=vim_instances,
                                      user_data=user_data, keys=keys)
        log.debug("Executing ALLOCATE_RESOURCES")
        result = self.__exec_rpc_call__(json.dumps(nfv_message))
        log.debug("vnf_record: %s has allocated resources" % result.get("vnfr").get("name"))
        return result

    def __exec_rpc_call__(self, nfv_message, queue="vnfm.nfvo.actions.reply"):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.properties.get("broker_ip"),
                                                                       credentials=self.rabbit_credentials,
                                                                       heartbeat_interval=int(self.heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel = connection.channel()
        log.debug("Channel created")
        result = channel.queue_declare(exclusive=True)
        callback_queue = result.method.queue
        response = {}
        channel.basic_consume(lambda ch, method, properties, body: operator.setitem(response, "result", body),
                              no_ack=True,
                              queue=callback_queue)
        log.debug("Callback Queue is: %s" % callback_queue)
        log.debug("Sending to %s" % queue)
        corr_id = str(uuid.uuid4())
        channel.basic_publish(exchange="",
                              routing_key="vnfm.nfvo.actions.reply",
                              properties=pika.BasicProperties(reply_to=callback_queue,
                                                              correlation_id=corr_id,
                                                              content_type='text/plain'),
                              body=nfv_message)
        while len(response) == 0:
            connection.process_data_events()

        channel.queue_delete(queue=callback_queue)

        return json.loads(response["result"].decode('utf-8'))

    def get_user_data(self):
        """
        Looks for the user data content in the 'user_data' configuration property or if any,
        in the '/etc/openbaton/<type>/userdata.sh' file. if not found returns None :return: the userdata content or
        None.
        :return the userdata content or None if the file was not found
        """
        userdata_path = self.properties.get("userdata_path", "/etc/openbaton/%s/userdata.sh" % self.type)
        if os.path.isfile(userdata_path):
            with open(userdata_path, "r") as f:
                return f.read()
        else:
            return None

    def _register(self):
        """
        This method sends a message to the nfvo in order to register
        """
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.properties.get("broker_ip"), credentials=self.rabbit_credentials,
                                      heartbeat_interval=int(self.heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)

        log.debug("Sending endpoint type: " + self.manager_endpoint.toJSON())
        channel.basic_publish(exchange='', routing_key='nfvo.vnfm.register',
                              properties=pika.BasicProperties(content_type='text/plain'),
                              body=self.manager_endpoint.toJSON())

    def _unregister(self):
        """
        This method sends a message to the nfvo in order to unregister
        """
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.properties.get("broker_ip"), credentials=self.rabbit_credentials,
                                      heartbeat_interval=int(self.heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel = connection.channel()
        log.info("Unregistering VNFM of type %s" % self.type)
        endpoint_type = self.properties.get("endpoint_type")
        check_endpoint_type(endpoint_type)
        manager_endpoint = ManagerEndpoint(type=self.type, endpoint="nfvo.%s.actions" % self.type,
                                           endpointType="RABBIT",
                                           description="First python vnfm")
        log.debug("Sending endpoint type: " + manager_endpoint.toJSON())
        channel.basic_publish(exchange='', routing_key='nfvo.vnfm.unregister',
                              properties=pika.BasicProperties(content_type='text/plain'),
                              body=manager_endpoint.toJSON())


def start_vnfm_instances(vnfm_klass, _type, config_file, instances=1):
    """
    This utility method start :instances number of thread of class vnfm_klass.

    :param vnfm_klass: the Class of the VNFM
    :param _type: the type of the VNFM
    :param config_file: the configuration file of the VNFM
    :param instances: the number of instances

    """

    vnfm = vnfm_klass(_type, config_file)
    log.debug("VNFM Class: %s" % vnfm_klass)
    vnfm._register()
    threads = []
    vnfm.start()
    threads.append(vnfm)

    for index in range(1, instances):
        instance = vnfm_klass(_type, config_file)
        instance.start()
        threads.append(instance)

    while len(threads) > 0:
        new_threads = []
        try:
            for t in threads:
                if t is not None and t.isAlive():
                    t.join(1)
                    new_threads.append(t)
            threads = new_threads
        except KeyboardInterrupt:
            log.info("Ctrl-c received! Sending kill to threads...")
            for t in threads:
                t._set_stop()
            vnfm._unregister()
            vnfm._set_stop()
            return

    vnfm._unregister()
    vnfm._set_stop()
