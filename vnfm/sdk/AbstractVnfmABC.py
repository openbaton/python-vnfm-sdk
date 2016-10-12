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

import operator
import uuid

import abc
import os
import pika
from utils.Utilities import get_map, get_nfv_message, check_endpoint_type, ManagerEndpoint
from vnfm.sdk.exceptions import PyVnfmSdkException

__author__ = 'lto'

log = logging.getLogger("org.openbaton.python.vnfm.sdk")


class AbstractVnfm(threading.Thread):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def instantiate(self, vnf_record, scripts, vim_instances):
        pass

    @abc.abstractmethod
    def query(self):
        """This operation allows retrieving VNF instance state and attributes."""
        pass

    @abc.abstractmethod
    def scale(self, scale_out, vnf_record, vnf_component, scripts, dependency):
        """This operation allows scaling (out / in, up / down) a VNF instance."""
        pass

    @abc.abstractmethod
    def checkInstantiationFeasibility(self):
        """This operation allows verifying if the VNF instantiation is possible."""
        pass

    @abc.abstractmethod
    def heal(self, vnf_record, vnf_instance, cause):
        pass

    @abc.abstractmethod
    def updateSoftware(self):
        """This operation allows applying a minor / limited software update(e.g.patch) to a VNF instance."""
        pass

    @abc.abstractmethod
    def modify(self, vnf_record, dependency):
        """This  operation allows making structural changes (e.g.configuration, topology, behavior, redundancy model) to a VNF instance."""
        pass

    @abc.abstractmethod
    def upgradeSoftware(self):
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
    def start_vnfr(self, vnf_record):
        """

        :param vnf_record:
        :return:
        """
        pass

    @abc.abstractmethod
    def stop(self, vnf_record):
        """

            :param vnf_record:
            :return:
            """
        pass

    @abc.abstractmethod
    def startVNFCInstance(self, vnf_record, vnfc_instance):
        """

        :param vnf_record:
        :param vnfc_instance:
        :return:
        """
        pass

    @abc.abstractmethod
    def stopVNFCInstance(self, vnf_record, vnfc_instance):
        """

        :param vnf_record:
        :param vnfc_instance:
        :return:
        """
        pass

    @abc.abstractmethod
    def handleError(self, vnf_record):
        """

        :param vnf_record
        :return:
        """
        pass

    # TODO to be DOUBLE checked!
    @staticmethod
    def create_vnf_record(vnfd, flavor_key, vlrs, extension):

        log.debug("Requires is: %s" % vnfd.get("requires"))
        log.debug("Provides is: %s" % vnfd.get("provides"))

        vnfr = dict(lifecycle_event_history=[], parent_ns_id=extension.get("nsr-id"), name=vnfd.get("name"),
                    type=vnfd.get("type"), requires=vnfd.get("requires"), provides=dict(),
                    endpoint=vnfd.get("endpoint"),
                    packageId=vnfd.get("vnfPackageLocation"), monitoring_parameter=vnfd.get("monitoring_parameter"),
                    auto_scale_policy=vnfd.get("auto_scale_policy"), cyclicDependency=vnfd.get("cyclicDependency"),
                    configurations=vnfd.get("configurations"), vdu=vnfd.get("vdu"), version=vnfd.get("version"),
                    connection_point=vnfd.get("connection_point"), deployment_flavour_key=flavor_key,
                    vnf_address=[], status="NULL", descriptor_reference=vnfd.get("id"),
                    lifecycle_event=vnfd.get("lifecycle_event"), virtual_link=vnfd.get("virtual_link"))
        if vnfr.get("requires") is not None:
            if vnfr.get("requires").get("configurationParameters") is None:
                vnfr["requires"]["configurationParameters"] = []
        if vnfr.get("provides") is not None:
            if vnfr.get("provides").get("configurationParameters") is None:
                vnfr["provides"]["configurationParameters"] = []

        # for (VirtualDeploymentUnit virtualDeploymentUnit: vnfd.getVdu()) {
        # for (VimInstance vi: vimInstances.get(virtualDeploymentUnit.getId())) {
        # for (String name: virtualDeploymentUnit.getVimInstanceName()) {
        # if (name.equals(vi.getName()))
        # {
        # if (!existsDeploymentFlavor(
        #     virtualNetworkFunctionRecord.getDeployment_flavour_key(), vi)) {
        #     throw
        # new
        # BadFormatException(
        #     "no key "
        #     + virtualNetworkFunctionRecord.getDeployment_flavour_key()
        #     + " found in vim instance: "
        #     + vi);
        # }
        # }
        # }
        # }
        # }

        for vlr in vlrs:
            for internal_vlr in vnfr["virtual_link"]:
                if vlr.get("name") == internal_vlr.get("name"):
                    internal_vlr["exId"] = vlr.get("extId")

        return vnfr

    def on_message(self, body):
        """
        This message is in charge of dispaching the message to the right method
        :param body:
        :return:
        """
        msg = json.loads(body)

        action = msg.get("action")
        log.debug("Action is %s" % action)
        vnfr = {}
        if action == "INSTANTIATE":
            extension = msg.get("extension")
            keys = msg.get("keys")
            log.debug("Got these keys: %s" % keys)
            vim_instances = msg.get("vimInstances")
            vnfd = msg.get("vnfd")
            vnf_package = msg.get("vnfPackage")
            vlrs = msg.get("vlrs")
            vnfdf = msg.get("vnfdf")
            if vnf_package.get("scriptsLink") is None:
                scripts = vnf_package.get("scripts")
            else:
                scripts = vnf_package.get("scriptsLink")
            vnf_record = self.create_vnf_record(vnfd, vnfdf.get("flavour_key"), vlrs, extension)

            grant_operation = self.grant_operation(vnf_record)
            vnf_record = grant_operation["virtualNetworkFunctionRecord"]
            vim_instances = grant_operation["vduVim"]

            if bool(self._map.get("allocate", True)):
                vnf_record = self.allocate_resources(vnf_record, vim_instances, keys).get(
                    "vnfr")
            vnfr = self.instantiate(vnf_record=vnf_record, scripts=scripts, vim_instances=vim_instances)

        if action == "MODIFY":
            vnfr = self.modify(vnf_record=msg.get("vnfr"), dependency=msg.get("vnfrd"))
        if action == "START":
            vnfr = self.start_vnfr(vnf_record=msg.get("virtualNetworkFunctionRecord"))
        if action == "ERROR":
            vnfr = self.handleError(vnf_record=msg.get("vnfr"))
        if action == "RELEASE_RESOURCES":
            vnfr = self.terminate(vnf_record=msg.get("vnfr"))

        if len(vnfr) == 0:
            raise PyVnfmSdkException("Unknown action!")
        nfv_message = get_nfv_message(action, vnfr)
        # log.debug("answer is: %s" % nfv_message)
        return nfv_message

    def on_request(self, ch, method, props, body):
        log.info("Waiting for actions")
        response = self.on_message(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

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

    def thread_function(self, ch, method, properties, body):
        log.info("here")
        threading.Thread(target=self.on_request, args=(ch, method, properties, body)).start()

    def __init__(self, type):
        super(AbstractVnfm, self).__init__()
        self.queuedel = True
        self._stop = False
        log.addHandler(logging.NullHandler())
        self.type = type
        config_file_name = "/etc/openbaton/%s/conf.ini" % self.type  # understand if it works
        log.debug("Config file location: %s" % config_file_name)
        config = ConfigParser.ConfigParser()
        config.read(config_file_name)  # read config file
        self._map = get_map(section='vnfm', config=config)  # get the data from map
        username = self._map.get("username")
        password = self._map.get("password")
        log.debug("Configuration is: %s" % self._map)
        logging_dir = self._map.get('log_path')

        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)

        file_handler = logging.FileHandler("{0}/{1}-vnfm.log".format(logging_dir, self.type))
        file_handler.setLevel(level=50)
        log.addHandler(file_handler)

        self.heartbeat = self._map.get("heartbeat")
        self.exchange_name = self._map.get("exchange")
        if not self.heartbeat:
            self.heartbeat = '60'
        if not self.exchange_name:
            self.exchange_name = 'openbaton-exchange'

        self.rabbit_credentials = pika.PlainCredentials(username, password)

    def run(self):

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._map.get("broker_ip"), credentials=self.rabbit_credentials,
                                      heartbeat_interval=int(self.heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.exchange_declare(exchange=self.exchange_name, type="topic", durable=True)

        channel.queue_declare(queue='vnfm.nfvo.actions', auto_delete=self.queuedel, durable=True)
        channel.queue_declare(queue='vnfm.nfvo.actions.reply', auto_delete=self.queuedel, durable=True)
        channel.queue_declare(queue='nfvo.%s.actions' % self.type, auto_delete=self.queuedel, durable=True)
        channel.basic_consume(self.thread_function, queue='nfvo.%s.actions' % self.type)
        log.info("Waiting for actions")
        while channel._consumer_infos and not self._stop:
            channel.connection.process_data_events(time_limit=1)

    def set_stop(self):
        self._stop = True

    def grant_operation(self, vnf_record):
        nfv_message = get_nfv_message("GRANT_OPERATION", vnf_record)
        log.info("Executing GRANT_OPERATION")
        result = self.exec_rpc_call(json.dumps(nfv_message))
        log.debug("grant_allowed: %s" % result.get("grantAllowed"))
        log.debug("vdu_vims: %s" % result.get("vduVim").keys())
        log.debug("vnf_record: %s" % result.get("virtualNetworkFunctionRecord").get("name"))

        return result

    def allocate_resources(self, vnf_record, vim_instances, keys):
        user_data = self.get_user_data()
        nfv_message = get_nfv_message(action="ALLOCATE_RESOURCES", vnfr=vnf_record, vim_instances=vim_instances,
                                      user_data=user_data, keys=keys)
        log.debug("Executing ALLOCATE_RESOURCES")
        result = self.exec_rpc_call(json.dumps(nfv_message))
        # log.debug("Allocate resources response: \n%s" % result)
        log.debug("vnf_record: %s" % result.get("vnfr").get("name"))
        return result

    def exec_rpc_call(self, nfv_message, queue="vnfm.nfvo.actions.reply"):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._map.get("broker_ip"), credentials=self.rabbit_credentials,
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
                              properties=pika.BasicProperties(
                                  reply_to=callback_queue,
                                  correlation_id=corr_id,
                                  content_type='text/plain'
                              ),
                              body=nfv_message)
        while len(response) == 0:
            connection.process_data_events()

        channel.queue_delete(queue=callback_queue)
        return json.loads(response["result"])

    def get_user_data(self):
        userdata_path = self._map.get("userdata_path", "/etc/openbaton/%s/userdata.sh" % self.type)
        if os.path.isfile(userdata_path):
            with open(userdata_path, "r") as f:
                return f.read()
        else:
            return None

    def register(self):
        # Registration
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._map.get("broker_ip"), credentials=self.rabbit_credentials,
                                      heartbeat_interval=int(self.heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel = connection.channel()
        log.info("Registering VNFM of type %s" % self.type)
        endpoint_type = self._map.get("endpoint_type")
        log.debug("Got endpoint type: %s" % endpoint_type)
        check_endpoint_type(endpoint_type)
        manager_endpoint = ManagerEndpoint(type=self.type, endpoint="nfvo.%s.actions" % self.type,
                                           endpoint_type=endpoint_type,
                                           description="First python vnfm")
        log.debug("Sending endpoint type: " + manager_endpoint.toJSON())
        channel.basic_publish(exchange='', routing_key='nfvo.vnfm.register',
                              properties=pika.BasicProperties(content_type='text/plain'),
                              body=manager_endpoint.toJSON())

    def unregister(self):
        # UnRegistration
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._map.get("broker_ip"), credentials=self.rabbit_credentials,
                                      heartbeat_interval=int(self.heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel = connection.channel()
        log.info("Unregistering VNFM of type %s" % self.type)
        endpoint_type = self._map.get("endpoint_type")
        check_endpoint_type(endpoint_type)
        manager_endpoint = ManagerEndpoint(type=self.type, endpoint="nfvo.%s.actions" % self.type,
                                           endpoint_type=endpoint_type,
                                           description="First python vnfm")
        log.debug("Sending endpoint type: " + manager_endpoint.toJSON())
        channel.basic_publish(exchange='', routing_key='nfvo.vnfm.unregister',
                              properties=pika.BasicProperties(content_type='text/plain'),
                              body=manager_endpoint.toJSON())


def start_vnfm_instances(vnfm_klass, type, instances=1):
    vnfm = vnfm_klass(type)
    vnfm.register()
    threads = []
    vnfm.start()
    threads.append(vnfm)

    for index in range(1, instances):
        instance = vnfm_klass(type)
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
            for t in threads:
                t.set_stop()
            log.info("Ctrl-c received! Sending kill to threads...")
            vnfm.unregister()
            vnfm.set_stop()
            return

    vnfm.unregister()
    vnfm.set_stop()
