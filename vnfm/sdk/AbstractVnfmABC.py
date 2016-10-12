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
import operator
import threading
import uuid

import abc
import os
import pika
from exceptions import PyVnfmSdkException
from utils.Utilities import get_map, check_endpoint_type, ManagerEndpoint, get_nfv_message

__author__ = 'lto'

log = logging.getLogger("org.openbaton.python.vnfm.sdk")


class AbstractVnfm(threading.Thread):
    __metaclass__ = abc.ABCMeta

    def __init__(self, type):
        super(AbstractVnfm, self).__init__()
        self._stop = threading.Event()
        log.addHandler(logging.NullHandler())
        self.type = type
        config_file_name = "/etc/openbaton/%s/conf.ini" % self.type  # understand if it works
        log.debug("Config file location: %s" % config_file_name)
        config = ConfigParser.ConfigParser()
        config.read(config_file_name)  # read config file
        self._map = get_map(section='vnfm', config=config)  # get the data from map
        log.debug("Map is: %s" % self._map)
        logging_dir = self._map.get('log_path')

        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)

        file_handler = logging.FileHandler("{0}/{1}-vnfm.log".format(logging_dir, type))
        file_handler.setLevel(level=50)
        log.addHandler(file_handler)

        username = self._map.get("username")
        password = self._map.get("password")
        heartbeat = self._map.get("heartbeat")
        exchange_name = self._map.get("exchange")
        queuedel = True
        if not heartbeat:
            heartbeat = '60'
        if not exchange_name:
            exchange_name = 'openbaton-exchange'

        rabbit_credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._map.get("broker_ip"), credentials=rabbit_credentials,
                                      heartbeat_interval=int(heartbeat)))

        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.exchange_declare(exchange=exchange_name, type="topic", durable=True)

        self.channel.queue_declare(queue='vnfm.nfvo.actions', auto_delete=queuedel, durable=True)
        self.channel.queue_declare(queue='vnfm.nfvo.actions.reply', auto_delete=queuedel, durable=True)
        self.channel.queue_declare(queue='nfvo.%s.actions' % self.type, auto_delete=queuedel, durable=True)

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

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
                    vnf_address=vnfd.get("vnf_address"), status="NULL", descriptor_reference=vnfd.get("id"),
                    lifecycle_event=vnfd.get("lifecycle_event"), virtual_link=vnfd.get("virtual_link"))
        if vnfr.get("requires") is not None:
            if vnfr.get("requires").get("configurationParameters") is None:
                vnfr["requires"]["configurationParameters"] = []
        if vnfr.get("provides") is not None:
            if vnfr.get("provides").get("configurationParameters") is None:
                vnfr["provides"]["configurationParameters"] = []
        if vnfr.get("vnf_address") is None:
            vnfr["vnf_address"] = []

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
        This message is in charge of dispatching the message to the right method
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
        response = self.on_message()
        log.debug("Aknowledging %s" % method.delivery_tag)
        ch.basic_ack(delivery_tag=method.delivery_tag)

        ch.basic_publish(exchange='',
                         routing_key="vnfm.nfvo.actions",
                         properties=pika.BasicProperties(content_type='text/plain'),
                         body=json.dumps(response))

        log.info("Answer sent")

    # def thread_function(self, ch, method, properties, body):
    #     thread.start_new_thread(self.on_request, args=(ch, method, properties, body))

    def run(self):
        self.channel.basic_consume(self.on_request, queue='nfvo.%s.actions' % self.type)
        log.info("Waiting for actions")
        self.channel.start_consuming()

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
        channel = self.connection.channel()
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
            self.connection.process_data_events()

        channel.queue_delete(queue=callback_queue)
        return json.loads(response["result"])

    def get_user_data(self):
        userdata_path = self._map.get("userdata_path", "/etc/openbaton/%s/userdata.sh" % self.type)
        if os.path.isfile(userdata_path):
            with open(userdata_path, "r") as f:
                return f.read()
        else:
            return None

    @staticmethod
    def _register(_map, type, channel):
        # Registration
        log.info("Registering VNFM of type %s" % type)
        endpoint_type = _map.get("endpoint_type")
        log.debug("Got endpoint type: %s" % endpoint_type)
        check_endpoint_type(endpoint_type)
        manager_endpoint = ManagerEndpoint(type=type, endpoint="nfvo.%s.actions" % type,
                                           endpoint_type=endpoint_type,
                                           description="First python vnfm")
        log.debug("Sending endpoint type: " + manager_endpoint.toJSON())
        channel.basic_publish(exchange='', routing_key='nfvo.vnfm.register',
                                   properties=pika.BasicProperties(content_type='text/plain'),
                                   body=manager_endpoint.toJSON())

    @staticmethod
    def _unregister(_map, type, channel):
        # UnRegistration
        log.info("Unregistering VNFM of type %s" % type)
        endpoint_type = _map.get("endpoint_type")
        check_endpoint_type(endpoint_type)
        manager_endpoint = ManagerEndpoint(type=type, endpoint="nfvo.%s.actions" % type,
                                           endpoint_type=endpoint_type,
                                           description="First python vnfm")
        log.debug("Unregistering: " + manager_endpoint.toJSON())
        channel.basic_publish(exchange='', routing_key='nfvo.vnfm.unregister',
                                   properties=pika.BasicProperties(content_type='text/plain'),
                                   body=manager_endpoint.toJSON())


def start_vnfm_instances(vnfm, type, listeners=1):

    vnfm_first_instance = vnfm(type)
    channel = vnfm_first_instance.channel
    type = vnfm_first_instance.type
    _map = vnfm_first_instance._map

    vnfm._register(_map, type, channel)
    threads = []
    vnfm_first_instance.start()
    threads.append(vnfm_first_instance)
    try:
        for index in range(1, listeners):
            log.debug("Starting listener number: %s" % (index + 1))
            vnfm_instance = vnfm(type)
            vnfm_instance.start()
            threads.append(vnfm_first_instance)
    except:
        log.debug("Got exception!")
        import traceback
        traceback.print_exc()
        vnfm._unregister(_map, type, channel)
        exit(1)

    while len(threads) > 0:
        try:
            # Join all threads using a timeout so it doesn't block
            # Filter out threads which have been joined or are None
            threads = [t.join(1) for t in threads if t is not None and t.isAlive()]
        except KeyboardInterrupt:
            print "Ctrl-c received! Sending kill to threads..."
            print "Exiting VNFM"
            vnfm._unregister(_map, type, channel)
            os.kill(os.getpid(), 9)
            exit(0)
