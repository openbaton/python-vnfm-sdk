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
    import configparser as cp # py3
except ImportError:
    import ConfigParser as cp # py2

import json
import logging
import threading

import operator
import uuid
import copy

import abc
import os
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
    def scale_out(self, vnf_record, vnf_component, scripts, dependency):
        """This operation allows scaling out a VNF instance."""
        pass

    @abc.abstractmethod
    def scale_in(self, vnf_record, vnfc_instance):
        """This operation allows scaling in a VNF instance."""
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
    def create_vnf_record(vnfd, flavor_key, vlrs, vim_instances, extension):

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

        # for python 2 and 3 compatibility
        try:
            msg = json.loads(body)
        except TypeError:
            msg = json.loads(body.decode('utf-8'))

        try:
            action = msg.get("action")
            log.debug("Action is %s" % action)
            vnfr = msg.get('virtualNetworkFunctionRecord')
            if not vnfr:
                vnfr = msg.get('vnfr')
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
                if vnf_package:
                    if vnf_package.get("scriptsLink") is None:
                        scripts = vnf_package.get("scripts")
                    else:
                        scripts = vnf_package.get("scriptsLink")
                vnfr = self.create_vnf_record(vnfd, vnfdf.get("flavour_key"), vlrs, vim_instances, extension)

                grant_operation = self.grant_operation(vnfr)
                vnfr = grant_operation["virtualNetworkFunctionRecord"]
                vim_instances = grant_operation["vduVim"]

                if str2bool(self._map.get("allocate", 'True')):
                    vnfr = self.allocate_resources(vnfr, vim_instances, keys, extension).get(
                        "vnfr")
                vnfr = self.instantiate(vnf_record=vnfr, scripts=scripts, vim_instances=vim_instances)


            if action == "MODIFY":
                vnfr = self.modify(vnf_record=vnfr, dependency=msg.get("vnfrd"))
            if action == "START":
                vnfr = self.start_vnfr(vnf_record=vnfr)
            if action == "ERROR":
                vnfr = self.handleError(vnf_record=vnfr)
            if action == "RELEASE_RESOURCES":
                vnfr = self.terminate(vnf_record=vnfr)
            if action == 'SCALE_OUT':
                component = msg.get('component')
                vnf_package = msg.get('vnfPackage')
                dependency = msg.get('dependency')
                mode = msg.get('mode')
                extension = msg.get('extension')

                if str2bool(self._map.get("allocate", 'True')):
                    scaling_message = get_nfv_message('SCALING', vnfr, user_data=self.get_user_data())
                    log.debug('The NFVO allocates resources. Send SCALING message.')
                    result = self.exec_rpc_call(json.dumps(scaling_message))
                    log.debug('Received {} message.'.format(result.get('action')))
                    vnfr = result.get('vnfr')

                vnfr = self.scale_out(vnfr, component, None, dependency)
                new_vnfc_instance = None
                for vdu in vnfr.get('vdu'):
                    for vnfc_instance in vdu.get('vnfc_instance'):
                        if vnfc_instance.get('vnfComponent').get('id') == component.get('id'):
                            if mode == 'STANDBY':
                                vnfc_instance['state'] = 'STANDBY'
                            new_vnfc_instance = vnfc_instance
                if new_vnfc_instance == None:
                    raise PyVnfmSdkException('Did not find a new VNFCInstance after scale out.')
                nfv_message = get_nfv_message('SCALED', vnfr, new_vnfc_instance)
            if action == 'SCALE_IN':
                vnfr = self.scale_in(vnfr, msg.get('vnfcInstance'))

            if len(vnfr) == 0:
                raise PyVnfmSdkException("Unknown action!")
            if nfv_message == None:
                nfv_message = get_nfv_message(action, vnfr)
            return nfv_message
        except PyVnfmSdkException as exception:
            nfv_message = get_nfv_message('ERROR', vnfr, exception=exception)
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
        self._stop_running = False
        log.addHandler(logging.NullHandler())
        self.type = type
        config_file_name = "/etc/openbaton/%s/conf.ini" % self.type  # understand if it works
        log.debug("Config file location: %s" % config_file_name)
        config = cp.ConfigParser()
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
        self.durable = self._map.get("exchange_durable")
        if not self.heartbeat:
            self.heartbeat = '60'
        if not self.exchange_name:
            self.exchange_name = 'openbaton-exchange'
        if not self.durable:
            self.durable = True

        self.rabbit_credentials = pika.PlainCredentials(username, password)

    def run(self):

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self._map.get("broker_ip"), credentials=self.rabbit_credentials,
                                      heartbeat_interval=int(self.heartbeat)))

        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.exchange_declare(exchange=self.exchange_name, type="topic", durable=self.durable)

        channel.queue_declare(queue='vnfm.nfvo.actions', auto_delete=self.queuedel, durable=self.durable)
        channel.queue_declare(queue='vnfm.nfvo.actions.reply', auto_delete=self.queuedel, durable=self.durable)
        channel.queue_declare(queue='nfvo.%s.actions' % self.type, auto_delete=self.queuedel, durable=self.durable)
        channel.basic_consume(self.thread_function, queue='nfvo.%s.actions' % self.type)
        log.info("Waiting for actions")
        while channel._consumer_infos and not self._stop_running:
            channel.connection.process_data_events(time_limit=1)

    def set_stop(self):
        self._stop_running = True

    def grant_operation(self, vnf_record):
        nfv_message = get_nfv_message("GRANT_OPERATION", vnf_record)
        log.info("Executing GRANT_OPERATION")
        result = self.exec_rpc_call(json.dumps(nfv_message))
        log.debug("grant_allowed: %s" % result.get("grantAllowed"))
        log.debug("vdu_vims: %s" % result.get("vduVim").keys())
        log.debug("vnf_record: %s" % result.get("virtualNetworkFunctionRecord").get("name"))

        return result

    def allocate_resources(self, vnf_record, vim_instances, keys, **kwargs):
        user_data = self.get_user_data()
        user_data.replace("export MONITORING_IP=","export MONITORING_IP=%s" % kwargs.get("monitoringIp"))
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

        """
        decode() ensures that this is a unicode string suitable for json
        (json requires valid unicode), converting bytes into str in py3 and
        str into unicode in py2.
        """
        return json.loads(response["result"].decode('utf-8'))

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
