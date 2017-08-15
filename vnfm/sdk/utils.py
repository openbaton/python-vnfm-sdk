import json
import logging
import time
import uuid

import pika

from vnfm.sdk.exceptions import PyVnfmSdkException

__author__ = 'lto'

log = logging.getLogger("org.openbaton.vnfm.sdk.%s" % __name__)

# TODO improve this
ENDPOINT_TYPES = ["RABBIT", "REST"]

response = corr_id = None


def get_version():
    return "2.2.1b4"


def get_map(section, config):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                log.debug(("skip: %s" % option))
        except:
            log.debug(("exception on %s!" % option))
            dict1[option] = None
    return dict1


class ManagerEndpoint(object):
    def __init__(self, type, endpoint, endpointType, description=None, enabled=True, active=True):
        self.type = type
        self.endpoint = endpoint
        self.endpointType = endpointType
        self.description = description
        self.enabled = enabled
        self.active = active

    def toJSON(self):
        return json.dumps(
            dict(type=self.type, endpoint=self.endpoint, endpointType=self.endpointType, description=self.description,
                 enabled=self.enabled, active=self.active))

    pass


def check_endpoint_type(endpoint_type):
    if endpoint_type not in ENDPOINT_TYPES:
        raise PyVnfmSdkException("The endpoint type must be in %s" % ENDPOINT_TYPES)


def get_nfv_message(action, vnfr, vnfc_instance=None, vnfr_dependency=None, exception=None, vim_instances=None,
                    keys=None, user_data=None):
    if action == "INSTANTIATE":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr}
    if action == "ERROR":
        java_exception = {'detailMessage': str(exception), 'cause': {'detailMessage': str(exception)}}
        if vnfr is None:
            vnfr = {}
        return {"action": action, "virtualNetworkFunctionRecord": vnfr, "nsrId": vnfr.get("parent_ns_id"),
                "exception": java_exception}
    if action == "MODIFY":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr}
    if action == "GRANT_OPERATION":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr}
    if action == "ALLOCATE_RESOURCES":
        if user_data is None or user_data == "":  user_data = "none"
        return {
            "action": action,
            "virtualNetworkFunctionRecord": vnfr,
            "vimInstances": vim_instances,
            "keyPairs": keys,
            "userdata": user_data
        }
    if action == "SCALING":
        if user_data:
            return {"action": action, "virtualNetworkFunctionRecord": vnfr, "userData": user_data}
        else:
            return {"action": action, "virtualNetworkFunctionRecord": vnfr, "userData": ""}
    if action == "RELEASE_RESOURCES":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr}
    if action == "START":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr, "vnfcInstance": vnfc_instance,
                "vnfrDependency": vnfr_dependency}
    if action == "SCALED":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr, "vnfcInstance": vnfc_instance}
    if action == 'HEAL':
        return {'action': action, "virtualNetworkFunctionRecord": vnfr, "vnfcInstance": vnfc_instance}
    if action == 'STOP':
        msg = {'action': action, "virtualNetworkFunctionRecord": vnfr}
        if vnfc_instance:
            msg['vnfcInstance'] = vnfc_instance
        return msg

    pass


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


def get_rabbit_vnfm_credentials(_type, broker_ip="localhost", port=5672, username=None, password=None, heartbeat=60,
                                endpoint=None, description=None, exchange_name="openbaton-exchange"):
    global response, corr_id
    rabbit_credentials = pika.PlainCredentials(username=username, password=password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=broker_ip,
                                                                   port=port,
                                                                   credentials=rabbit_credentials,
                                                                   heartbeat_interval=int(heartbeat)))

    channel = connection.channel()

    result = channel.queue_declare(exclusive=True)
    callback_queue = result.method.queue

    def _on_response(ch, method, props, body):
        global response, corr_id
        if corr_id == props.correlation_id:
            response = body

    channel.basic_consume(_on_response, no_ack=True, queue=callback_queue)
    response = None
    corr_id = str(uuid.uuid4())
    register_message = json.dumps(dict(type=_type,
                                       action="register",
                                       vnfmManagerEndpoint=get_manager_endpoint(_type, description, endpoint)))
    log.debug("Sending register message %s" % register_message)
    channel.basic_publish(exchange=exchange_name,
                          routing_key='nfvo.manager.handling',
                          properties=pika.BasicProperties(
                              reply_to=callback_queue,
                              correlation_id=corr_id,
                          ),
                          body=register_message)
    while response is None:
        connection.process_data_events()
        time.sleep(0.1)

    if isinstance(response, bytes):
        response = response.decode("utf-8")

    response_dict = json.loads(response)
    return response_dict.get('rabbitUsername'), response_dict.get('rabbitPassword')


def unregister_vnfm(_type, username, password, broker_ip="localhost", port=5672, heartbeat=60,
                    exchange_name="openbaton-exchange",
                    description="", endpoint=None):
    rabbit_credentials = pika.PlainCredentials(username=username, password=password)
    if not endpoint:
        endpoint = _type
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=broker_ip,
                                                                   port=port,
                                                                   credentials=rabbit_credentials,
                                                                   heartbeat_interval=int(heartbeat)))

    channel = connection.channel()
    unregister_message = json.dumps(dict(username=username,
                                         password=password,
                                         action="unregister",
                                         vnfmManagerEndpoint=get_manager_endpoint(_type, description, endpoint)))
    channel.basic_publish(
        exchange=exchange_name,
        routing_key='nfvo.manager.handling',
        body=unregister_message
    )


def get_manager_endpoint(_type, description, endpoint):
    return dict(type=_type,
                endpoint=endpoint or _type,
                endpointType="RABBIT",
                description=description,
                enabled=True,
                active=True)
