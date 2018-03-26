import copy
import json
import logging
import uuid

import pika

log = logging.getLogger(__name__)

from openbaton.exceptions import PyVnfmSdkException

# TODO improve this
ENDPOINT_TYPES = ["RABBIT", "REST"]


def get_version():
    return "2.2.1b4"


def get_scripts_from_vnfp(vnf_package):
    if vnf_package:
        scripts = vnf_package.get("scripts")
        if not scripts:
            scripts = vnf_package.get('scriptsLink')
    else:
        scripts = {}
    return scripts


def get_new_vnfc_instance(virtual_network_function_record, component):
    for vdu in virtual_network_function_record.get('vdu'):
        for vnfc_instance in vdu.get('vnfc_instance'):
            if vnfc_instance.get('vnfComponent').get('id') == component.get('id'):
                return vnfc_instance
    raise PyVnfmSdkException('New VNFCInstance not found when scaling out!')


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


def check_endpoint_type(endpoint_type):
    if endpoint_type not in ENDPOINT_TYPES:
        raise PyVnfmSdkException("The endpoint type must be in %s" % ENDPOINT_TYPES)


def get_nfv_message(action, vnfr, vnfc_instance=None, vnfr_dependency=None, exception=None, vim_instances=None,
                    keys=None, user_data=None):
    if action == "INSTANTIATE":
        return {
            "action":                       action,
            "virtualNetworkFunctionRecord": vnfr
        }
    if action == "ERROR":
        java_exception = {
            'detailMessage': str(exception),
            'cause':         {
                'detailMessage': str(exception)
            }
        }
        if vnfr is None:
            vnfr = {}
        return {
            "action":                       action,
            "virtualNetworkFunctionRecord": vnfr,
            "nsrId":                        vnfr.get("parent_ns_id"),
            "exception":                    java_exception
        }
    if action == "MODIFY":
        return {
            "action":                       action,
            "virtualNetworkFunctionRecord": vnfr
        }
    if action == "GRANT_OPERATION":
        return {
            "action":                       action,
            "virtualNetworkFunctionRecord": vnfr
        }
    if action == "ALLOCATE_RESOURCES":
        if user_data is None or user_data == "":
            user_data = "none"
        return {
            "action":                       action,
            "virtualNetworkFunctionRecord": vnfr,
            "vimInstances":                 vim_instances,
            "keyPairs":                     keys,
            "userdata":                     user_data
        }
    if action == "SCALING":
        if user_data:
            return {
                "action":                       action,
                "virtualNetworkFunctionRecord": vnfr,
                "userData":                     user_data
            }
        else:
            return {
                "action":                       action,
                "virtualNetworkFunctionRecord": vnfr,
                "userData":                     ""
            }
    if action == "RELEASE_RESOURCES":
        return {
            "action":                       action,
            "virtualNetworkFunctionRecord": vnfr
        }
    if action == "START":
        return {
            "action":                       action,
            "virtualNetworkFunctionRecord": vnfr,
            "vnfcInstance":                 vnfc_instance,
            "vnfrDependency":               vnfr_dependency
        }
    if action == "SCALED":
        return {
            "action":                       action,
            "virtualNetworkFunctionRecord": vnfr,
            "vnfcInstance":                 vnfc_instance
        }
    if action == 'HEAL':
        return {
            'action':                       action,
            "virtualNetworkFunctionRecord": vnfr,
            "vnfcInstance":                 vnfc_instance
        }
    if action == 'STOP':
        msg = {
            'action':                       action,
            "virtualNetworkFunctionRecord": vnfr
        }
        if vnfc_instance:
            msg['vnfcInstance'] = vnfc_instance
        return msg

    pass


def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


def get_action_and_vnfr(msg):
    action = msg.get("action")
    log.debug("Action is %s" % action)
    virtual_network_function_record = msg.get('virtualNetworkFunctionRecord')
    if not virtual_network_function_record:
        virtual_network_function_record = msg.get('vnfr')

    return action, virtual_network_function_record


def remove_ids(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == 'id' or k == 'hbVersion':
                obj[k] = None
            elif isinstance(v, list):
                for o in v:
                    remove_ids(o)
            elif isinstance(v, dict):
                remove_ids(v)
    elif isinstance(obj, list):
        for x in obj:
            remove_ids(x)
    else:
        return


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
                version=vnfd.get("version"),
                connection_point=vnfd.get("connection_point"),
                deployment_flavour_key=flavor_key,
                vnf_address=[],
                status="NULL",
                descriptor_reference=vnfd.get("id"),
                virtual_link=vnfd.get("virtual_link"))

    remove_ids(vnfr)

    vnfr['lifecycle_event'] = []
    for le in vnfd.get("lifecycle_event"):
        tmp = {}
        for k, v in le.items():
            if k not in ['id', 'hbVersion']:
                tmp[k] = v
        vnfr['lifecycle_event'].append(tmp)

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
        remove_ids(vdu_new)
        vdu_new['parent_vdu'] = vnfd_vdu.get('id')
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

    log.debug("Created VNFR: %s" % vnfr.get('name'))
    return vnfr


def exec_rpc_call(connection, message, queue):
    if isinstance(message, dict):
        message = json.dumps(message)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    log.debug("Channel created")
    result = channel.queue_declare(exclusive=True)
    callback_queue = result.method.queue
    response = {}

    def on_response(ch, method, props, body):
        if corr_id == props.correlation_id:
            response['res'] = body

    channel.basic_consume(on_response,
                          no_ack=True,
                          queue=callback_queue)
    log.debug("Callback Queue is: %s" % callback_queue)
    log.debug("Sending to %s" % queue)
    corr_id = str(uuid.uuid4())
    channel.basic_publish(exchange="",
                          routing_key=queue,
                          properties=pika.BasicProperties(reply_to=callback_queue,
                                                          correlation_id=corr_id,
                                                          content_type='text/plain'),
                          body=message)
    while not response.get('res'):
        connection.process_data_events()

    channel.queue_delete(queue=callback_queue)
    # channel.close()

    return json.loads(response.get('res').decode('utf-8'))
