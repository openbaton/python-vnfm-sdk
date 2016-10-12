import json
import logging

from vnfm.sdk.exceptions import PyVnfmSdkException

__author__ = 'lto'

log = logging.getLogger(__name__)

# TODO improve this
ENDPOINT_TYPES = ["RABBIT", "REST"]


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


def get_nfv_message(action, vnfr, vnfc_instance=None, vnfr_dependency=None, exception=None, vim_instances=None,
                    keys=None, user_data=None):
    if action == "INSTANTIATE":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr}
    if action == "ERROR":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr, "nsrId": vnfr.get("parent_ns_id"),
                "exception": exception}
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
    if action == "RELEASE_RESOURCES":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr}
    if action == "START":
        return {"action": action, "virtualNetworkFunctionRecord": vnfr, "vnfcInstance": vnfc_instance,
                "vnfrDependency": vnfr_dependency}
    pass