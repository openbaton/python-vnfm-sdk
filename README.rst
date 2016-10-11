Python version of the vnfm-sdk
==============================

This project contains a vnfm sdk for python projects.

Technical Requirements
----------------------

This section covers the requirements that must be met by the
python-vnfm-sdk in order to satisfy the demands for such a component:

-  python 2.7
-  pika

How to install the python-vfnm-sdk
----------------------------------

The safer way to start is to use a `virtual environment <https://virtualenv.pypa.io/en/stable/>`__. Once activated, just run

.. code:: bash
 
   pip install python-vnfm-sdk

How to use the python-vfnm-sdk
------------------------------

First step, let's create a configuration file for the VNFManager under: /etc/openbaton/*<type>*/conf.ini (where type is the **endpoint** specified in the vnfd.json)

This file should be like this:

.. code:: ini

    [vnfm]

    log_path=/var/log/openbaton/
    broker_ip=localhost
    username=admin
    password=openbaton
    heartbeat=60
    exchange=openbaton-exchange

where:

   +-----------+---------------------------------------------------------+
   | name      |    description                                          |
   +===========+=========================================================+
   | log_path  |    path where the logfile will be written               |
   +-----------+---------------------------------------------------------+
   | broker_ip |   Ip of the rabbitmq broker used by the nfvo            |
   +-----------+---------------------------------------------------------+
   | username  |   username for the rabbitmq broker used by the nfvo     |
   +-----------+---------------------------------------------------------+
   | password  |   password for the rabbitmq broker used by the nfvo     |
   +-----------+---------------------------------------------------------+
   | exchange  |   exchange name used in the rabbitmq broker by the nfvo |
   +-----------+---------------------------------------------------------+
   | heartbeat |   heartbeat for the rabbitmq connection                 |
   +-----------+---------------------------------------------------------+


After installing the sdk, you will be able to write a Vnfm that inherit the *AbstractVnfm* class in this way:

.. code:: python

    import json
    import logging.config

    from interfaces.AbstractVnfmABC import AbstractVnfm

    class PythonVnfm(AbstractVnfm):
        def upgradeSoftware(self):
            pass

        def updateSoftware(self):
            pass

        def terminate(self, vnf_record):
            log.info("Executing TERMINATE for VNFR: %s" % vnf_record.get("name"))
            return vnf_record

        def stopVNFCInstance(self, vnf_record, vnfc_instance):
            pass

        def stop(self, vnf_record):
            pass

        def startVNFCInstance(self, vnf_record, vnfc_instance):
            pass

        def start(self, vnf_record):
            log.info("Executing start for VNFR: %s" % vnf_record.get("name"))
            return vnf_record

        def scale(self, scale_out, vnf_record, vnf_component, scripts, dependency):
            pass

        def query(self):
            pass

        def notifyChange(self):
            pass

        def modify(self, vnf_record, dependency):
            log.info("Executing modify for VNFR: %s" % vnf_record.get("name"))
            return vnf_record

        def instantiate(self, vnf_record, scripts, vim_instances):
            log.info("Executing instantiate for VNFR: %s" % vnf_record.get("name"))
            return vnf_record

        def heal(self, vnf_record, vnf_instance, cause):
            pass

        def handleError(self, vnf_record):
            log.info("Executing ERROR for VNFR: %s" % vnf_record.get("name"))
            return vnf_record

        def checkInstantiationFeasibility(self):
            pass



Then you must start it in this way, passing the **type** to the constructor

.. code:: python

    if __name__ == "__main__":
        logging.basicConfig(level="DEBUG")
        vnfm = PythonVnfm("python")
        vnfm.run()

This will register to the NFVO and start wait for the action addressed to the VNFM of type "python" in this example

Issue tracker
-------------

Issues and bug reports should be posted to the GitHub Issue Tracker of
this project

What is Open Baton?
===================

OpenBaton is an open source project providing a comprehensive
implementation of the ETSI Management and Orchestration (MANO)
specification.

Open Baton is a ETSI NFV MANO compliant framework. Open Baton was part
of the OpenSDNCore (www.opensdncore.org) project started almost three
years ago by Fraunhofer FOKUS with the objective of providing a
compliant implementation of the ETSI NFV specification.

Open Baton is easily extensible. It integrates with OpenStack, and
provides a plugin mechanism for supporting additional VIM types. It
supports Network Service management either using a generic VNFM or
interoperating with VNF-specific VNFM. It uses different mechanisms
(REST or PUB/SUB) for interoperating with the VNFMs. It integrates with
additional components for the runtime management of a Network Service.
For instance, it provides autoscaling and fault management based on
monitoring information coming from the the monitoring system available
at the NFVI level.

Source Code and documentation
-----------------------------

The Source Code of the other Open Baton projects can be found
`here <http://github.org/openbaton>`__ and the documentation can be
found `here <http://openbaton.org/documentation>`__ .

News and Website
----------------

Check the `Open Baton Website <http://openbaton.org>`__ Follow us on
Twitter @\ `openbaton <https://twitter.com/openbaton>`__.

Licensing and distribution
--------------------------

Copyright [2015-2016] Open Baton project

Licensed under the Apache License, Version 2.0 (the "License");

you may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Copyright © 2015-2016 `Open Baton <http://openbaton.org>`__. Licensed
under `Apache v2 License <http://www.apache.org/licenses/LICENSE-2.0>`__.

Support
-------

The Open Baton project provides community support through the Open Baton
Public Mailing List and through StackOverflow using the tags openbaton.

Supported by
------------

.. image:: https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/fokus.png
   :width: 250 px

.. image:: https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/tu.png
   :width: 250 px