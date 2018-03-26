Copyright © 2015-2016 `Open Baton <http://openbaton.org>`__. Licensed
under `Apache v2
License <http://www.apache.org/licenses/LICENSE-2.0>`__.

Python version of the vnfm-sdk
==============================

This project contains a vnfm sdk for python projects.

Technical Requirements
----------------------

This section covers the requirements that must be met by the
python-vnfm-sdk in order to satisfy the demands for such a component:

-  python 2.7 or 3.5
-  pika

How to install python-vfnm-sdk
------------------------------

The safer way to start is to use a `virtal
environment <https://virtualenv.pypa.io/en/stable/>`__. Once activated,
just run

.. code:: bash

    pip install python-vnfm-sdk

How to configure your vnfm
--------------------------

The sdk runs a certain number of instances of the vnfm by invoking the
method:

.. code:: python

    def start_vnfm_instances(vnfm_klass, config_file_path, instances=1, **kwargs):
        # ....
        pass

where the parameters are the Vnfm class that implements AbstractVnfm,
the config file path for the vnfm, the number of instances and the
instantiation arguments for the vnfm class.

The ``ini`` config file looks like

.. code:: ini

    [vnfm]

    endpoint_type=RABBIT
    type=python
    endpoint=python-endpoint
    log_path=/var/log/openbaton/
    broker_ip=127.0.0.1
    autodelete=true
    heartbeat=60
    exchange=openbaton-exchange

where:

+-----------------+--------------------------------------------------+
| name            | description                                      |
+=================+==================================================+
| type            | The type of the vnfm                             |
+-----------------+--------------------------------------------------+
| endpoint_type   | must be RABBIT                                   |
+-----------------+--------------------------------------------------+
| broker_ip       | Ip of the rabbitmq broker used by the nfvo       |
+-----------------+--------------------------------------------------+
| username        | username for the rabbitmq broker used to connect |
|                 | to the manager queue of the nfvo                 |
+-----------------+--------------------------------------------------+
| password        | password for the rabbitmq broker used to connect |
|                 | to the manager queue of the nfvo                 |
+-----------------+--------------------------------------------------+
| exchange        | exchange name used in the rabbitmq broker by the |
|                 | nfvo                                             |
+-----------------+--------------------------------------------------+
| autodelete      | true or false in case you want the endpoint      |
|                 | queue to have the autodelete property (usually   |
|                 | true)                                            |
+-----------------+--------------------------------------------------+
| heartbeat       | heartbeat for the rabbitmq connection            |
+-----------------+--------------------------------------------------+
| log_path        | path where the logfile will be written           |
+-----------------+--------------------------------------------------+

see the `python dummy vnfm as
example <https://github.com/openbaton/python-vnfm-dummy>`__

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

Licensed under the Apache License, Version 2.0 (the “License”);

you may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an “AS IS” BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Support
-------

The Open Baton project provides community support through the Open Baton
Public Mailing List and through StackOverflow using the tags openbaton.

Supported by
------------


