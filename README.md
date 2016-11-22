   <img src="https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/openBaton.png" width="250"/>

  Copyright © 2015-2016 [Open Baton](http://openbaton.org).
  Licensed under [Apache v2 License](http://www.apache.org/licenses/LICENSE-2.0).

# Python version of the vnfm-sdk
This project contains a vnfm sdk for python projects.

## Technical Requirements
This section covers the requirements that must be met by the python-vnfm-sdk in order to satisfy the demands for such a component:

* python 2.7 or 3.5
* pika

## How to install python-vfnm-sdk

The safer way to start is to use a [virtal environment](https://virtualenv.pypa.io/en/stable/). Once activated, just run

 ```bash
 python setup.py build
 python setup.py install
 ```

After that, in this virtual environment a module *interfaces* will be available from which you can inherit the AbstractVnfm class in this way:

```python
import logging

from interfaces.AbstractVnfmABC import AbstractVnfm

class PythonVnfm(AbstractVnfm):

    def upgradeSoftware(self):
        pass

    def updateSoftware(self):
        pass

    def terminate(self, virtualNetworkFunctionRecord):
        pass

    def scale(self, scaleOut, virtualNetworkFunctionRecord, component, scripts, dependency):
        pass

    def query(self):
        pass

    def notifyChange(self):
        pass

    def modify(self, virtualNetworkFunctionRecord, dependency):
        pass

    def instantiate(self, virtualNetworkFunctionRecord, scripts, vimInstances):
        pass

    def heal(self, virtualNetworkFunctionRecord, vnfcInstanceComponent, cause):
        pass

    def checkInstantiationFeasibility(self):
        pass


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    vnfm = PythonVnfm("python")
    vnfm.run()

```

## Issue tracker

Issues and bug reports should be posted to the GitHub Issue Tracker of this project

# What is Open Baton?

OpenBaton is an open source project providing a comprehensive implementation of the ETSI Management and Orchestration (MANO) specification.

Open Baton is a ETSI NFV MANO compliant framework. Open Baton was part of the OpenSDNCore (www.opensdncore.org) project started almost three years ago by Fraunhofer FOKUS with the objective of providing a compliant implementation of the ETSI NFV specification.

Open Baton is easily extensible. It integrates with OpenStack, and provides a plugin mechanism for supporting additional VIM types. It supports Network Service management either using a generic VNFM or interoperating with VNF-specific VNFM. It uses different mechanisms (REST or PUB/SUB) for interoperating with the VNFMs. It integrates with additional components for the runtime management of a Network Service. For instance, it provides autoscaling and fault management based on monitoring information coming from the the monitoring system available at the NFVI level.

## Source Code and documentation

The Source Code of the other Open Baton projects can be found [here][openbaton-github] and the documentation can be found [here][openbaton-doc] .

## News and Website

Check the [Open Baton Website][openbaton]
Follow us on Twitter @[openbaton][openbaton-twitter].

## Licensing and distribution
Copyright [2015-2016] Open Baton project

Licensed under the Apache License, Version 2.0 (the "License");

you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Support
The Open Baton project provides community support through the Open Baton Public Mailing List and through StackOverflow using the tags openbaton.

## Supported by
  <img src="https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/fokus.png" width="250"/><img src="https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/tu.png" width="150"/>

[fokus-logo]: https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/fokus.png
[openbaton]: http://openbaton.org
[openbaton-doc]: http://openbaton.org/documentation
[openbaton-github]: http://github.org/openbaton
[openbaton-logo]: https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/openBaton.png
[openbaton-mail]: mailto:users@openbaton.org
[openbaton-twitter]: https://twitter.com/openbaton
[tub-logo]: https://raw.githubusercontent.com/openbaton/openbaton.github.io/master/images/tu.png
