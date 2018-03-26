class PyVnfmSdkException(Exception):
    def __init__(self, vnfr=None, *args):
        super(PyVnfmSdkException, self).__init__(*args)
        self.vnfr = vnfr
