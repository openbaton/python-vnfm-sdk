class PyVnfmSdkException(Exception):
    def __init__(self, *args, vnfr=None):
        super(PyVnfmSdkException, self).__init__(*args)
        self.vnfr = vnfr
