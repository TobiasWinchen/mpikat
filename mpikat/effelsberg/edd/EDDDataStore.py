import redis
import json
import logging

log = logging.getLogger("mpikat.effelsberg.edd.EDDDataStore")


class redisfail2warn(object):
    """
    Context manager that turns redis connection errors into warnings.
    """
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, traceback):
        if isinstance(exc_val, redis.exceptions.ConnectionError):
            log.warning("Redis connection error\n\n{}\n\n - request ignored.".format(exc_val))
            return True
        else:
            return False


# Data Formats
# Global specified static data formats stored locally to always
# provide a lookup of the values in the code. The values are written to the
# online db to prpovide he information, but updates will not be looked up.

data_formats = {
        "GatedSpectrometer:1": {
            "ip": "",
            "port": "",
            "description": "Spead stream of integrated spectra."
        },
        "MPIFR_EDD_Packetizer:1": {
            "ip": "",
            "port": "",
            "bit_depth" : 0,
            "sample_rate" : 0,
            "sync_time" : 0,
            "band_flip": False,
            "central_freq": 0,

            "samples_per_heap": 4096,
            "description": "Spead stream of time-domain packetizer data as in EDD ICD."
        }
    }





class EDDDataStore:
    """
    @brief Interface to the data store for the EDD.

    @#detail The data store contains the current state of the EDD, augmented
    with additional data of the current state of telescope needed by products.
    """
    def __init__(self, host, port=6379):
        log.debug("Init data store connection: {}:{}".format(host, port))
        self.host = host
        self.port = port

        # The data colelcted by the ansible configuration
        self._ansible = redis.StrictRedis(host=host, port=port, db=0)
        # The currently configured data producers
        self._products = redis.StrictRedis(host=host, port=port, db=1)
        # The currently configured data streams (json objects)
        self._dataStreams = redis.StrictRedis(host=host, port=port, db=2)
        # EDD Static data
        self._edd_static_data = redis.StrictRedis(host=host, port=port, db=3)
        # Telescope meta data
        self._telescopeMetaData = redis.StrictRedis(host=host, port=port, db=4)

        self.__dataBases = [self._ansible, self._products, self._dataStreams, self._edd_static_data, self._telescopeMetaData]
        for d in self.__dataBases:
            with redisfail2warn():
                d.ping()


    def flush(self):
        """
        @brief Flush content of all databases.
        """
        log.debug("Flushing all databses")
        for d in self.__dataBases:
            with redisfail2warn():
                d.flushdb()


    def addDataStream(self, streamid, streamdescription):
        """
        @brief Add a new data stream to the store. Description as dict.
        """
        with redisfail2warn():
            if streamid in self._dataStreams:
                nd = json.dumps(streamdescription)
                if nd == self._dataStreams[streamid]:
                    log.debug("Duplicate output streams: {} defined but with same description".format(streamid))
                    return
                else:
                    log.warning("Duplicate output stream {} defined with conflicting description!\n Existing description: {}\n New description: {}".format(streamid, self._dataStreams[streamid], nd))
                    raise RuntimeError("Invalid configuration")
            self._dataStreams[streamid] = json.dumps(streamdescription)


    def getDataStream(self, streamid):
        """
        @brief Return data stream with stramid as dict.
        """
        return json.loads(self._dataStreams[streamid])



    def updateProduct(self, cfg):
        """
        Updates the global product database for a product with a given config.
        """
        netid = "{ip}:{port}".format(**cfg)
        self._products.hmset(netid, {key: cfg[key ]for key in ['id', 'type', 'ip', 'port']})


    @property
    def products(self):
        """
        @brief List of all product ids.
        """
        d = []
        # Create dict with id as key
        for k in self._products.keys():
            d.append(self._products.getall(k))
        return d


    def hasDataStream(self, streamid):
        """
        @brief True if data stream with given id exists.
        """
        return streamid in self._dataStreams

    def addDataFormatDefinition(self, format_definition):
        """
        @brief Adds a new data format description dict to store.
               All formats have the formatname as key.
        """
        with redisfail2warn():
            key = "DataFormats:{}".format(format_definition["format"])
            if key in self._edd_static_data:
                log.debug("Data format already defined.")
                pass
            params = json.dumps(format_description)
            log.debug("Add data format definition {} - {}".format(key, params))
            self._edd_static_data[key] = params

    def hasDataFormatDefinition(self, format_name):
        """
        @brief Check if data format description already exists.
        """
        key = "DataFormats:{}".format(format_name)
        return key in self._edd_static_data

    def getDataFormatDefinition(self, format_name):
        """
        @brief Returns data format description as dict.
        """
        key = "DataFormats:{}".format(format_name)
        if key in self._edd_static_data:
            return json.loads(self._edd_static_data[key])
        else:
            log.warning("Unknown data format: - {}".format(key))
            return {}


    def addTelescopeDataItem(self, key, pars):
        with redisfail2warn():
            pars['value'] = pars['default']
            try:
                self._telescopeMetaData.hmset(key, pars)
            except Exception as E:
                log.error("Error setting {}".format(key))

    def setTelescopeDataItem(self, key, value):
        with redisfail2warn():
            self._telescopeMetaData.hset(key, "value", value)

    def getTelescopeDataItem(self, key):
        return self._telescopeMetaData.hget(key, "value")

if __name__ == "__main__":
    logging.basicConfig()
    store = EDDDataStore("foo")
    store.setTelescopeDataItem("foo", "bar")
    print (store.getTelescopeDataItem("foo"))
