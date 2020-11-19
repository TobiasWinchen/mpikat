import logging
import json
from tornado.gen import Return, coroutine, with_timeout, TimeoutError
from katcp import KATCPClientResource

log = logging.getLogger("mpikat.edd_server_product_controller")


class EddServerProductController(object):

    def __init__(self, product_id, address, port):
        """
        Interface for pipeline instances using katcp.

        Args:
            product_id:        A unique identifier for this product
            r2rm_addr:         The address of the R2RM (ROACH2 resource manager) to be
                     used by this product. Passed in tuple format,
                     e.g. ("127.0.0.1", 5000)
        """
        log.debug("Installing controller for {} at {}, {}".format(product_id, address, port))
        self.ip = address
        self.port = port
        self._client = KATCPClientResource(dict(
            name="server-client_{}".format(product_id),
            address=(address, int(port)),
            controlled=True))

        self._product_id = product_id
        self._client.start()

    @coroutine
    def _safe_request(self, request_name, *args, **kwargs):
        log.info("Sending request '{}' to {} with arguments {}".format(request_name, self._product_id, args))
        try:
            yield self._client.until_synced()
            response = yield self._client.req[request_name](*args, **kwargs)
        except Exception as E:
            log.error("Error processing request: {} in {}".format(E, self._product_id))
            raise E
        if not response.reply.reply_ok():
            erm = "'{}' request failed in {} with error: {}".format(request_name, self._product_id, response.reply.arguments[1])
            log.error(erm)
            raise RuntimeError(erm)
        else:
            log.debug("'{}' request successful".format(request_name))
            raise Return(response)

    @coroutine
    def deconfigure(self):
        """
        @brief      Deconfigure the product

        @detail
        """
        yield self._safe_request('deconfigure', timeout=120.0)

    @coroutine
    def configure(self, config={}):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        log.debug("Send cfg to {}".format(self._product_id))
        yield self._safe_request("configure", json.dumps(config), timeout=120.0)

    @coroutine
    def capture_start(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        yield self._safe_request("capture_start", timeout=120.0)

    @coroutine
    def capture_stop(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        yield self._safe_request("capture_stop", timeout=120.0)

    @coroutine
    def measurement_prepare(self, config={}):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        yield self._safe_request("measurement_prepare", json.dumps(config), timeout=120.0)

    @coroutine
    def measurement_start(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        yield self._safe_request("measurement_start", timeout=20.0)

    @coroutine
    def measurement_stop(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        yield self._safe_request("measurement_stop",  timeout=20.0)


    @coroutine
    def set(self, config):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        log.debug("Send set to {}".format(self._product_id))
        yield self._safe_request("set", json.dumps(config), timeout=120.0)


    @coroutine
    def provision(self, config):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        log.debug("Send provision to {}".format(self._product_id))
        yield self._safe_request("provision", config, timeout=120.0)


    @coroutine
    def deprovision(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        log.debug("Send deprovision to {}".format(self._product_id))
        yield self._safe_request("deprovision", timeout=120.0)


    @coroutine
    def getConfig(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        log.debug("Send get config to {}".format(self._product_id))
        R = yield self._safe_request("sensor_value", "current-config", timeout=3)
        raise Return(json.loads(R.informs[0].arguments[-1]))

    @coroutine
    def ping(self):
        log.debug("Ping product {} at {}:{}.".format(self._product_id, self.ip, self.port))
        try:
            yield self._client.until_synced(timeout=2)
            log.debug("product reachable")
            cfg = yield self.getConfig()
            if cfg['id'] != self._product_id:
                log.warning('Product id changed!')
                raise Return(False)
            log.debug("ID match")
        except TimeoutError:
            log.debug("Timeout Reached. Product inactive")
            raise Return(False)
        except Exception as E:
            log.error("Error during ping: {}".format(E))
            raise Return(False)
        raise Return(True)
