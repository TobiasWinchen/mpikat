import logging
import time
import socket
import select
import json
import coloredlogs
import signal
from xml import etree
from threading import Thread, Event, Lock
from tornado.gen import coroutine, sleep
from tornado.ioloop import PeriodicCallback, IOLoop
from katcp import Sensor, AsyncDeviceServer, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str
from mpikat.effelsberg.status_config import EFF_JSON_CONFIG

from mpikat.effelsberg.edd.pipeline import EDDPipeline
from mpikat.effelsberg.edd import EDDDataStore

from katportalclient import KATPortalClient

sensor_map = {}
sensor_map['anc_mean_wind_speed'] =  dict(name='wind-speed', converter=None, type="float")
sensor_map['anc_gust_wind_speed'] =  dict(name='wind-speed-gusts', converter=None, type="float")
sensor_map['anc_wind_direction'] =  dict(name='wind-direction', converter=None, type="float")
sensor_map['anc_air_temperature'] =  dict(name='air-temperature', converter=None, type="float")
sensor_map['anc_air_pressure'] =  dict(name='air-pressure', converter=None, type="float")
sensor_map['anc_air_relative_humidity'] =  dict(name='humidity', converter=None, type="float")
sensor_map['anc_weather_rain'] =  dict(name='rain', converter=None, type="float")



TYPE_CONVERTER = {
    "float": float,
    "int": int,
    "string": str,
    "bool": int
}

KATPORTALIP = '10.97.1.14'

log = logging.getLogger('mpikat.ska_proto.status_server')

STATUS_MAP = {
    "error": 3,  # Sensor.STATUSES 'error'
    "norm": 1,  # Sensor.STATUSES 'nominal'
    "ok": 1,  # Sensor.STATUSES 'nominal'
    "warn": 2  # Sensor.STATUSES 'warn'
}


class JsonStatusServer(AsyncDeviceServer):
    VERSION_INFO = ("reynard-eff-jsonstatusserver-api", 0, 1)
    BUILD_INFO = ("reynard-eff-jsonstatusserver-implementation", 0, 1, "rc1")

    def __init__(self, server_host, server_port,
                 source_host=KATPORTALIP,
                 redis_ip ="localhost",
                 redis_port=6379,
                 dummy=False):

        self.__eddDataStore = EDDDataStore.EDDDataStore(redis_ip, redis_port)

        self.portal_client = KATPortalClient("http://{}/api/client".format(source_host), on_update_callback=self.__update, logger=log)

        self._monitor = None
        self._updaters = {}
        self._controlled = set()
        super(JsonStatusServer, self).__init__(server_host, server_port)


    @coroutine
    def __update(self, message):
        print(message)

    @coroutine
    def _update_sensors(self):
        log.debug("Updating sensor values")



        results = {}
        for name, params in sensor_map.items(): 
            results[name] = self.portal_client.sensor_value(name, include_value_ts=True)
        yield results

        try:
            for name, fr in results.items(): 
                res = fr.result()
                params = sensor_map[name]
                if params['name'] in self._controlled:
                    continue
    
        #        res = yield self.portal_client.sensor_value(name, include_value_ts=True)
                print(res)
                log.debug(" {}: {}".format(params['name'], res.value))
    
                if self._sensors[params['name']].value() != res.value:
                    self._sensors[params['name']].set_value(res.value, timestamp=res.value_time)
                    self.__eddDataStore.setTelescopeDataItem(params['name'], res.value)
        except Exception as E:
            log.error("Error updating sensors:")
            log.exception(E)


    @coroutine
    def start(self):
        """start the server"""
        log.debug("Starting server")
        super(JsonStatusServer, self).start()
        #while not self.portal_client.is_connected():
        #    log.debug("Waiting for portal client connection.")
        #    yield sleep(.1)
        #log.debug("Portal client is connected.")
        self.delayed_setup_sensors()

        self._monitor = PeriodicCallback(
            self._update_sensors, 10000, io_loop=self.ioloop)
        self._monitor.start()

    @coroutine
    def stop(self):
        """stop the server"""
        return super(JsonStatusServer, self).stop()

    @request(Str())
    @return_reply(Str())
    def request_sensor_control(self, req, name):
        """take control of a given sensor value"""
        if name not in self._sensors:
            return ("fail", "No sensor named '{0}'".format(name))
        else:
            self._controlled.add(name)
            return ("ok", "{0} under user control".format(name))

    @request()
    @return_reply(Str())
    def request_sensor_control_all(self, req):
        """take control of all sensors value"""
        for name, sensor in self._sensors.items():
            self._controlled.add(name)
        return ("ok", "{0} sensors under user control".format(
            len(self._controlled)))

    @request()
    @return_reply(Int())
    def request_sensor_list_controlled(self, req):
        """List all controlled sensors"""
        count = len(self._controlled)
        for name in list(self._controlled):
            req.inform("{0} -- {1}".format(name, self._sensors[name].value()))
        return ("ok", count)

    @request(Str())
    @return_reply(Str())
    def request_sensor_release(self, req, name):
        """release a sensor from user control"""
        if name not in self._sensors:
            return ("fail", "No sensor named '{0}'".format(name))
        else:
            self._controlled.remove(name)
            return ("ok", "{0} released from user control".format(name))

    @request(Int())
    @return_reply()
    def request_set_sensor_update_intervall(self, req, intervall):
        """release a sensor from user control"""
        self._monitor.stop()
        self._monitor = PeriodicCallback(
            self._update_sensors, intervall * 1000, io_loop=self.ioloop)
        self._monitor.start()
        req.reply("ok")
        raise AsyncReply 




    @request()
    @return_reply(Str())
    def request_sensor_release_all(self, req):
        """take control of all sensors value"""
        self._controlled = set()
        return ("ok", "All sensors released")

    @request(Str(), Str())
    @return_reply(Str())
    def request_sensor_set(self, req, name, value):
        """Set the value of a sensor"""
        if name not in self._sensors:
            return ("fail", "No sensor named '{0}'".format(name))
        if name not in self._controlled:
            return ("fail", "Sensor '{0}' not under user control".format(name))
        try:
            param = self._parser[name]
            value = TYPE_CONVERTER[param["type"]](value)
            self._sensors[name].set_value(value)
            self.__eddDataStore.setTelescopeDataItem(name, value)
        except Exception as error:
            return ("fail", str(error))
        else:
            return (
                "ok", "{0} set to {1}".format(
                    name, self._sensors[name].value()))

    def setup_sensors(self):
        pass

    @coroutine
    def delayed_setup_sensors(self):
        """Set up basic monitoring sensors.
        """
        log.debug('Setup sensors called') 

        results = {}

        for name, params in sensor_map.items(): 
            results[name] = self.portal_client.sensor_detail(name)
        yield results

        for name, v in sensor_map.items():

            log.debug('Create sensor: {}'.format(name)) 
            try:
                params = results[name].result() 
                params['default'] = 'NN' 
                for k, val in params.items():
                    if val is None:
                        params[k] = 'None'
                log.debug('Got sensor information: {}'.format(params)) 
            except Exception as E:
                log.error("Cannot get sesnor information")
                log.exception(E)
                continue
            
            self.__eddDataStore.addTelescopeDataItem(v['name'], params)
            if params["type"] == "float":
                sensor = Sensor.float(
                    v['name'],
                    description=params["description"],
                    unit=params.get("units", None),
                    initial_status=Sensor.UNKNOWN)
            elif params["type"] == "string":
                sensor = Sensor.string(
                    v['name'],
                    description=params["description"],
                    initial_status=Sensor.UNKNOWN)
            elif params["type"] == "int":
                sensor = Sensor.integer(
                    v['name'],
                    description=params["description"],
                    unit=params.get("units", None),
                    initial_status=Sensor.UNKNOWN)
            elif params["type"] == "bool":
                sensor = Sensor.boolean(
                    v['name'],
                    default=params.get("default", False),
                    initial_status=Sensor.UNKNOWN)
            else:
                raise Exception(
                    "Unknown sensor type '{0}' requested".format(
                        params["type"]))
            self.add_sensor(sensor)
        log.debug('Setup sensors done') 
        


if __name__ == "__main__":
    parser = EDDPipeline.getArgumentParser()

    args = parser.parse_args()
    EDDPipeline.setup_logger(args)

    server = JsonStatusServer(args.host, args.port, redis_port=args.redis_port, redis_ip=args.redis_ip)
    EDDPipeline.launchPipelineServer(server, args)

