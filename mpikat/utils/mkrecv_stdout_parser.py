from katcp import Sensor
import logging
import re
import numpy as np

log = logging.getLogger('mpikat.mkrecv_stdout_parser')

MKRECV_STDOUT_KEYS = { "STAT": [("slot-nr", int), ("slot-nheaps", int), ("stc", int),
             ("heaps-completed", int),
             ("heaps-discarded", int), ("heaps-open", int),
             ("payload-expected", int), ("payload-received", int),
             ("global-heaps-completed", int),
             ("global-heaps-discarded", int), ("global-heaps-open", int),
             ("global-payload-expected", int), ("global-payload-received", int)],
             "MCAST": [("group-idx", int), ("missing-heaps", int) ]
}

def mkrecv_stdout_parser(line):
    log.debug(line)

    line = line.replace('slot', '')
    line = line.replace('total', '')

    tokens = line.split()
    params = {}
    if tokens and (tokens[0] in MKRECV_STDOUT_KEYS):
        params = {}
        parser = MKRECV_STDOUT_KEYS[tokens[0]]
        for ii, (key, dtype) in enumerate(parser):
            params[key] = dtype(tokens[ii+1])
    return params


class MkrecvSensors:
    def __init__(self, name_suffix, nsum=100):
        """
        List of sensors and handler to be connected to a mkrecv process
        """
        self.sensors = {}

        self.sensors['global_payload_frac'] = Sensor.float(
                    "global-payload-received-fraction-{}".format(name_suffix),
                    description="Ratio of received and expected payload.",
                    params=[0, 1]
                    )
        self.sensors['received_heaps_frac'] = Sensor.float(
                    "received-heaps-frac-{}".format(name_suffix),
                    description="Fraction of received heaps for last {} slots.".format(100),
                    params=[0, 1]
                    )
        self.sensors['slot_received_heaps'] = Sensor.integer(
                    "slot_received-heaps-{}".format(name_suffix),
                    description="Received heaps.",
                    )
        self.sensors['slot_expected_heaps'] = Sensor.integer(
                    "slot_expected-heaps-{}".format(name_suffix),
                    description="Expected heaps.",
                    )

        self.sensors['missing_heaps'] = Sensor.string(
                    "missing-heaps-{}".format(name_suffix),
                    description="Missing heaps per multicast group.",
                    )


        self.__received_heaps = 0.
        self.__expected_heaps = 0.
        self.__idx = 0
        self.__nsum = nsum

        self.__missing_heaps = np.zeros(8)

    def stdout_handler(self, line):
        data = mkrecv_stdout_parser(line)

        if "global-payload-received" in data:
            try:
                self.sensors["global_payload_frac"].set_value(float(data["global-payload-received"]) / float(data["global-payload-expected"]))
            except ZeroDivisionError as E:
                log.error("Zero division in sensor update. Received line:\n{}".format(line))
        if "heaps-completed" in data and "slot-nheaps" in data:
            self.__idx += 1
            self.__received_heaps += data["heaps-completed"]
            self.__expected_heaps += data["slot-nheaps"]
            if self.__idx >= self.__nsum:
                self.sensors["received_heaps_frac"].set_value( self.__received_heaps / float(self.__expected_heaps + 1E-128))
                self.__idx = 0
                self.__received_heaps = 0.
                self.__expected_heaps = 0.

            self.sensors['slot_expected_heaps'].set_value(data["slot-nheaps"])
            self.sensors['slot_received_heaps'].set_value(data["heaps-completed"])
            self.sensors['missing_heaps'].set_value(np.array2string(self.__missing_heaps))
            # Zero out missing heaps data. Missing heaps is reported before
            # stat line!
            self.__missing_heaps -= self.__missing_heaps

        if "group-idx" in data:
            if data['group-idx'] >= self.__missing_heaps.size:
                # Resize for next round
                self.__missing_heaps = np.zeros(data['group-idx'] + 1)
            else:
                self.__missing_heaps[data['group-idx']] = data['missing-heaps']

