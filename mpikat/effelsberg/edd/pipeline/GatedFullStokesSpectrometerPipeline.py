#Copyright (c) 2020 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
"""
The Gate Full-Stokes Spectrometer pipeline receives data in MPIFR EDD packetizer Format from both
polarizations and calculates the resulting Stokes spectra IQUV.


Configuration Settings
----------------------
    input_data_streams
        The spectrometer requires two input data streams ``polarization_0,
        polarization_1`` in MPIFR_EDD_Packetizer format.

    fft_length (int)
        Number of samples to be included in a FFT. As the dc channel is
        dropped, the output contains fft_length/2 channels.

    naccumulate (int)
        Number of output spectra to be accumulated into one output spectrum.

Note:
    The integration time is given by fft_length * naccumulate / sampling_rate
    of the packetizer.


Output Data Streams
-------------------
    Eight data streams in GatedSpectrometer format corresponding to Stokes IQUV spectra for noise diode on and off. 


Warning:
    Although the output is labeled as IQUV throughout the pipeline and the data
    processing code, this is only correct for a linear feed and the secondary
    (primary?) focus.  For circular feed Q->V ,U->Q, V->U. Observing in the
    (primary?) focus results in a factor -1 due to the effective polarization
    flip from the single reflection.


Expert/Debugging Settings
-------------------------
    samples_per_block (int)
        Configure the size of the internal ring buffer. (Default 256*1024*1024)
        This limits the maximum size of the FFT to 128 M Channels by default.
        With this option the code can be tweaked to run on low-mem GPUs or if
        the GPU is  shared with other codes.

    output_rate_factor (float)
        The nominal output data rate is multiplied by this factor and the
        result used to send the data fast enough to keep up with the data
        stream while avoiding bursts. Default: 1.10

    output_type ('network', 'disk', 'null')
        Instead of sending the output to the network, it can be dropped ('null') or written to 'disk'.

    output_directory (str)
        Output directory used for output_type 'disk'. Defaults to '/mnt'

    dummy_input (bool)
        Don't connect to packetizer data streams but use randomly generated
        input data.

    nonfatal_numacheck
        The code check the available numa nodes and selects only nodes for
        execution with GPU and network card. If no suitable numa node exists,
        configuration will fail unless this this option is set to True. If set
        to True, only a warning is emitted. Used e.g. in automatic testing.
"""



from mpikat.utils.process_tools import ManagedProcess, command_watcher
from mpikat.utils.process_monitor import SubprocessMonitor
from mpikat.utils.sensor_watchdog import SensorWatchdog
from mpikat.utils.db_monitor import DbMonitor
from mpikat.utils.mkrecv_stdout_parser import MkrecvSensors
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig, state_change
from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore

import mpikat.utils.numa as numa
from mpikat.utils.core_manager import CoreManager
from mpikat.utils.ip_utils import ipstring_to_list

from tornado.gen import coroutine, sleep
from katcp import Sensor, AsyncReply, FailReply

import os
import logging
import json
import tempfile

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.GatedFullStokesSpectrometerPipeline")

_DEFAULT_CONFIG = {
        "id": "GatedFullStokesSpectrometer",                           # default cfgs for master controler. Needs to get a unique ID -- TODO, from ansible
        "type": "GatedFullStokesSpectrometer",
        "supported_input_formats": {"MPIFR_EDD_Packetizer": [1]},      # supported input formats name:version
        "samples_per_block": 256 * 1024 * 1024,             # 256 Mega samples per buffer block to allow high res  spectra - the
                                                            # theoretical  maximum is thus  128 M Channels.   This option  allows
        "input_data_streams":
        {
            "polarization_0" :
            {
                "format": "MPIFR_EDD_Packetizer:1",         # Format has version seperated via colon
                "ip": "225.0.0.156+3",
                "port": "7148",
                "bit_depth" : 8,
                "sample_rate" : 3200000000,
            },
             "polarization_1" :
            {
                "format": "MPIFR_EDD_Packetizer:1",
                "ip": "225.0.0.156+3",
                "port": "7148",
                "bit_depth" : 8,
                "sample_rate" : 3200000000,
            }
        },
        "output_data_streams":
        {
            "Stokes_I_0" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.172",
                "port": "7152",
            },
            "Stokes_I_1" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.173",
                "port": "7152",
            },
            "Stokes_Q_0" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.174",
                "port": "7152",
            },
            "Stokes_Q_1" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.175",
                "port": "7152",
            },
            "Stokes_U_0" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.176",
                "port": "7152",
            },
            "Stokes_U_1" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.177",
                "port": "7152",
            },
            "Stokes_V_0" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.178",
                "port": "7152",
            },
            "Stokes_V_1" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.179",
                "port": "7152",
            }
        },

        "fft_length": 1024 * 1024 * 2 / 2,
        "naccumulate": 512,
        "output_bit_depth": 32,

        "input_level": 100,
        "output_level": 100,

        "output_directory": "/mnt",                         # ToDo: Should be a output data stream def.
        "output_type": 'network',                           # ['network', 'disk', 'null']  ToDo: Should be a output data stream def.
        "dummy_input": False,                               # Use dummy input instead of mkrecv process. Should be input data stream option.
        "nonfatal_numacheck": False,                             # Ignore numa node constraints, e.g. due to missing networks for debugging 
        "log_level": "debug",

        "output_rate_factor": 1.10,                         # True output date rate is multiplied by this factor for sending.
        "idx1_modulo": "auto",
    }


# static configuration for mkrec. all items that can be configured are passed
# via cmdline
_mkrecv_header = """
## Dada header configuration
HEADER          DADA
HDR_VERSION     1.0
HDR_SIZE        4096
DADA_VERSION    1.0

## MKRECV configuration
PACKET_SIZE         8400
IBV_VECTOR          -1          # IBV forced into polling mode
IBV_MAX_POLL        10
BUFFER_SIZE         128000000

SAMPLE_CLOCK_START  0 # This is updated with the sync-time of the packetiser to allow for UTC conversion from the sample clock

DADA_NSLOTS         3

SLOTS_SKIP          4  # Skip the first four slots

NTHREADS            8

#SPEAD specifcation for EDD packetiser data stream
NINDICES            2      # Although there is more than one index, we are only receiving one polarisation so only need to specify the time index

# The first index item is the running timestamp
IDX1_ITEM           0      # First item of a SPEAD heap

IDX2_ITEM 1
IDX2_LIST 0,1
IDX2_MASK 0x1

# Add side item to buffer
SCI_LIST            2
"""

# static configuration for mksend. all items that can be configured are passed
# via cmdline
_mksend_header = """
HEADER          DADA
HDR_VERSION     1.0
HDR_SIZE        4096
DADA_VERSION    1.0

# MKSEND CONFIG
NETWORK_MODE  1
PACKET_SIZE 8400
IBV_VECTOR   -1          # IBV forced into polling mode
IBV_MAX_POLL 10

SYNC_TIME           unset  # Default value from mksend manual
SAMPLE_CLOCK        unset  # Default value from mksend manual
UTC_START           unset  # Default value from mksend manual

#number of heaps with the same time stamp.
HEAP_COUNT 1
HEAP_ID_OFFSET  1
HEAP_ID_STEP    13

NSCI            1
NITEMS          9
ITEM1_ID        5632    # timestamp, slowest index
ITEM1_SERIAL

ITEM2_ID        5633    # polarization
ITEM2_LIST      0,1,2,3
ITEM2_INDEX     2

ITEM3_ID        5634    # noise diode status
ITEM3_LIST      0,1
ITEM3_INDEX     3

ITEM4_ID        5635    # fft_length

ITEM5_ID        5636
ITEM5_SCI       1       # number of input heaps with ndiode on/off

ITEM6_ID        5637    # sync_time

ITEM7_ID        5638    # sampling rate
ITEM8_ID        5639    # naccumulate

ITEM9_ID        5640    # payload item (empty step, list, index and sci)
"""


class GatedFullStokesSpectrometerPipeline(EDDPipeline):
    """Full Stokes Spectrometer 
    """

    def __init__(self, ip, port):
        """initialize the pipeline."""
        EDDPipeline.__init__(self, ip, port, _DEFAULT_CONFIG)
        self.mkrec_cmd = []
        self._dada_buffers = []
        self.__dada_key = "dada"  # key of inpt buffer, output is inverse



    def setup_sensors(self):
        """
        Setup monitoring sensors
        """
        EDDPipeline.setup_sensors(self)

        self._integration_time_status = Sensor.float(
            "integration-time",
            description="Integration time [s]",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._integration_time_status)

        self._output_rate_status = Sensor.float(
            "output-rate",
            description="Output data rate [Gbyte/s]",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._output_rate_status)

        self._mkrecv_sensors = MkrecvSensors("")

        for s in self._mkrecv_sensors.sensors.values():
            self.add_sensor(s)

        self._input_buffer_fill_level = Sensor.float(
                "input-buffer-fill-level",
                description="Fill level of the input buffer",
                params=[0, 1])
        self.add_sensor(self._input_buffer_fill_level)

        self._input_buffer_total_write = Sensor.float(
                "input-buffer-total-write",
                description="Total write into input buffer ",
                params=[0, 1])
        self.add_sensor(self._input_buffer_total_write)

        self._output_buffer_fill_level = Sensor.float(
                "output-buffer-fill-level",
                description="Fill level of the output buffer")
        self.add_sensor(self._output_buffer_fill_level)
        self._output_buffer_total_read = Sensor.float(
                "output-buffer-total-read",
                description="Total read from output buffer")
        self.add_sensor(self._output_buffer_total_read)


    @coroutine
    def _create_ring_buffer(self, bufferSize, blocks, key, numa_node):
         """
         Create a ring buffer of given size with given key on specified numa node.
         Adds and register an appropriate sensor to thw list
         """
         # always clear buffer first. Allow fail here
         yield command_watcher("dada_db -d -k {key}".format(key=key), allow_fail=True)

         cmd = "numactl --cpubind={numa_node} --membind={numa_node} dada_db -k {key} -n {blocks} -b {bufferSize} -p -l".format(key=key, blocks=blocks, bufferSize=bufferSize, numa_node=numa_node)
         log.debug("Running command: {0}".format(cmd))
         yield command_watcher(cmd)

         M = DbMonitor(key, self._buffer_status_handle)
         M.start()
         self._dada_buffers.append({'key': key, 'monitor': M})


    def _buffer_status_handle(self, status):
        """
        Process a change in the buffer status
        """
        if status['key'] == self.__dada_key:
            self._input_buffer_total_write.set_value(status['written'])
            self._input_buffer_fill_level.set_value(status['fraction-full'])

        elif status['key'] == self.__dada_key[::-1]:
            self._output_buffer_fill_level.set_value(status['fraction-full'])
            self._output_buffer_total_read.set_value(status['read'])


    @state_change(target="configured", allowed=["idle"], intermediate="configuring")
    @coroutine
    def configure(self, config_json):
        """
        Configure the EDD gated spectrometer

        Args:
            config_json:    A JSON dictionary object containing configuration information
        """
        log.info("Configuring EDD backend for processing")
        log.debug("Configuration string: '{}'".format(config_json))

        yield self.set(config_json)

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)

        self.__numa_node_pool = []
        # remove numa nodes with missing capabilities
        for node in numa.getInfo():
            if len(numa.getInfo()[node]['gpus']) < 1:
                log.debug("Not enough gpus on numa node {} - removing from pool.".format(node))
                continue
            elif len(numa.getInfo()[node]['net_devices']) < 1:
                log.debug("Not enough nics on numa node {} - removing from pool.".format(node))
                continue
            else:
                self.__numa_node_pool.append(node)

        log.debug("{} numa nodes remaining in pool after constraints.".format(len(self.__numa_node_pool)))

        if len(self.__numa_node_pool) == 0:
            if self._config['nonfatal_numacheck']:
                log.warning("Not enough numa nodes to process data!")
                self.__numa_node_pool = numa.getInfo().keys()
            else:
                raise FailReply("Not enough numa nodes to process data!")

        self._subprocessMonitor = SubprocessMonitor()

        if len(self._config['input_data_streams']) != 2:
            raise FailReply("Require 2 polarization input, got {}".format(len(self._config['input_data_streams'])))

        log.debug("Merging ip ranges")
        self.stream_description = self._config['input_data_streams'].items()[0][1]
        self.stream_description["ip"] += ",{}".format(self._config['input_data_streams'].items()[1][1]["ip"])
        log.debug("Merged ip ranges: {}".format(self.stream_description["ip"]))

        self.input_heapSize =  self.stream_description["samples_per_heap"] * self.stream_description['bit_depth'] // 8

        nHeaps = self._config["samples_per_block"] // self.stream_description["samples_per_heap"]
        input_bufferSize = nHeaps * (self.input_heapSize + 64 // 8)
        log.info('Input dada parameters created from configuration:\n\
                heap size:        {} byte\n\
                heaps per block:  {}\n\
                buffer size:      {} byte'.format(self.input_heapSize, nHeaps, input_bufferSize))

        # calculate output buffer parameters
        nSlices = max(self._config["samples_per_block"] // 2  // self._config['fft_length'] //  self._config['naccumulate'], 1)
        nChannels = self._config['fft_length'] // 2 + 1
        # on / off spectrum  + one side channel item per spectrum
        output_bufferSize = nSlices * (8 * (nChannels * self._config['output_bit_depth'] // 8 + 8))

        output_heapSize = nChannels * self._config['output_bit_depth'] // 8
        integrationTime = self._config['fft_length'] * self._config['naccumulate']  / (float(self.stream_description["sample_rate"]))
        self._integration_time_status.set_value(integrationTime)
        rate = output_heapSize / integrationTime # in spead documentation BYTE per second and not bit!
        rate *= self._config["output_rate_factor"]        # set rate to (100+X)% of expected rate
        self._output_rate_status.set_value(rate / 1E9)

        log.info('Output parameters calculated from configuration:\n\
                spectra per block:  {} \n\
                nChannels:          {} \n\
                buffer size:        {} byte \n\
                integrationTime :   {} s \n\
                heap size:          {} byte\n\
                rate ({:.0f}%):        {} Gbps'.format(nSlices, nChannels, output_bufferSize, integrationTime, output_heapSize, self._config["output_rate_factor"]*100, rate / 1E9))

        numa_node = self.__numa_node_pool[0]
        log.debug("Associating with numa node {}".format(numa_node))

        # configure dada buffer
        yield self._create_ring_buffer(input_bufferSize, 64, self.__dada_key, numa_node)

        ofname = self.__dada_key[::-1]
        # we write nSlice blocks on each go
        yield self._create_ring_buffer(output_bufferSize, 8 * nSlices, ofname, numa_node)

        ## specify all subprocesses
        self.__coreManager = CoreManager(numa_node)
        self.__coreManager.add_task("gated_spectrometer", 1)

        N_inputips = 0
        for p in self.stream_description["ip"].split(','):
            N_inputips+= len(ipstring_to_list(p))
        log.debug("Found {} input ips".format(N_inputips))
        self.__coreManager.add_task("mkrecv", N_inputips + 1, prefere_isolated=True)
        self.__coreManager.add_task("mksend", 2, prefere_isolated=True)

        # Configure + launch 
        cmd = "taskset -c {physcpu} gated_spectrometer --nsidechannelitems=1 --input_key={dada_key} --speadheap_size={heapSize} --selected_sidechannel=0 --nbits={bit_depth} --fft_length={fft_length} --naccumulate={naccumulate} --input_level={input_level} --output_bit_depth={output_bit_depth} --output_level={output_level} -o {ofname} --log_level={log_level} --output_format=Stokes  --input_polarizations=Dual --output_type=dada".format(dada_key=self.__dada_key, ofname=ofname, heapSize=self.input_heapSize, numa_node=numa_node, bit_depth=self.stream_description['bit_depth'], physcpu=self.__coreManager.get_coresstr('gated_spectrometer'), **self._config)
        log.debug("Command to run: {}".format(cmd))

        cudaDevice = numa.getInfo()[numa_node]['gpus'][0]
        gated_cli = ManagedProcess(cmd, env={"CUDA_VISIBLE_DEVICES": cudaDevice})
        log.debug("Visble Cuda Device: {}".format(cudaDevice))
        self._subprocessMonitor.add(gated_cli, self._subprocess_error)
        self._subprocesses.append(gated_cli)

        cfg = self._config.copy()
        cfg.update(self.stream_description)
        cfg["dada_key"] = self.__dada_key

        ip_range = []
        port = set()
        for key in self._config["output_data_streams"]:
            ip_range.append(self._config["output_data_streams"][key]['ip'])
            port.add(self._config["output_data_streams"][key]['port'])
        if len(port)!=1:
            raise FailReply("Output data has to be on the same port! ")

        if self._config["output_type"] == 'network':
            mksend_header_file = tempfile.NamedTemporaryFile(delete=False)
            mksend_header_file.write(_mksend_header)
            mksend_header_file.close()

            nhops = len(ip_range)

            timestep = cfg["fft_length"] * cfg["naccumulate"]
            #select network interface
            fastest_nic, nic_params = numa.getFastestNic(numa_node)
            heap_id_start = 0 #2 * i    # two output spectra per pol

            log.info("Sending data on NIC {} [ {} ] @ {} Mbit/s".format(fastest_nic, nic_params['ip'], nic_params['speed']))
            cmd = "taskset -c {physcpu} mksend --header {mksend_header} --heap-id-start {heap_id_start} --dada-key {ofname} --ibv-if {ibv_if} --port {port_tx} --sync-epoch {sync_time} --sample-clock {sample_rate} --item1-step {timestep} --item4-list {fft_length} --item6-list {sync_time} --item7-list {sample_rate} --item8-list {naccumulate} --rate {rate} --heap-size {heap_size} --nhops {nhops} {mcast_dest}".format(mksend_header=mksend_header_file.name, heap_id_start=heap_id_start , timestep=timestep,
                        ofname=ofname, nChannels=nChannels, physcpu=self.__coreManager.get_coresstr('mksend'), integrationTime=integrationTime,
                        rate=rate, nhops=nhops, heap_size=output_heapSize, ibv_if=nic_params['ip'],
                        mcast_dest=" ".join(ip_range),
                        port_tx=port.pop(), **cfg)
            log.debug("Command to run: {}".format(cmd))

        elif self._config["output_type"] == 'disk':
            ofpath = os.path.join(cfg["output_directory"], ofname)
            log.debug("Writing output to {}".format(ofpath))
            if not os.path.isdir(ofpath):
                os.makedirs(ofpath)
            cmd = "dada_dbdisk -k {ofname} -D {ofpath} -W".format(ofname=ofname, ofpath=ofpath, **cfg)
        else:
            log.warning("Selected null output. Not sending data!")
            cmd = "dada_dbnull -z -k {}".format(ofname)

        log.debug("Command to run: {}".format(cmd))
        mks = ManagedProcess(cmd, env={"CUDA_VISIBLE_DEVICES": cudaDevice})
        self._subprocessMonitor.add(mks, self._subprocess_error)
        self._subprocesses.append(mks)

        self._subprocessMonitor.start()


    @state_change(target="streaming", allowed=["configured"], intermediate="capture_starting")
    @coroutine
    def capture_start(self, config_json=""):
        """
        start streaming of spectrometer output.
        """
        log.info("Starting EDD backend")
        try:


            mkrecvheader_file = tempfile.NamedTemporaryFile(delete=False)
            log.debug("Creating mkrec header file: {}".format(mkrecvheader_file.name))
            mkrecvheader_file.write(_mkrecv_header)
            # DADA may need this
            # ToDo: Check for input stream definitions
            mkrecvheader_file.write("NBIT {}\n".format(self.stream_description["bit_depth"]))
            mkrecvheader_file.write("HEAP_SIZE {}\n".format(self.input_heapSize))

            mkrecvheader_file.write("\n#OTHER PARAMETERS\n")
            mkrecvheader_file.write("samples_per_block {}\n".format(self._config["samples_per_block"]))

            mkrecvheader_file.write("\n#PARAMETERS ADDED AUTOMATICALLY BY MKRECV\n")
            mkrecvheader_file.close()

            cfg = self._config.copy()
            cfg.update(self.stream_description)
            cfg["dada_key"] = self.__dada_key
            if not self._config['dummy_input']:
                numa_node = self.__numa_node_pool[0]
                fastest_nic, nic_params = numa.getFastestNic(numa_node)
                log.info("Receiving data on NIC {} [ {} ] @ {} Mbit/s".format(fastest_nic, nic_params['ip'], nic_params['speed']))

                if self._config['idx1_modulo'] == 'auto': # Align along output ranges
                    idx1modulo = self._config['fft_length'] * self._config['naccumulate'] // self.stream_description['samples_per_heap']
                else:
                    idx1modulo = self._config['idx1_modulo']

                cmd = "taskset -c {physcpu} mkrecv_v4 --quiet --header {mkrecv_header} --idx1-step {samples_per_heap} --heap-size {input_heap_size} --idx1-modulo {idx1modulo} \
                --dada-key {dada_key} --sync-epoch {sync_time} --sample-clock {sample_rate} \
                --ibv-if {ibv_if} --port {port} {ip}".format(mkrecv_header=mkrecvheader_file.name, physcpu=self.__coreManager.get_coresstr('mkrecv'), ibv_if=nic_params['ip'], input_heap_size=self.input_heapSize, idx1modulo=idx1modulo,
                        **cfg )
                mk = ManagedProcess(cmd, stdout_handler=self._mkrecv_sensors.stdout_handler)
            else:
                log.warning("Creating Dummy input instead of listening to network!")
                cmd = "dada_junkdb -c 1 -R 1000 -t 3600 -k {dada_key} {mkrecv_header}".format(mkrecv_header=mkrecvheader_file.name,
                            **cfg )

                mk = ManagedProcess(cmd)

            self.mkrec_cmd.append(mk)
            self._subprocessMonitor.add(mk, self._subprocess_error)
            self._subprocesses.append(mk)

        except Exception as E:
            log.error("Error starting pipeline: {}".format(E))
            raise E
        else:
            self.__watchdogs = []
            wd = SensorWatchdog(self._input_buffer_total_write,
                    10 * self._integration_time_status.value(),
                    self.watchdog_error)
            wd.start()
            self.__watchdogs.append(wd)
            # Wait for one integration period before finishing to ensure
            # streaming has started before OK
            yield sleep(self._integration_time_status.value())


    @state_change(target="idle", allowed=["streaming", "deconfiguring"], intermediate="capture_stopping")
    @coroutine
    def capture_stop(self):
        """
        @brief Stop streaming of data
        """
        log.info("Stoping EDD backend")
        for wd in self.__watchdogs:
            wd.stop_event.set()
            yield
        if self._subprocessMonitor is not None:
            self._subprocessMonitor.stop()
            yield

        # stop mkrec process
        log.debug("Stopping mkrecv processes ...")
        for proc in self.mkrec_cmd:
            proc.terminate()
            yield
        # This will terminate also the gated spectromenter automatically

        yield self.deconfigure()


    @state_change(target="idle", intermediate="deconfiguring", error='panic')
    @coroutine
    def deconfigure(self):
        """
        @brief deconfigure the gated spectrometer pipeline.
        """
        log.info("Deconfiguring EDD backend")
        if self.previous_state == 'streaming':
            yield self.capture_stop()

        if self._subprocessMonitor is not None:
            yield self._subprocessMonitor.stop()
        for proc in self._subprocesses:
            yield proc.terminate()

        self.mkrec_cmd = []

        log.debug("Destroying dada buffers")
        for k in self._dada_buffers:
            k['monitor'].stop()
            cmd = "dada_db -d -k {0}".format(k['key'])
            log.debug("Running command: {0}".format(cmd))
            yield command_watcher(cmd, allow_fail=True)

        self._dada_buffers = []



if __name__ == "__main__":
    launchPipelineServer(GatedFullStokesSpectrometerPipeline)
