import h5py
import numpy as np
import datetime
import os

import unittest

import logging

_log = logging.getLogger("mpikat.effelsberg.edd.EDDHDFFileWriter")

import string


def gated_spectrometer_format(nchannels):
    """
    Create a format dictionary for the spectrometer format. This has to go somewhere else.
    """
    dformat = {}
    dformat['timetamp'] = {'dtype':float, 'shape': (1,)}
    dformat['spectrum'] = {'dtype':float, 'shape': (nchannels,)}
    dformat['integration_time'] = {'dtype':float, 'shape': (1,)}
    dformat['saturated_samples'] = {'dtype':np.int64, 'shape': (1,)}

    return dformat






## Items from the spead heap edd_fits_interface  --> to be refactrored
#self.ig.add_item(5633, "polarization", "", (6,), dtype=">u1")
#self.ig.add_item(5634, "noise_diode_status", "", (6,), dtype=">u1")
#
#self.ig.add_item(5635, "fft_length", "", (6,), dtype=">u1")
#self.ig.add_item(5637, "sync_time", "", (6,), dtype=">u1")
#self.ig.add_item(5638, "sampling_rate", "", (6,), dtype=">u1")
#self.ig.add_item(5639, "naccumulate", "", (6,), dtype=">u1")
#
#
#self.ig.add_item(5636, "number_of_input_samples", "", (6,), dtype=">u1")
#self.ig.add_item(5632, "timestamp_count", "", (6,), dtype=">u1")











class EDDHDFFileWriter(object):
    """
    A HDF File Wrtier for EDD backend output.
    """

    def __init__(self, filename=None, path = None, file_id_no=None, mode='a', chunksize = 'auto'):
        """
        Args;
            filename:
                If specified this filename will be used. Otherwise a unique
                filename will be generated automatically based on the current
                time.
            path:
                path to use to create the file. Current working directory if None of filename is specified.
            file_id_no:
                If specified this will be used in the automatic file name.
                Otherwise this will be a random string.
            mode:
                w        Create file, truncate if exists
                w- or x  Create file, fail if exists
                a        write if exists, create otherwise (default)
            chunksize:
                Number of data blocks that are hold in memory and written to
                file at once. This also defines the chunksize of the actual hdf
                file.
                Auto uses hdf5 auto chunk size selection.
                Preliminary test showed ~ 25% performance in write spead with
                manual chunk size set to 8 spectra compared to auto.

        ToDo:
            Performance: Tune chunk cache size
            Performance: Check memoro offsets + alignment? Likely auto tuned and already good
        """

        # Use full path for file

        if filename:
            path = os.path.dirname(filename)

        if not path:
            path = os.getcwd()
            _log.debug('No path provided. Using default path: {}'.format(path))

        if filename:
            ofile = os.path.join(path, filename)
        else:
            while True:
                now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
                if not file_id_no:
                    suffix = "".join([string.ascii_letters[i] for i in np.random.randint(0, len(string.ascii_letters), size=5)])
                else:
                    suffix = str(file_id_no)
                filename = "EDD_{}UTC_{}.hdf5".format(now, suffix)
                ofile = os.path.join(path, filename)
                if not os.path.exists(ofile):
                    break

        _log.debug('Using file: {}'.format(ofile))
        self.__filename = ofile
        self.__chunksize = chunksize

        self.__file = h5py.File(self.__filename, mode)

        self.__subscan_id = 0


    @property
    def filename(self):
        return self.__filename


    def newSubscan(self):
        """
        Starts a new subscan.
        """

        if "scan" in self.__file:
            scannum = len(self.__file['scan'].keys())
        else:
            scannum = 0

        scanid = "scan/{:03}".format(scannum)
        _log.debug('Starting new subscan: {}'.format(scanid))
        self.__subscan = self.__file.create_group(scanid)


    def addData(self, section, data):
        """
        Add data block to a section of the current subscan.

        Args:
            section (str): Name of the section
            data (dict):
                data[did] needs to return the data for did in the selected format.

            ToDo:
               Format management
        """

        if not section in self.__subscan:
            _log.debug('Creating new section {} for subscan: {}'.format(section, self.__subscan.name))
            self.__subscan.create_group(section)
#            columns = gated_spectrometer_format
            for k, c in data.items():
                if self.__chunksize == 'auto':
                    self.__subscan[section].create_dataset(k, dtype=c.dtype, shape=(0,) + c.shape, maxshape=(None,)+ c.shape, chunks=True)
                else:
                    self.__subscan[section].create_dataset(k, dtype=c.dtype, shape=(0,) + c.shape, maxshape=(None,)+ c.shape, chunks=(self.__chunksize, )+ c.shape, )


        for did, dataset in self.__subscan[section].items():
            shape = list(dataset.shape)
            shape[0] += 1
            _log.debug('Resizing {}: {} -> {}'.format(dataset.name, dataset.shape, tuple(shape)))
            dataset.resize(tuple(shape))
            dataset[-1] = data[did]


    def close(self):
        """
        Closes the HDf File
        """
        _log.debug('Closing: {}'.format(self.filename))
        self.__file.close()



