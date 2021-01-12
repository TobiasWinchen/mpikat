import unittest
import numpy as np
import uuid
import h5py
import os
import logging

from mpikat.effelsberg.edd.edd_HDF5_writer import EDDHDFFileWriter, gated_spectrometer_format 


class TestEDDHdfFileWriter(unittest.TestCase):
    def test_auto_filename(self):
        f = EDDHDFFileWriter()
        self.addCleanup(os.remove, f.filename)
        f.close()
        self.assertTrue(os.path.exists(f.filename))


    def test_manual_filename(self):
        fn = '/tmp/{}'.format(uuid.uuid4())
        f = EDDHDFFileWriter(fn)
        self.addCleanup(os.remove, f.filename)
        f.close()
        self.assertTrue(os.path.exists(f.filename))

    def test_creation_of_subscans(self):
        fn = '/tmp/{}'.format(uuid.uuid4())
        f = EDDHDFFileWriter(fn)
        self.addCleanup(os.remove, f.filename)

        f.newSubscan()
        f.newSubscan()
        f.close()
        infile = h5py.File(f.filename, "r")
        self.assertEqual(len(infile['scan'].keys()), 2)


    def test_gated_spectrometer_data_insert(self):
        f = EDDHDFFileWriter()
        self.addCleanup(os.remove, f.filename)

        nchannels = 64 * 1024

        data = {}
        for n,d in gated_spectrometer_format(nchannels).items():
            data[n] = np.empty(**d)


        f.newSubscan()
        attr = {'foo':'bar', 'nu': 3}
        f.addData('mysection', data, attr)
        f.close()

        infile = h5py.File(f.filename, "r")

        self.assertTrue("scan" in infile)
        self.assertTrue("scan/000" in infile)

        dataset = infile["scan/000/mysection"]
        for k in data:
            # Strip NaNs from test
            idx = data[k] == data[k]
            self.assertTrue((data[k] == dataset[k][0])[idx].all())

        self.assertEqual(dataset.attrs['foo'], 'bar')
        self.assertEqual(dataset.attrs['nu'], 3)



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
    unittest.main()



