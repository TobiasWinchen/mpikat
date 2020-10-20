from __future__ import print_function, division, unicode_literals
import unittest
import logging

from mpikat.utils.core_manager import CoreManager
import mpikat.utils.numa as numa

class Test_core_manager(unittest.TestCase):
    def setUp(self):
        self.cm = CoreManager('0')

    def test_too_many_cores(self):
        self.cm.add_task('T1', len(numa.getInfo()['0']['cores']) + len(numa.getInfo()['0']['isolated_cores'])+ 1 )
        with self.assertRaises(Exception) as context:
            self.cm.get_cores('T1')

    def test_too_many_isolated_cores(self):
        self.cm.add_task('T1', len(numa.getInfo()['0']['isolated_cores']) + 1, require_isolated=True)
        with self.assertRaises(Exception) as context:
            self.cm.get_cores('T1')
 
    def test_use_non_isolated_cores(self):
        self.cm.add_task('T1', len(numa.getInfo()['0']['isolated_cores']) + 1, prefere_isolated=True)
        self.cm.get_cores('T1')

    def test_list_return(self):
        self.cm.add_task('T1', 1)
        self.cm.add_task('T2', 2)

        self.assertTrue(isinstance(self.cm.get_cores('T1'), list))
        self.assertTrue(isinstance(self.cm.get_cores('T2'), list))

        self.assertEqual(len(self.cm.get_cores('T1')), 1)
        self.assertEqual(len(self.cm.get_cores('T2')), 2)


    def test_list_str_equivalence(self):
        self.cm.add_task('T1', 3)

        self.assertEqual(self.cm.get_cores('T1'), self.cm.get_coresstr("T1").split(','))


    def test_non_ovverlap(self):
        self.cm.add_task('T1', len(numa.getInfo()['0']['cores']) // 2)
        self.cm.add_task('T2', len(numa.getInfo()['0']['cores']) // 2)
        T1 = self.cm.get_cores('T1')
        T2 = self.cm.get_cores('T2')
        #print("XXX", T1, type(T1))
        self.assertEqual(len(T1), len(set(T1)), "Cores associated to task multiple times: {}".format(T1))
        self.assertEqual(len(T2), len(set(T2)), "Cores associated to task multiple times: {}".format(T2))
        self.assertEqual(len(T2 + T1), len(set(T2 + T1)), "Cores not unique: T1={}, T2={}".format(T1, T2))

if __name__ == '__main__':
    unittest.main()
