from __future__ import print_function, division, unicode_literals

import mpikat.utils.numa as numa

import logging

log = logging.getLogger('mpikat.core_manager')

class CoreManager(object):
    """
    Manage cores on one numa nude to reserve for one tasksets
    """
    def __init__(self, numa_node='0'):
        self.__numa_node = numa_node
        self.__tasks = {}
        self.__new_task = True

    def add_task(self, taskname, requested_cores, prefere_isolated=False, require_isolated=False):
        log.debug("Task {}: Requeste cores {} ".format(taskname, requested_cores))
        if not self.__new_task:
            log.warning("Core Management for node {} was already finalized, reevaluating core usage!".format(self.__numa_node))
            self.__new_task = True
        self.__tasks[taskname] = dict(requested_cores=requested_cores, prefere_isolated=prefere_isolated, require_isolated=require_isolated)

    def __finalize_cores(self):
        self.__new_task = False
        print(numa.getInfo())
        numa_info = numa.getInfo()[self.__numa_node]

        cores = numa_info['cores']
        iso_cores = numa_info['isolated_cores']
        # ToDo: Add advanced checks so that tasks requiring isolated cores get them first etc.

        for taskname, taskprops in self.__tasks.items():
            if taskprops['require_isolated']:
                log.debug("Task {}: Requeste cores {} - available: {}".format(taskname, taskprops['requested_cores'], len(iso_cores)))
                if len(iso_cores) < taskprops['requested_cores']:
                    raise RuntimeError("Task {} requires {} isolated cores, but only {} are available.".format(taskprops['requested_cores'], len(iso_cores)))
                taskprops['reserved_cores'] = iso_cores[:taskprops['requested_cores']]
                iso_cores = iso_cores[taskprops['requested_cores']:]
            else:
                if taskprops['prefere_isolated']:
                    if len(iso_cores) < taskprops['requested_cores']:
                        cores = iso_cores + cores
                        iso_cores = []
                    else:
                        cores = iso_cores[:taskprops['requested_cores']] + cores
                        iso_cores = iso_cores[taskprops['requested_cores']:]

                if len(cores) < taskprops['requested_cores']:
                    cores = cores + iso_cores
                    iso_cores = []
                if len(cores) < taskprops['requested_cores']:
                    raise RuntimeError("Requested reservation of {} cores, but only {} available on node!".format(taskprops['requested_cores'], len(cores)))

                taskprops['reserved_cores'] = cores[:taskprops['requested_cores']]
                cores = cores[taskprops['requested_cores']:]



    def get_cores(self, taskname):
        if self.__new_task:
            self.__finalize_cores()
        return self.__tasks[taskname]['reserved_cores']


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
    cm = CoreManager()
    cm.add_task("T1", 3)
    print(cm.get_cores('T1'))


