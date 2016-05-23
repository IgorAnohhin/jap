__author__ = 'v-iganoh'
import threading
from Queue import Queue


class Node(threading.Thread):

    def __init__(self, nid, time_required):
        threading.Thread.__init__(self)
        self.id = nid
        self.time_required = time_required
        self.queue = Queue()
        self.callback = None

    def run(self):
        while True:
            job = self.queue.get()
            if job is not None:
                print "Node '%s', execute job '%s', time required '%s'..." % (self.id, job.id, self.time_required)
                job.execute()
                self.adjust_estimate(job)

                if self.callback is not None:
                    self.callback(self, job, job.current_completion_time)

    def adjust_estimate(self, job):
        print "Adjust job '%s' estimate..." % job.id

        job.overall_completion_time.append(self.time_required)
        job.current_completion_time = self.time_required

        n = len(job.overall_completion_time)
        sm = sum(job.overall_completion_time)
        average = sm/n
        job.estimate += job.current_completion_time - average

        print "Job '%s' estimate '%s' in episode '%s'..." % (job.id, job.estimate, len(job.overall_completion_time))