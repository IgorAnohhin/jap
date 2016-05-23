__author__ = 'v-iganoh'
from time import sleep


class Episode:
    busy_nodes = []
    running_jobs = []
    estimated_jobs = []
    overall_completion_times = []
    overall_completion_time = 0
    node_completion_times = {}
    id = 1

    def __init__(self, jobs, nodes, tries):
        self.jobs = jobs
        self.nodes = nodes
        self.tries = tries

    def execute(self):
        Episode.estimated_jobs = []
        Episode.overall_completion_time = 0

        job = self._choose_job(self.jobs)
        while job is not None:

            can_proceed = True
            if len(job.predecessors) > 0:
                for pred_job in job.predecessors:
                    if pred_job not in Episode.estimated_jobs and pred_job not in Episode.running_jobs:
                        job = pred_job
                        can_proceed = False
                        break
                if not can_proceed:
                    continue

            self.jobs.remove(job)
            node = self._choose_node(job, self.nodes, Episode.busy_nodes)
            if node is None:
                print "Waiting for free node..."

            while node is None:
                sleep(0.3)
                node = self._choose_node(job, self.nodes, Episode.busy_nodes)

            self._execute(node, job)

            job = self._choose_job(self.jobs)

        self._wait_jobs_done()
        self.jobs = Episode.estimated_jobs
        print "Jobs count in episode '%s' => '%s'..." % (Episode.id + 1, len(self.jobs))
        self.overall_completion_times.append({'eid': Episode.id, 'ctime': self.overall_completion_time})
        self.tries -= 1
        if self.tries > 0:
            print "Episode '%s', Overall completion time '%s'..." \
                  % (Episode.id, self.overall_completion_times[len(self.overall_completion_times) - 1]['ctime'])
            print "Episode '%s', Nodes time '%s'..." % (Episode.id, self.node_completion_times)
            Episode.id += 1
            self.execute()
        else:
            print "Overall completion times '%s'..." % self.overall_completion_times
            best = self._best_overall_completion_time()
            print "Best overall completion time '%s' in episode '%s'..." \
                  % (best['ctime'], best['eid'])
            print "Nodes from episode with best completion time => '%s'..." \
                  % self._episode_nodes(best['eid'])

            exit()

    @staticmethod
    def _execute(node, job):
        Episode.busy_nodes.append(node)
        Episode.running_jobs.append(job)
        print "Running jobs count in callback function '%s'..." % len(Episode.running_jobs)

        def _callback(bnode, cjob, completion_time):
            Episode.overall_completion_time += completion_time
            Episode._set_node_completion_time(bnode, cjob)
            Episode.estimated_jobs.append(cjob)
            print "Estimated jobs count in callback functions '%s'..." % len(Episode.estimated_jobs)
            Episode.busy_nodes.remove(bnode)
            Episode.running_jobs.remove(cjob)

        node.queue.put(job)
        if node.isAlive() is not True:
            node.callback = _callback
            node.start()

    @staticmethod
    def _set_node_completion_time(node, job):
        if Episode.id not in Episode.node_completion_times:
            Episode.node_completion_times[Episode.id] = {}
        if node.id not in Episode.node_completion_times[Episode.id]:
            Episode.node_completion_times[Episode.id][node.id] = []

        Episode.node_completion_times[Episode.id][node.id].append({'jid': job.id, 'ctime': node.time_required})

    @staticmethod
    def _wait_jobs_done():
        print "In wait for all jobs done, running jobs count '%s'" % len(Episode.running_jobs)
        while len(Episode.running_jobs) > 0:
            sleep(0.1)

    @staticmethod
    def _choose_job(jobs, job=None):
        jobs.sort(key=lambda j: j.estimate, reverse=True)
        if job is not None:
            tjobs = [fj for fj in jobs if fj.id != job.id]
            return tjobs[0] if len(tjobs) > 0 else None

        return jobs[0] if len(jobs) > 0 else None

    @staticmethod
    def _choose_node(job, nodes, busy_nodes):
        nodes = [node for node in nodes[job.id] if node.id not in [n.id for n in busy_nodes]]
        nodes.sort(key=lambda n: n.time_required)
        return nodes[0] if len(nodes) > 0 else None

    @staticmethod
    def _best_overall_completion_time():
        Episode.overall_completion_times.sort(key=lambda j: j['ctime'])
        return Episode.overall_completion_times[0]

    @staticmethod
    def _episode_nodes(eid):
        enodes = Episode.node_completion_times[eid]
        txt = ''
        for i in range(1, 4):
            node = enodes[i]
            txt += "\nNode: '%s' => " % i
            for j in node:
                txt += "'%s:%s' " % (j['jid'], j['ctime'])

        return txt