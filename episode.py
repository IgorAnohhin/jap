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
            self.jobs.remove(job)
            node = self._choose_node(job, self.nodes, Episode.busy_nodes)
            if node is None:
                print "Waiting for free node..."

            while node is None:
                sleep(1)
                node = self._choose_node(job, self.nodes, Episode.busy_nodes)

            self._execute(node, job)

            job = self._choose_job(self.jobs)

        self.jobs = Episode.estimated_jobs
        self._wait_jobs_done()
        self.overall_completion_times.append({'eid': Episode.id, 'ctime': self.overall_completion_time})
        self.tries -= 1
        if self.tries > 0:
            print "Episode '%s', Overall complition time '%s'..." \
                  % (Episode.id, self.overall_completion_times[len(self.overall_completion_times)-1]['ctime'])
            print "Episode '%s', Nodes time '%s'..." % (Episode.id, self.node_completion_times)
            Episode.id += 1
            self.execute()
        else:
            print "Overall complition times '%s'..." % self.overall_completion_times
            best = self._best_overall_completion_time()
            print "Best overall complition time '%s' in episode '%s'..." \
                % (best['ctime'], best['eid'])
            print "Nodes from episode with best complition time => '%s'..." \
                % self._episode_nodes(best['eid'])

            exit()

    @staticmethod
    def _execute(node, job):
        Episode.busy_nodes.append(node)
        Episode.running_jobs.append(job)

        def _callback(bnode, cjob, complition_time):
            Episode.overall_completion_time += complition_time
            Episode._set_node_completion_time(bnode, cjob)
            Episode.estimated_jobs.append(cjob)
            Episode.busy_nodes.remove(bnode)
            Episode.running_jobs.remove(cjob)

        node.queue.put(job)
        node.callback = _callback
        if node.isAlive() is not True:
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
        if len(Episode.running_jobs) > 0:
            print "'%s' Jobs are running, wait all jobs are done..." % len(Episode.running_jobs)

        while len(Episode.running_jobs) > 0:
            sleep(1)

    @staticmethod
    def _choose_job(jobs):
        jobs.sort(key=lambda j: j.estimate, reverse=True)
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