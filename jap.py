__author__ = 'v-iganoh'
import os
from job import Job
from node import Node
from episode import Episode


def _get_job_by_id(job_id, jobs):
    tjobs = [job for job in jobs if job.id == job_id]
    if len(tjobs) > 0:
        jobs.remove(tjobs[0])

    return tjobs[0] if len(tjobs) > 0 else None


def _init_node(jid, job_data, nodes):
    nodes_count = len(job_data) - 2
    for c in range(nodes_count):
        n_time = int(job_data[c+2])
        n_id = int(c+1)
        node = Node(n_id, n_time)

        if jid not in nodes:
            nodes[jid] = []

        nodes[jid].append(node)


def _init_jobs_and_nodes_no_deps(config):
    nodes = {}
    jobs = []

    f = open('%s/%s' % (os.path.dirname(__file__), config), 'r')
    for line in f:
        line = line.replace('\n', '')
        job_data = line.split(' ')
        if len(job_data) > 2:
            job_id = int(job_data[0])
            job = _get_job_by_id(job_id, jobs)
            job = Job(job_id) if job is None else job

            if '-' not in job_data[1]:
                successors_ids = job_data[1].split(',')
                for sjob_id in successors_ids:
                    sjob_id = int(sjob_id)
                    pjob = _get_job_by_id(sjob_id, jobs)
                    pjob = Job(sjob_id) if pjob is None else pjob
                    job.successors.append(pjob)
                    pjob.predecessors.append(job)
                    jobs.append(pjob)

                jobs.append(job)
                _init_node(job_id, job_data, nodes)
            else:
                jobs.append(job)
                _init_node(job_id, job_data, nodes)

    return {'jobs': jobs, 'nodes': nodes}

data = _init_jobs_and_nodes_no_deps('japinstance1')
episode = Episode(data['jobs'], data['nodes'], 7)
episode.execute()