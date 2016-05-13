__author__ = 'v-iganoh'
import os
from job import Job
from node import Node
from episode import Episode


def _init_jobs_and_nodes_with_desp(j_count):
    nodes = {}
    jobs = []

    j_id = 1
    while j_id <= j_count:
        jobs.append(Job(j_id))
        j_id += 1

    for inx, job in jobs:
        job.add_predecessor()


def _init_jobs_and_nodes_no_deps(config):
    nodes = {}
    jobs = []

    f = open('%s/%s' % (os.path.dirname(__file__), config), 'r')
    for line in f:
        line = line.replace('\n', '')
        job_data = line.split(' ')
        if len(job_data) > 2:
            job_id = int(job_data[0])
            job = Job(job_id)
            if '-' not in job_data[1]:
                print 'dependencies'
            else:
                jobs.append(job)

            nodes_count = len(job_data) - 2
            for c in range(nodes_count):
                if job_id in nodes:
                    nodes[job_id].append(Node(int(c+1), int(job_data[c+2])))
                else:
                    nodes[job_id] = []
                    nodes[job_id].append(Node(int(c+1), int(job_data[c+2])))

    return {'jobs': jobs, 'nodes': nodes}

data = _init_jobs_and_nodes_no_deps('japinstance1')
episode = Episode(data['jobs'], data['nodes'], 10)
episode.execute()