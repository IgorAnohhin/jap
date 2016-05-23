__author__ = 'v-iganoh'


class Job:

    def __init__(self, jid):
        self.id = jid
        self.predecessors = []
        self.successors = []
        self.estimate = 0
        self.current_completion_time = 0
        self.overall_completion_time = []

    def add_predecessor(self, predecessor):
        self.predecessors.append(predecessor)

    def remove_predecessor(self, predecessor):
        self.predecessors.remove(predecessor)

    def add_successor(self, successor):
        self.successors.append(successor)

    def remove_successor(self, successor):
        self.successors.remove(successor)

    def execute(self):
        print "Job '%s' is running..." % self.id