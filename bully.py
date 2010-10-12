from twisted.internet import task
from twisted.trial import unittest


class Bully(object):
    state = 'unknown'
    callID = None

    def __init__(self, clock, node_id, transport, service):
        self.clock = clock
        self.this_id = node_id
        self.transport = transport
        self.master_id = None
        self.known = []
        self.service = service
        self.election = None

    def join(self, node_id):
        """
        Called when a node joins the network.
        """
        self.transport.send(node_id, 'hello', self.this_id)
        self.known.append(node_id)
        #if self.state == 'leader':
        #    # FIXME: is this needed, since we're sending a hello
        #    # message to it anyway.
        #    self.transport.send(node_id, 'leader', self.this_id)
        #else:
        #    self.transport.send(node_id, 'shoot-down')

    def leave(self, node_id):
        """
        Called when a node leaves the network.
        """
        self.known.remove(node_id)
        if self.master_id == node_id:
            self._start_election()

    def _proclaim(self):
        """
        Proclaim that we are the leader.
        """
        for node_id in self.known:
            self.transport.send(node_id, 'leader', self.this_id)
        self.state = 'leader'
        self.master_id = self.this_id
        self.service.setUp()

    def _restart_election(self):
        """
        Restart election.
        """
        self.callID = None
        self.election = 0
        self._start_election()

    def shot_down(self, node_id):
        """
        This node got shut down.
        """
        assert node_id > self.this_id
        if self.callID is not None:
            self.callID.cancel()
            self.callID = None
        self.election = 0
        if self.state == 'leader':
            self.service.tearDown()
        self.state = 'waiting'
        self.callID = self.clock.callLater(5, self._restart_election)

    def leader(self, node_id):
        """
        A node proclaimed that it is the new leader.
        """
        assert node_id > self.this_id
        if self.callID is not None:
            self.callID.cancel()
            self.callID = None
        self.election = 0
        self.master_id = node_id
        if self.state == 'leader':
            self.service.tearDown()
        self.state = 'slave'

    def _start_election(self):
        """
        start an election
        """
        if not self.election:
            for node_id in self.known:
                if node_id > self.this_id:
                    self.transport.send(node_id, 'elect', self.this_id)
            self.election = 1
            self.callID = self.clock.callLater(5, self._proclaim)

    def elect(self, node_id):
        """
        Received an "ELECT" message from C{node_id}.
        """
        if node_id < self.this_id:
            self.transport.send(node_id, 'shoot-down')
        self._start_election()


class MockTransport:

    def __init__(self):
        self.tx = []

    def send(self, *args):
        self.tx.append(args)


class MockService:
    running = 0

    def setUp(self):
        self.running = 1

    def tearDown(self):
        self.running = 0


class BullyTestCase(unittest.TestCase):

    def setUp(self):
        self.service = MockService()
        self.transport = MockTransport()
        self.clock = task.Clock()

    def test_newLeaderJoins(self):
        """
        Verify that a leader can re-join.
        """
        # setup
        self.bully = Bully(self.clock, 4, self.transport, self.service)
        self.bully.known = []
        self.bully.master_id = 4
        self.bully.state = 'leader'
        # test
        self.bully.join(8)		# new leader joins
        # sends out a leader message
        self.bully.leader(8)
        self.assertEquals(self.bully.state, 'slave')

    def test_dieInElection(self):
        """
        Verify that election is restarted if we do not receive a
        leader within a certain time.
        """
        # setup
        self.bully = Bully(self.clock, 4, self.transport, self.service)
        self.bully.known = [5, 6]
        self.bully.master_id = 6
        # test
        self.bully.leave(6)		# the leader dies
        self.assertEquals(len(self.transport.tx), 1)
        self.assertEquals(self.transport.tx[0], (5, 'elect', 4))
        self.assertTrue(self.bully.election)
        # assume that node six was still alive
        self.bully.shot_down(6)
        self.clock.advance(5)
        # at this point the election should have finished
        # restart
        self.assertEquals(len(self.transport.tx), 2)
        self.assertEquals(self.transport.tx[1], (5, 'elect', 4))
        self.clock.advance(5)
        self.assertEquals(self.bully.state, 'leader')

    def test_nextSeesLeaderDie(self):
        """
        Verify that this node steps up and take the role of leader if
        the leader dies.
        """
        # setup
        self.bully = Bully(self.clock, 4, self.transport, self.service)
        self.bully.known = [1, 2, 3, 7]
        self.bully.master_id = 7
        # test
        self.bully.leave(7)		# the leader dies
        self.assertTrue(self.bully.election)
        self.clock.advance(5)
        self.assertEquals(self.bully.state, 'leader')
        self.assertIn((1, 'leader', 4), self.transport.tx)
        self.assertIn((2, 'leader', 4), self.transport.tx)
        self.assertIn((3, 'leader', 4), self.transport.tx)

    def test_middleSeesLeaderDie(self):
        """
        Verify what happens when a middle node sees that leader die
        before hearing anything else from the other nodes.
        """
        # setup
        self.bully = Bully(self.clock, 4, self.transport, self.service)
        self.bully.known = [1, 2, 3, 5, 6, 7]
        self.bully.master_id = 7
        # test
        self.bully.leave(7)		# the leader dies
        self.assertEquals(self.transport.tx[0], (5, 'elect', 4))
        self.assertEquals(self.transport.tx[1], (6, 'elect', 4))
        self.assertTrue(self.bully.election)
        # assume that node six was still alive
        self.bully.shot_down(5)
        self.bully.shot_down(6)
        self.assertFalse(self.bully.election)
        self.bully.leader(6)
        self.assertEquals(self.bully.master_id, 6)

    def test_allDiesProclaimLeader(self):
        """
        Verify that if all nodes die we proclaim ourselves the leader.
        """
        # setup
        self.bully = Bully(self.clock, 4, self.transport, self.service)
        self.bully.known = [1, 2, 3, 5, 6, 7]
        self.bully.master_id = 7
        # test
        self.bully.leave(1)
        self.assertEquals(len(self.transport.tx), 0)
        self.bully.leave(2)
        self.assertEquals(len(self.transport.tx), 0)
        self.bully.leave(3)
        self.assertEquals(len(self.transport.tx), 0)
        self.bully.leave(5)
        self.assertEquals(len(self.transport.tx), 0)
        self.bully.leave(7)
        self.assertEquals(len(self.transport.tx), 1)	# ping 6
        self.bully.leave(6)
        self.assertTrue(self.bully.election)
        self.clock.advance(5)
        self.assertEquals(self.bully.state, 'leader')
