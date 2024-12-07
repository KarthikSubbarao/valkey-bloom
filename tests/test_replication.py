import pytest
from valkey import ResponseError
from valkeytests.valkey_test_case import ReplicationTestCase
from valkeytests.conftest import resource_port_tracker
import os

class TestBloomReplication(ReplicationTestCase):

    # Global Parameterized Configs
    use_random_seed = 'no'

    def get_custom_args(self):
        self.set_server_version(os.environ['SERVER_VERSION'])
        return {
            'loadmodule': os.getenv('MODULE_PATH'),
            'bf.bloom-use-random-seed': self.use_random_seed,
        }

    @pytest.fixture(autouse=True)
    def use_random_seed_fixture(self, bloom_config_parameterization):
        if bloom_config_parameterization == "random-seed":
            self.use_random_seed = "yes"
        elif bloom_config_parameterization == "fixed-seed":
            self.use_random_seed = "no"

    def test_replication_behavior(self):
        self.setup_replication(num_replicas=1)
        # Test replication for write commands.
        bloom_write_cmds = [
            ('BF.ADD', 'BF.ADD key item', 'BF.ADD key item1', 1),
            ('BF.MADD', 'BF.MADD key item', 'BF.MADD key item1', 1),
            ('BF.RESERVE', 'BF.RESERVE key 0.001 100000', 'BF.ADD key item1', 1),
            ('BF.INSERT', 'BF.INSERT key items item', 'BF.INSERT key items item1', 2),
        ]
        for test_case in bloom_write_cmds:
            prefix = test_case[0]
            create_cmd = test_case[1]
            # New bloom object being created is replicated.
            # Validate that the bloom object creation command replicated as BF.INSERT.
            self.client.execute_command(create_cmd)
            assert self.client.execute_command('EXISTS key') == 1
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('EXISTS key') == 1
            primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
            assert primary_cmd_stats["calls"] == 1
            replica_insert_cmd_stats = self.replicas[0].client.info("Commandstats")['cmdstat_BF.INSERT']
            assert replica_insert_cmd_stats["calls"] == 1

            # New item added to an existing bloom is replicated.
            item_add_cmd = test_case[2]
            self.client.execute_command(item_add_cmd)
            assert self.client.execute_command('BF.EXISTS key item1') == 1
            self.waitForReplicaToSyncUp(self.replicas[0])
            assert self.replicas[0].client.execute_command('BF.EXISTS key item1') == 1
            # Validate that item addition (not bloom creation) is using the original command
            if prefix != 'BF.RESERVE':
                primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
                assert primary_cmd_stats["calls"] == 2
                expected_calls = test_case[3]
                replica_cmd_stats = self.replicas[0].client.info("Commandstats")['cmdstat_' + prefix]
                assert replica_cmd_stats["calls"] == expected_calls
            else:
                primary_cmd_stats = self.client.info("Commandstats")
                replica_cmd_stats = self.replicas[0].client.info("Commandstats")
                assert primary_cmd_stats['cmdstat_BF.RESERVE']["calls"] == 1 and primary_cmd_stats['cmdstat_BF.ADD']["calls"] == 1
                # In case of the BF.RESERVE test case, we use BF.ADD to add items. Validate this is replicated.
                assert replica_cmd_stats['cmdstat_BF.ADD']["calls"] == 1 and replica_cmd_stats['cmdstat_BF.INSERT']["calls"] == 1

            # Attempting to add an existing item to an existing bloom will NOT replicated.
            self.client.execute_command(item_add_cmd)
            self.waitForReplicaToSyncUp(self.replicas[0])
            primary_cmd_stats = self.client.info("Commandstats")
            replica_cmd_stats = self.replicas[0].client.info("Commandstats")
            if prefix != 'BF.RESERVE':
                primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
                assert primary_cmd_stats["calls"] == 3
                expected_calls = test_case[3]
                replica_cmd_stats = self.replicas[0].client.info("Commandstats")['cmdstat_' + prefix]
                assert replica_cmd_stats["calls"] == expected_calls
            else:
                primary_cmd_stats = self.client.info("Commandstats")
                replica_cmd_stats = self.replicas[0].client.info("Commandstats")
                assert primary_cmd_stats['cmdstat_BF.RESERVE']["calls"] == 1 and primary_cmd_stats['cmdstat_BF.ADD']["calls"] == 2
                # In case of the BF.RESERVE test case, we use BF.ADD to add items. Validate this is not replicated since
                # the item already exists.
                assert replica_cmd_stats['cmdstat_BF.ADD']["calls"] == 1 and replica_cmd_stats['cmdstat_BF.INSERT']["calls"] == 1

            # cmd debug digest
            server_digest_primary = self.client.debug_digest()
            assert server_digest_primary != None or 0000000000000000000000000000000000000000
            server_digest_replica = self.client.debug_digest()
            assert server_digest_primary == server_digest_replica
            object_digest_primary = self.client.execute_command('DEBUG DIGEST-VALUE key')
            debug_digest_replica = self.replicas[0].client.execute_command('DEBUG DIGEST-VALUE key')
            assert object_digest_primary == debug_digest_replica

            self.client.execute_command('FLUSHALL')
            self.waitForReplicaToSyncUp(self.replicas[0])
            self.client.execute_command('CONFIG RESETSTAT')
            self.replicas[0].client.execute_command('CONFIG RESETSTAT')

        self.client.execute_command('BF.ADD key item1')
        self.waitForReplicaToSyncUp(self.replicas[0])

        # Read commands executed on the primary will not be replicated.
        read_commands = [
            ('BF.EXISTS', 'BF.EXISTS key item1', 1),
            ('BF.MEXISTS', 'BF.MEXISTS key item1 item2', 1),
            ('BF.INFO', 'BF.INFO key', 1),
            ('BF.INFO', 'BF.INFO key Capacity', 2),
            ('BF.INFO', 'BF.INFO key ITEMS', 3),
            ('BF.INFO', 'BF.INFO key filters', 4),
            ('BF.INFO', 'BF.INFO key size', 5),
            ('BF.INFO', 'BF.INFO key expansion', 6),
            ('BF.CARD', 'BF.CARD key', 1)
        ]
        for test_case in read_commands:
            prefix = test_case[0]
            cmd = test_case[1]
            expected_primary_calls = test_case[2]
            self.client.execute_command(cmd)
            primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
            assert primary_cmd_stats["calls"] == expected_primary_calls
            assert ('cmdstat_' + prefix) not in self.replicas[0].client.info("Commandstats")

        # Deletes of bloom objects are replicated
        assert self.client.execute_command("EXISTS key") == 1
        assert self.replicas[0].client.execute_command('EXISTS key') == 1
        assert self.client.execute_command("DEL key") == 1
        self.waitForReplicaToSyncUp(self.replicas[0])
        assert self.client.execute_command("EXISTS key") == 0
        assert self.replicas[0].client.execute_command('EXISTS key') == 0

        self.client.execute_command('CONFIG RESETSTAT')
        self.replicas[0].client.execute_command('CONFIG RESETSTAT')

        # Write commands with errors are not replicated.
        invalid_bloom_write_cmds = [
            ('BF.ADD', 'BF.ADD key item1 item2'),
            ('BF.MADD', 'BF.MADD key'),
            ('BF.RESERVE', 'BF.RESERVE key 1.001 100000'),
            ('BF.INSERT', 'BF.INSERT key CAPACITY 0 items item'),
        ]
        for test_case in invalid_bloom_write_cmds:
            prefix = test_case[0]
            cmd = test_case[1]
            try:
                self.client.execute_command(cmd)
                assert False
            except ResponseError as e:
                pass
            primary_cmd_stats = self.client.info("Commandstats")['cmdstat_' + prefix]
            assert primary_cmd_stats["calls"] == 1
            assert primary_cmd_stats["failed_calls"] == 1
            assert ('cmdstat_' + prefix) not in self.replicas[0].client.info("Commandstats")

    # TODO: Review all tests through package and identify any flaky test/code and deflake it.
    def test_deterministic_replication(self):
        self.setup_replication(num_replicas=1)
        # Set non default global properties (config) on the primary node. Any bloom creation on the primary should be
        # replicated with the properties below.
        assert self.client.execute_command('CONFIG SET bf.bloom-capacity 1000') == b'OK'
        assert self.client.execute_command('CONFIG SET bf.bloom-expansion 3') == b'OK'
        # Test bloom object creation with every command type.
        bloom_write_cmds = [
            ('BF.ADD', 'BF.ADD key item'),
            ('BF.MADD', 'BF.MADD key item'),
            ('BF.RESERVE', 'BF.RESERVE key 0.001 100000'),
            ('BF.INSERT', 'BF.INSERT key items item'),
        ]
        for test_case in bloom_write_cmds:
            prefix = test_case[0]
            create_cmd = test_case[1]
            self.client.execute_command(create_cmd)
            server_digest_primary = self.client.debug_digest()
            assert server_digest_primary != None or 0000000000000000000000000000000000000000
            server_digest_replica = self.client.debug_digest()
            object_digest_primary = self.client.execute_command('DEBUG DIGEST-VALUE key')
            debug_digest_replica = self.replicas[0].client.execute_command('DEBUG DIGEST-VALUE key')
            assert server_digest_primary == server_digest_replica
            assert object_digest_primary == debug_digest_replica
            self.client.execute_command('FLUSHALL')
            self.waitForReplicaToSyncUp(self.replicas[0])
