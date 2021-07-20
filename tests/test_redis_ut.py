import time
import pytest
from threading import Thread
from pympler.tracker import SummaryTracker
from swsscommon import swsscommon
from swsscommon.swsscommon import ConfigDBPipeConnector, DBInterface, SonicV2Connector, SonicDBConfig, ConfigDBConnector
import json

existing_file = "./tests/redis_multi_db_ut_config/database_config.json"

@pytest.fixture(scope="session", autouse=True)
def prepare(request):
    SonicDBConfig.initialize(existing_file)

def test_ProducerTable():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    ps = swsscommon.ProducerTable(db, "abc")
    cs = swsscommon.ConsumerTable(db, "abc")
    fvs = swsscommon.FieldValuePairs([('a','b')])
    ps.set("bbb", fvs)
    (key, op, cfvs) = cs.pop()
    assert key == "bbb"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

def test_ProducerStateTable():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    ps = swsscommon.ProducerStateTable(db, "abc")
    cs = swsscommon.ConsumerStateTable(db, "abc")
    fvs = swsscommon.FieldValuePairs([('a','b')])
    ps.set("aaa", fvs)
    (key, op, cfvs) = cs.pop()
    assert key == "aaa"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

def test_Table():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    tbl = swsscommon.Table(db, "test_TABLE")
    fvs = swsscommon.FieldValuePairs([('a','b'), ('c', 'd')])
    tbl.set("aaa", fvs)
    keys = tbl.getKeys()
    assert len(keys) == 1
    assert keys[0] == "aaa"
    (status, fvs) = tbl.get("aaa")
    assert status == True
    assert len(fvs) == 2
    assert fvs[0] == ('a', 'b')
    assert fvs[1] == ('c', 'd')
    alltable = db.hgetall("test_TABLE:aaa")
    assert len(alltable) == 2
    assert alltable['a'] == 'b'

def test_SubscriberStateTable():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    db.flushdb()
    t = swsscommon.Table(db, "testsst")
    sel = swsscommon.Select()
    cst = swsscommon.SubscriberStateTable(db, "testsst")
    sel.addSelectable(cst)
    fvs = swsscommon.FieldValuePairs([('a','b')])
    t.set("aaa", fvs)
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

def thread_test_func():
    print("Start thread: thread_test_func")
    time.sleep(2)
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    t = swsscommon.Table(db, "testsst")
    fvs = swsscommon.FieldValuePairs([('a','b')])
    t.set("aaa", fvs)
    print("Leave thread: thread_test_func")

def test_SelectYield():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    db.flushdb()
    sel = swsscommon.Select()
    cst = swsscommon.SubscriberStateTable(db, "testsst")
    sel.addSelectable(cst)

    print("Spawning thread: thread_test_func")
    test_thread = Thread(target=thread_test_func)
    test_thread.start()

    while True:
        # timeout 10s is too long and indicates thread hanging
        (state, c) = sel.select(10000)
        if state == swsscommon.Select.OBJECT:
            break
        elif state == swsscommon.Select.TIMEOUT:
            assert False

    test_thread.join()
    (key, op, cfvs) = cst.pop()
    assert key == "aaa"
    assert op == "SET"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

def test_Notification():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    ntfc = swsscommon.NotificationConsumer(db, "testntf")
    sel = swsscommon.Select()
    sel.addSelectable(ntfc)
    fvs = swsscommon.FieldValuePairs([('a','b')])
    ntfp = swsscommon.NotificationProducer(db, "testntf")
    ntfp.send("aaa", "bbb", fvs)
    (state, c) = sel.select()
    assert state == swsscommon.Select.OBJECT
    (op, data, cfvs) = ntfc.pop()
    assert op == "aaa"
    assert data == "bbb"
    assert len(cfvs) == 1
    assert cfvs[0] == ('a', 'b')

def test_DBConnectorRedisClientName():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    time.sleep(1)
    assert db.getClientName() == ""
    client_name = "foo"
    db.setClientName(client_name)
    time.sleep(1)
    assert db.getClientName() == client_name
    client_name = "bar"
    db.setClientName(client_name)
    time.sleep(1)
    assert db.getClientName() == client_name
    client_name = "foobar"
    db.setClientName(client_name)
    time.sleep(1)
    assert db.getClientName() == client_name


def test_SelectMemoryLeak():
    N = 50000
    def table_set(t, state):
        fvs = swsscommon.FieldValuePairs([("status", state)])
        t.set("123", fvs)

    def generator_SelectMemoryLeak():
        app_db = swsscommon.DBConnector("APPL_DB", 0, True)
        t = swsscommon.Table(app_db, "TABLE")
        for i in range(int(N/2)):
            table_set(t, "up")
            table_set(t, "down")

    tracker = SummaryTracker()
    appl_db = swsscommon.DBConnector("APPL_DB", 0, True)
    sel = swsscommon.Select()
    sst = swsscommon.SubscriberStateTable(appl_db, "TABLE")
    sel.addSelectable(sst)
    thr = Thread(target=generator_SelectMemoryLeak)
    thr.daemon = True
    thr.start()
    time.sleep(5)
    for _ in range(N):
        state, c = sel.select(1000)
    diff = tracker.diff()
    cases = []
    for name, count, _ in diff:
        if count >= N:
            cases.append("%s - %d objects for %d repeats" % (name, count, N))
    thr.join()
    assert not cases


def test_DBInterface():
    dbintf = DBInterface()
    dbintf.set_redis_kwargs("", "127.0.0.1", 6379)
    dbintf.connect(15, "TEST_DB")

    db = SonicV2Connector(use_unix_socket_path=True, namespace='')
    assert db.TEST_DB == 'TEST_DB'
    assert db.namespace == ''
    db.connect("TEST_DB")
    db.set("TEST_DB", "key0", "field1", "value2")
    fvs = db.get_all("TEST_DB", "key0")
    assert "field1" in fvs
    assert fvs["field1"] == "value2"
    try:
        json.dumps(fvs)
    except:
        assert False, 'Unexpected exception raised in json dumps'

    # Test keys
    ks = db.keys("TEST_DB", "key*");
    assert len(ks) == 1
    ks = db.keys("TEST_DB", u"key*");
    assert len(ks) == 1

    # Test del
    db.set("TEST_DB", "key3", "field4", "value5")
    deleted = db.delete("TEST_DB", "key3")
    assert deleted == 1
    deleted = db.delete("TEST_DB", "key3")
    assert deleted == 0

    # Test pubsub
    redisclient = db.get_redis_client("TEST_DB")
    pubsub = redisclient.pubsub()
    dbid = db.get_dbid("TEST_DB")
    pubsub.psubscribe("__keyspace@{}__:pub_key*".format(dbid))
    msg = pubsub.get_message()
    assert len(msg) == 0
    db.set("TEST_DB", "pub_key", "field1", "value1")
    msg = pubsub.get_message()
    assert len(msg) == 4
    assert msg["data"] == "hset"
    assert msg["channel"] == "__keyspace@{}__:pub_key".format(dbid)
    msg = pubsub.get_message()
    assert len(msg) == 0
    db.set("TEST_DB", "pub_key", "field1", "value1")
    db.set("TEST_DB", "pub_key", "field2", "value2")
    db.set("TEST_DB", "pub_key", "field3", "value3")
    db.set("TEST_DB", "pub_key", "field4", "value4")
    msg = pubsub.get_message()
    assert len(msg) == 4
    msg = pubsub.get_message()
    assert len(msg) == 4
    msg = pubsub.get_message()
    assert len(msg) == 4
    msg = pubsub.get_message()
    assert len(msg) == 4
    msg = pubsub.get_message()
    assert len(msg) == 0

    # Test dict.get()
    assert fvs.get("field1") == "value2"
    assert fvs.get("field1_noexisting") == None
    assert fvs.get("field1", "default") == "value2"
    assert fvs.get("nonfield", "default") == "default"

    # Test dict.update()
    other = { "field1": "value3", "field4": "value4" }
    fvs.update(other)
    assert len(fvs) == 2
    assert fvs["field1"] == "value3"
    assert fvs["field4"] == "value4"
    # Test dict.update() accepts no arguments, and then no update happens
    fvs.update()
    assert len(fvs) == 2
    assert fvs["field1"] == "value3"
    assert fvs["field4"] == "value4"
    fvs.update(field5='value5', field6='value6')
    assert fvs["field5"] == "value5"
    with pytest.raises(TypeError):
        fvs.update(fvs, fvs)

    # Test blocking
    fvs = db.get_all("TEST_DB", "key0", blocking=True)
    assert "field1" in fvs
    assert fvs["field1"] == "value2"
    assert fvs.get("field1", "default") == "value2"
    assert fvs.get("nonfield", "default") == "default"

    # Test empty/none namespace
    db = SonicV2Connector(use_unix_socket_path=True, namespace=None)
    assert db.namespace == ''

    # Test default namespace parameter
    db = SonicV2Connector(use_unix_socket_path=True)
    assert db.namespace == ''

    # Test no exception
    try:
        db = SonicV2Connector(host='127.0.0.1')
        db = SonicV2Connector(use_unix_socket_path=True, namespace='', decode_responses=True)
        db = SonicV2Connector(use_unix_socket_path=False, decode_responses=True)
        db = SonicV2Connector(host="127.0.0.1", decode_responses=True)
    except:
        assert False, 'Unexpected exception raised'

    # Test exception
    with pytest.raises(ValueError):
        db = SonicV2Connector(decode_responses=False)

def test_ConfigDBConnector():
    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    config_db.get_redis_client(config_db.CONFIG_DB).flushdb()
    config_db.set_entry("TEST_PORT", "Ethernet111", {"alias": "etp1x"})
    allconfig = config_db.get_config()
    assert allconfig["TEST_PORT"]["Ethernet111"]["alias"] == "etp1x"

    config_db.set_entry("TEST_PORT", "Ethernet111", {"mtu": "12345"})
    allconfig =  config_db.get_config()
    assert "alias" not in allconfig["TEST_PORT"]["Ethernet111"]
    assert allconfig["TEST_PORT"]["Ethernet111"]["mtu"] == "12345"

    config_db.delete_table("TEST_PORT")
    allconfig =  config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBConnectorSeparator():
    db = swsscommon.DBConnector("APPL_DB", 0, True)
    config_db = ConfigDBConnector()
    config_db.db_connect("APPL_DB", False, False)
    config_db.get_redis_client(config_db.APPL_DB).flushdb()
    config_db.set_entry("TEST_PORT", "Ethernet222", {"alias": "etp2x"})
    db.set("ItemWithoutSeparator", "item11")
    allconfig = config_db.get_config()
    assert "TEST_PORT" in allconfig
    assert "ItemWithoutSeparator" not in allconfig

    alltable = config_db.get_table("*")
    assert "Ethernet222" in alltable

    config_db.delete_table("TEST_PORT")
    db.delete("ItemWithoutSeparator")
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBPipeConnector():
    config_db = ConfigDBPipeConnector()
    config_db.connect(wait_for_init=False)
    config_db.get_redis_client(config_db.CONFIG_DB).flushdb()
    config_db.set_entry("TEST_PORT", "Ethernet112", {"alias": "etp1x"})
    allconfig = config_db.get_config()
    assert allconfig["TEST_PORT"]["Ethernet112"]["alias"] == "etp1x"

    config_db.set_entry("TEST_PORT", "Ethernet112", {"mtu": "12345"})
    allconfig =  config_db.get_config()
    assert "alias" not in allconfig["TEST_PORT"]["Ethernet112"]
    assert allconfig["TEST_PORT"]["Ethernet112"]["mtu"] == "12345"

    config_db.mod_config(allconfig)
    allconfig["TEST_PORT"]["Ethernet113"] = None
    allconfig["TEST_VLAN"] = None
    config_db.mod_config(allconfig)
    allconfig.setdefault("ACL_TABLE", {}).setdefault("EVERFLOW", {})["ports"] = ["Ethernet0", "Ethernet4", "Ethernet8"]
    config_db.mod_config(allconfig)
    allconfig = config_db.get_config()

    config_db.delete_table("TEST_PORT")
    config_db.delete_table("ACL_TABLE")
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBScan():
    config_db = ConfigDBPipeConnector()
    config_db.connect(wait_for_init=False)
    config_db.get_redis_client(config_db.CONFIG_DB).flushdb()
    n = 1000
    for i in range(0, n):
        s = str(i)
        config_db.mod_entry("TEST_TYPE" + s, "Ethernet" + s, {"alias" + s: "etp" + s})

    allconfig = config_db.get_config()
    assert len(allconfig) == n

    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    allconfig = config_db.get_config()
    assert len(allconfig) == n

    for i in range(0, n):
        s = str(i)
        config_db.delete_table("TEST_TYPE" + s)

def test_ConfigDBFlush():
    config_db = ConfigDBConnector()
    config_db.connect(wait_for_init=False)
    config_db.set_entry("TEST_PORT", "Ethernet111", {"alias": "etp1x"})
    client = config_db.get_redis_client(config_db.CONFIG_DB)

    assert ConfigDBConnector.INIT_INDICATOR == "CONFIG_DB_INITIALIZED"
    assert config_db.INIT_INDICATOR == "CONFIG_DB_INITIALIZED"

    suc = client.set(config_db.INIT_INDICATOR, 1)
    assert suc
    # TODO: redis.get is not yet supported
    # indicator = client.get(config_db.INIT_INDICATOR)
    # assert indicator == '1'

    client.flushdb()
    allconfig = config_db.get_config()
    assert len(allconfig) == 0

def test_ConfigDBConnect():
    config_db = ConfigDBConnector()
    config_db.db_connect('CONFIG_DB')
    client = config_db.get_redis_client(config_db.CONFIG_DB)
    client.flushdb()
    allconfig = config_db.get_config()
    assert len(allconfig) == 0
