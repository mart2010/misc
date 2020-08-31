import pytest
from datetime import datetime 
from tracker import *


def test_tickerEventTracker():

    fivemin_ago = datetime.now().timestamp() - 60*5
    pair = 'XTZUSD'
    mockservice = SimpleTickerDataFeed()
    mockservice.url = "bitstamp"
    tracker = TickerEventTracker(mockservice, symbol=pair, ranges=['1.1-1.2', '10-12'], max_day=10.0, max_lag=[20.0, -1])
    tracker.setup()
    # reset wait_time
    tracker.wait_time = 60
    print("le tracker est {}".format(tracker))

    
    # 1 
    resp1 = dict(last=0.91, open=1.0, timestamp=fivemin_ago)
    msgs = tracker.signal_events(resp1)
    assert len(msgs) == 0

    # 2 
    resp2 = dict(last=1.101, open=1.0, timestamp=fivemin_ago+10)
    msgs = tracker.signal_events(resp2)
    assert msgs[0].text == 'XTZUSD at 1.101 (prev=0.910) enter{} [1.100-1.200]'.format(tracker.dir_symbol['up'])
    assert msgs[1].text == 'XTZUSD at 1.101 changes 10.10% from open 1.0'
    assert msgs[2].text == 'XTZUSD at 1.101 changes 20.99% from lag-1(0.910)'
    assert len(msgs) == 3
    
    # "{symbol} at {price:.3f} changes {change:.2f}% from lag-{lag}"

    # 3 
    resp3 = dict(last=1.09, open=1.0, timestamp=fivemin_ago+20)
    msgs = tracker.signal_events(resp3)
    assert msgs[0].text == 'XTZUSD at 1.090 (prev=1.101) exit{} [1.100-1.200]'.format(tracker.dir_symbol['down'])
    assert len(msgs) == 1

    # 4 reenter and exceeded change_day within "wait time"
    resp4 = dict(last=1.15, open=1.0, timestamp=fivemin_ago+30)
    msgs = tracker.signal_events(resp4)
    assert len(msgs) == 0

    # 5 reexit and exceeded change_day past "wait time" 
    resp5 = dict(last=0.8, open=1.0, timestamp=fivemin_ago+30000)
    msgs = tracker.signal_events(resp5)
    assert msgs[0].text == 'XTZUSD at 0.800 (prev=1.150) exit{} [1.100-1.200]'.format(tracker.dir_symbol['down'])
    assert msgs[1].text == 'XTZUSD at 0.800 changes -20.00% from open 1.0'
    assert msgs[2].text == 'XTZUSD at 0.800 changes -30.43% from lag-1(1.150)'
    assert len(msgs) == 3

    # 6 cross (new event) and exceeded change_day 
    resp6 = dict(last=1.5, open=1.0, timestamp=fivemin_ago+30100)
    msgs = tracker.signal_events(resp6)
    assert msgs[0].text == 'XTZUSD at 1.500 (prev=0.800) cross{} [1.100-1.200]'.format(tracker.dir_symbol['up'])
    assert msgs[1].text == 'XTZUSD at 1.500 changes 50.00% from open 1.0'
    assert msgs[2].text == 'XTZUSD at 1.500 changes 87.50% from lag-1(0.800)'
    assert len(msgs) == 3

