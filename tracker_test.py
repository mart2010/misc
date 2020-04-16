import pytest
from datetime import datetime 
from tracker import *


def test_tickerEventTracker():

    fivemin_ago = datetime.now().timestamp() - 60*5
    pair = 'XTZUSD'
    tracker = TickerEventTracker(pair, lo=1.1, hi=1.2, max_daily=10.0, min_daily=-10.0)
    mockservice = SimpleTickerDataFeed()
    mockservice.url = "bitstamp"
    tracker.datafeed_service = mockservice
    tracker.setup()
    #print("le tracker est {}".format(tracker))

    
    # 1 
    resp1 = dict(last=0.91, open=1.0, timestamp=fivemin_ago)
    msgs = tracker.signal_events(resp1)
    assert len(msgs) == 0

    # 2 
    resp2 = dict(last=1.101, open=1.0, timestamp=fivemin_ago+10)
    msgs = tracker.signal_events(resp2)
    assert msgs[0] == 'Pair XTZUSD at 1.101 (prev=0.910) enter (up) the range [1.100-1.200]'
    assert msgs[1] == 'Pair XTZUSD at 1.101 changes 10.10% from open value (1.0)'
    assert len(msgs) == 2
    
    # 3 
    resp3 = dict(last=1.09, open=1.0, timestamp=fivemin_ago+20)
    msgs = tracker.signal_events(resp3)
    assert msgs[0] == 'Pair XTZUSD at 1.090 (prev=1.101) exit (down) the range [1.100-1.200]'
    assert len(msgs) == 1

    # 4 reenter and exceeded max_daily within "wait time" 
    resp4 = dict(last=1.15, open=1.0, timestamp=fivemin_ago+30)
    msgs = tracker.signal_events(resp4)
    assert len(msgs) == 0

    # 5 reexit and exceeded max_daily past "wait time" 
    resp5 = dict(last=0.8, open=1.0, timestamp=fivemin_ago+30000)
    msgs = tracker.signal_events(resp5)
    assert msgs[0] == 'Pair XTZUSD at 0.800 (prev=1.150) exit (down) the range [1.100-1.200]'
    assert msgs[1] == 'Pair XTZUSD at 0.800 changes -20.00% from open value (1.0)'
    assert len(msgs) == 2

    # 6 cross (new event) and exceeded max_daily 
    resp6 = dict(last=1.5, open=1.0, timestamp=fivemin_ago+30010)
    msgs = tracker.signal_events(resp6)
    assert msgs[0] == 'Pair XTZUSD at 1.500 (prev=0.800) cross (up) the range [1.100-1.200]'
    assert msgs[1] == 'Pair XTZUSD at 1.500 changes 50.00% from open value (1.0)'
    assert len(msgs) == 2

