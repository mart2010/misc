# -*- coding: utf-8 -*-
"""
Created on Wed Mar  7 10:28:47 2018

@author: d7loz9
"""

from datetime import datetime, timedelta
import requests
import ruamel.yaml
import argparse
import importlib
import schedule
import time
import os
import smtplib
from notify_run import Notify

class Bot(object):
    def __init__(self, notification_services, datafeed_services, event_trackers):
        self.notification_services = notification_services
        self.datafeed_services = datafeed_services
        self.event_trackers = event_trackers

    def setup(self):
        for n in self.notification_services:
            n.setup()
        for s in self.datafeed_services:
            s.setup()
        for e in self.event_trackers:
            e.setup()
        self.schedule_trackers()

    def schedule_trackers(self):
        schedule.clear()
        
        for r_s in getattr(self, 'run_schedules', []):
            interval_full = r_s['interval']
            interval_time = interval_full.lstrip('0123456789 ')
            assert interval_time in ('seconds','minutes','hours')
            val_int = int(interval_full[:-len(interval_time)])
            the_tracker = r_s['tracker']
            schedule_scheme = getattr(schedule.every(val_int),interval_time)
            schedule_scheme.do(self.run_tracker, the_tracker)

    def run_tracker(self, tracker):
        request_p = getattr(tracker, 'request_params', None)
        service_response = tracker.datafeed_service.request(request_params=request_p)
        events_msg = tracker.signal_events(service_response)
        for n in self.notification_services:
            n.notify(events_msg)

    def __str__(self):
        if hasattr(self, 'run_schedules') and len(self.run_schedules) > 0:
            return "Bot has run schedules:\n\t{}".format(self.run_schedules)
        else:
            return "Bot has no run schedules"
        

class NotificationService(object):
    def __init__(self, **params):
        self.params = params

    def setup(self):
        if hasattr(self, 'active') and self.active == 'N':
            self.active = False
        else:
            self.active = True
        self._setup()

    def notify(self, events):
        if self.active and not events.is_empty:
            try:
                self._notify(events)
            except Exception as e: 
                print("{} failed to notify due to error:\n{}".format(self.__class__.__name__, e))
        
    def _setup(self):
        pass
    def _notify(events):
        pass

class ConsolNotificationService(NotificationService):
    def _notify(self, events):
        short_msgs = [s[0] for s in events]
        long_msgs = [s[1] for s in events]
        long_s = "_"*50 + "\n{nb} Event(s) signaled:\n\t{msgs}" + "\n" + "_"*50 + "\n\n"
        m = "\n\t- " + "\n\t- ".join(long_msgs)
        print("Short message--> " + " || ".join(short_msgs))
        print("Long message-->\n" + long_s.format(nb=len(long_msgs), msgs=m))


class AndroidPushNotifyService(NotificationService):
    """Use notify.run free service using "Web Push API", to push notification to 
    Android phone through a Channel (ok for non-private data --> https://notify.run)
    """
    def _setup(self):
        self.notifier = Notify()
        #not to expose my channel unecessarily, Notify is configured in ~/.config/notify-run
        
    def _notify(self, events):
        short_msgs = [s[0] for s in events]
        self.notifier.send(" || ".join(short_msgs))


class EmailNotificationService(NotificationService):
    # TODO: à finir...
    def _setup(self):
        self.smtp = self.params['smtp']
        self.to =  self.params['to'] 
        # self.server = smtplib.SMTP(self.smtp, 587)
        # self.server.starttls()
        # self.server.login(sender_email, password)
        # self.message = 'Subject: {subject}\n\n{msg}'.format(subject="ddd")

    def _notify(self, events):
        m = "\n".join(events)
        print("Notify email to {} using smtp: {}, with msg:\n{}".format(self.to, self.smtp, m))

        try:
            self.server.sendmail(sender_email, to, message)
            self.server.quit()
        except Exception as e:
            print(e)


class DataFeedService(object):
    """Responsible to fetch online live data (ticker, ..) & return a response
    """
    def __init__(self, **params):
        self.params = params

    def setup(self):
        pass

    def request(self, request_params=None):
        """Send a request and return the response as dict. This is used by the Bot which may also 
        provides a request_params if specified in the EventTracker 
        """
        pass

    def __str__(self):
        atts = ", ".join(['{}:{}'.format(k,v) for k,v in self.__dict__.items()])
        return "'{}' with attributes {}".format(self.__class__.__name__, atts) 


class SimpleTickerDataFeed(DataFeedService):
    def request(self, request_params):
        complete_url = self.url.format(**request_params)

        try:
            # r = requests.get(complete_url)
            # if r.status_code != requests.codes.ok:
            #     raise Exception("Request {} response not 200-OK: {}".format(complete_url, r))
            # response = r.json()
            # mock-up for test..
            import random
            p = str(random.uniform(2.0, 2.1))
            t = str(datetime.now().timestamp())
            if self.url.find('bitstamp') > -1:
                r = {"last": p, "timestamp": t, "volume": "8000", "open": "1.5", "high": "1", "bid": "1", "vwap":"1", "low":"1", "ask":"1"}
            elif self.url.find('kraken') > -1:
                r = {"error":[],"result":{"XTZUSD":{"a":["1.963400","1135","1135.000"],"b":["1.960200","829","829.000"],"c":[p,"169.08065753"],"v":["162651.08050865","733026.03677556"],"p":["1.929881","1.904369"],"t":["397","1553"],"l":["1.896800","1.894300"],"h":["1.993500","2.007000"],"o":"1.5"}}}
            response = r
        except requests.exceptions.RequestException as e:
            print(e)
        #print("Request at '{}', returned -->{}".format(complete_url, response))
        return response

class Events(object):
    """Super CLass to hold list of Events related to Ticker tracker as a list of tuple(short_msg, long_msg)
    """
    def __init__(self):
        self.events = []

    @property
    def is_empty(self):
        return len(self.events) == 0

    def __iter__(self):
        return iter(self.events)

class EventsTicker(Events):
    range_long = "Pair {t.symbol} at {t.current:.3f} (prev={prev.current:.3f}) {a} ({dir}) the range [{r[0]:.3f}-{r[1]:.3f}]"
    range_short = "{t.symbol} {t.current:.3f} (prev={prev.current:.3f}) {a}{dir} [{r[0]:.3f}-{r[1]:.3f}]"
    change_long = "Pair {t.symbol} at {t.current:.3f} changes {t.day_change:.2f}% from open value {t.open}"
    change_short = "{t.symbol} {t.current:.3f} changes {t.day_change:.2f}% from open {t.open}"
    dir_symbol = {'down': '\u2193', 'up': '\u2191'}
    
    def add_range_event(self, ticker, prev_ticker, action, range):
        direction = ticker.direction(prev_ticker)
        msg = self.range_long.format(t=ticker, prev=prev_ticker, a=action, dir=direction, r=range) 
        short_msg = self.range_short.format(t=ticker, prev=prev_ticker, a=action, dir=self.dir_symbol[direction], r=range) 
        self.events.append((short_msg, msg))

    def add_change_event(self, ticker):
        msg = self.change_long.format(t=ticker)
        short_msg = self.change_short.format(t=ticker)
        self.events.append((short_msg, msg))

class EventTracker(object):
    """AbstractClass for EventTracker that track/return events as a list of events.
    The datafeed_service is loosely coupled, it's Bot's responsability to call the datafeed_service  
    and to provide the request_params (when specified by the EventTracker)
    """
    def __init__(self, datafeed_service, request_params=None):
        self.datafeed_service = datafeed_service
        self.request_params = request_params

    def setup(self):
        pass

    def signal_events(self, response):
        """Signal events and return them as list of text events.
        Call by Bot using the response obtained from datafeed_service.request(self.request_params)
        """
        pass

    def __str__(self):
        atts = ", ".join(['{}:{}'.format(k,v) for k,v in self.__dict__.items()])
        return "'{}' with attributes {}".format(self.__class__.__name__, atts) 

    def __repr__(self):
        return self.__str__()




class TickerEventTracker(EventTracker):

    def signal_range_event(self):
        if not self.prev_ticker or not self.lo:
            return None
        action = None
        if not self.prev_ticker.inside(self.lo, self.hi) and self.ticker.inside(self.lo, self.hi):
            action = 'enter'
        if self.prev_ticker.inside(self.lo, self.hi) and not self.ticker.inside(self.lo, self.hi):
            action = 'exit'
        if not self.prev_ticker.inside(self.lo, self.hi) and not self.ticker.inside(self.lo, self.hi):
            if self.prev_ticker.current <= self.lo and self.ticker.current >= self.hi or \
               self.prev_ticker.current >= self.hi and self.ticker.current <= self.lo:
                action = 'cross'
        return action

    def signal_change_event(self):
        action = None
        if self.max_daily and self.ticker.day_change >= self.max_daily:
            action = 'max_daily'
        if self.min_daily and self.ticker.day_change <= self.min_daily:
            action = 'min_daily'
        return action

    def __init__(self, symbol, **params):
        self.symbol = symbol
        self.params = params

    def setup(self):
        if not self.params.get('lo') and not self.params.get('hi'):
            self.lo = None
            self.hi = None
        elif self.params.get('lo') and not self.params.get('hi'):
            self.lo = float(self.params['lo'])
            self.hi = float('inf')
        elif not self.params.get('lo') and self.params.get('hi'):
            self.hi = float(self.params['hi'])
            self.lo = 0.0
        else:
            self.lo = float(self.params['lo'])
            self.hi = float(self.params['hi'])
        self.max_daily = self.params.get('max_daily', None)
        self.min_daily = self.params.get('min_daily', None)

        self.ticker = None
        #default is to wait 1 hour before sending same event
        self.wait_time = self.params.get('wait_time',3600)

        # last time for range-events
        self.lastime_rangevents = dict(enter_up=0, enter_down=0, exit_up=0, exit_down=0, cross_up=0, cross_down=0)
        # last time for change-event
        self.lastime_changevents = dict(max_daily=0, min_daily=0) 

    def signal_events(self, response):
        ticker = ticker_response_adapter(response, self.symbol, self.datafeed_service.url)
        self.prev_ticker = self.ticker
        self.ticker = ticker
        
        evts = EventsTicker()
        range_evt = self.signal_range_event()
        if range_evt:
            rkey = range_evt + "_" + self.ticker.direction(self.prev_ticker)
            last_rtime = self.lastime_rangevents[rkey]
            if self.ticker.timestamp - last_rtime > self.wait_time:
                evts.add_range_event(self.ticker, self.prev_ticker, action=range_evt, range=(self.lo, self.hi))
                self.lastime_rangevents[rkey] = self.ticker.timestamp

        change_evt = self.signal_change_event()
        if change_evt:
            last_ctime = self.lastime_changevents[change_evt]
            if self.ticker.timestamp - last_ctime > self.wait_time:
                evts.add_change_event(self.ticker)
                self.lastime_changevents[change_evt] = self.ticker.timestamp
        if evts.is_empty:
            print("No event signaled for {}".format(self.ticker))
        return evts



def ticker_response_adapter(response, symbol, service_url):
    # Bitstamp: high->Last 24h high, low->Last 24h low, last->Last price, open->First of the day, bid->Highest buy order, ask->Lowest sell order, volume-> Last 24h vol, vwap->Last 24h vol weighted average price, timestamp->Unix timestamp date and time
    #   {"high","last", "timestamp", "bid", "vwap", "volume", "low", "ask", "open"}
    # Bitfinex: mid-> (bid+ask)/2, bid:bid, ask:ask, last_price:last order price, low:lowest since 24hrs, high:highest since 24hrs, volume:vol last 24h, timestamp:timestamp
    #   check all symbols supported: https://api.bitfinex.com/v1/symbols
    #   {"mid", "bid", "ask", "last_price", "low", "high", "volume", "timestamp"
    # Kraken: a:ask array, b:bid array, c:last price array, v:vol array, p:vol weighted array, t:nb of trade array, l:low array, h:high array, o:today's opening price
    # check all symbols: https://api.kraken.com/0/public/AssetPairs
    # {"error":[],"result":{"XXBTZUSD":{"a":["","",""],"b":["6.7","3","3.0"],"c":["6.3","0.087"],"v":["6.5","96."],"p":["7.8","71.7"],"t":[1,2],"l":["6.1","6.1"],"h":["7.5","7.0"],"o":"7.1"}}}
    # {"error":[],"result":{"XTZUSD":{"a":["1.963400","1135","1135.000"],"b":["1.960200","829","829.000"],"c":["1.953400","169.08065753"],"v":["162651.08050865","733026.03677556"],"p":["1.929881","1.904369"],"t":[397,1553],"l":["1.896800","1.894300"],"h":["1.993500","2.007000"],"o":"1.974000"}}}
    
    # Adapt any service custom responses here only:
    if service_url.lower().find('bitstamp') > -1:
        values = dict(current=  response['last'],
                      open=     response['open'],
                      timestamp=response['timestamp'],
                      high=     response.get('high',-1),
                      low=      response.get('low',-1),
                      volume=   response.get('volume',-1),
                      bid=      response.get('bid',-1),
                      ask=      response.get('ask',-1),
                      vwap=     response.get('vwap',-1))
    elif service_url.lower().find('kraken') > -1:
        # we need the symbol (pair)
        symbol_key = symbol.upper()
        values = dict(current=  response['result'][symbol_key]['c'][0],
                      open=     response['result'][symbol_key]['o'],
                      high=     response['result'][symbol_key]['h'][0],
                      low=      response['result'][symbol_key]['l'][0],
                      volume=   response['result'][symbol_key]['p'][0],
                      bid=      response['result'][symbol_key]['b'][0],
                      ask=      response['result'][symbol_key]['a'][0],
                      vwap=     response['result'][symbol_key]['p'][1])
    else:
        raise Exception("Unsupported service:{}".format(service_url))
    
    return Ticker(symbol, values)

class Ticker(object):
    def __init__(self, symbol, values):
        self.symbol = symbol
        if type(values) != dict:
            raise Exception("Unsupported values: {}".format(values))
        
        # required attributes
        self.current = float(values['current'])
        self.open = float(values['open'])
        # use now() when not available
        if values.get('timestamp'):
            self.timestamp = round(float(values['timestamp']))
        else:
            self.timestamp = round(datetime.utcnow().timestamp())

        # OHLC values and other possible values
        self.set_float_values(values, ('high','low','volume','bid','ask','vwap','mid'))

        
    def set_float_values(self, values, keys):
        for k in keys:
            setattr(self, k, float(values.get(k,-1)))

    @property
    def day_change(self):
        return (self.current - self.open) / self.open * 100.0

    def inside(self, range_lo, range_hi):
        return range_lo < self.current < range_hi

    def direction(self, prev_ticker):
        if not prev_ticker:
            return 'nil'
        if self.current > prev_ticker.current:
            return 'up'
        elif self.current < prev_ticker.current:
            return 'down'
        else:
            return 'flat'  

    def __str__(self):
        n = datetime.fromtimestamp(self.timestamp)
        return "Ticker {t.symbol} at {t.current:.3f} (open={t.open:.3f}, vol={t.volume:.1f}, time={now})".format(t=self, now=n)


def setup_bot(yaml_file):
    """Main entry to configure and return a Bot based on YAML config file.
    It registers yaml classes, load YAML file and setup bot.
    """
    def register_classes(classes):
        for c in classes:
            yaml.register_class(c)

    yaml = ruamel.yaml.YAML()
    register_classes((Bot, NotificationService, 
                    ConsolNotificationService, EmailNotificationService, AndroidPushNotifyService,
                    DataFeedService, SimpleTickerDataFeed, 
                    EventTracker, TickerEventTracker))
    with open(yaml_file) as yf:
        bot = yaml.load(yf)
    bot.setup()
    print("Finished setting up Bot--> {}".format(bot))
    return bot
    

def get_args():
    parser = argparse.ArgumentParser(description="Bot managing EventTracker(s) and sending their events to NotificationService(s) based on yaml config file")
    parser.add_argument("-y", "--yaml", default="./tracker_conf.yaml", help="Yaml config file")
    return parser.parse_args()
              
if __name__ == '__main__':
    args = get_args()
    if not os.path.exists(args.yaml):
        exit("YAML file '{0}' not found, exit program!".format(args.yaml))
        
    bot = setup_bot(args.yaml)
    last_yaml_update = os.path.getmtime(args.yaml)
    print("Running schedules, press Ctrl-C to stop!")
    try:
        while True:
            if last_yaml_update == os.path.getmtime(args.yaml):
                schedule.run_pending()
            else:
                del(bot)
                print("Yaml config file {} was changed, resetting the Bot!".format(args.yaml))
                bot = setup_bot(args.yaml)
                last_yaml_update = os.path.getmtime(args.yaml)
            time.sleep(1)
    except KeyboardInterrup:
        pass
        

