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
        for r_s in self.run_schedules:
            interval_full = r_s['interval']
            interval_time = interval_full.lstrip('0123456789 ')
            val_int = int(interval_full[:-len(interval_time)])
            the_tracker = r_s['tracker']
            if interval_time == 'sec':
                schedule.every(val_int).seconds.do(self.run_tracker, the_tracker)
            elif interval_time == 'min':
                schedule.every(val_int).minutes.do(self.run_tracker, the_tracker)
            elif interval_time == 'hour':
                schedule.every(val_int).hours.do(self.run_tracker, the_tracker)
            else:
                raise Exception("Interval {} not supported".format(interval_time))

    def run_tracker(self, tracker):
        request_p = None
        if hasattr(tracker, 'request_params'):
            request_p = tracker.request_params
        service_resp = tracker.datafeed_service.request(request_params=request_p)
        msgs = tracker.signal_events(service_resp)
        for n in self.notification_services:
            n.notify(msgs)

    def launch_schedulers(self):
        if hasattr(self, 'run_schedules') and len(self.run_schedules) > 0:
            print("Running schedules, press Ctrl-C to stop!")
            try:
                while True:
                    schedule.run_pending()
                    time.sleep(1)
            except KeyboardInterrup:
                pass
        else:
            print("No schedulers to run, exit program!")

    def __str__(self):
        return "Bot for EventTrackers: {}".format(self.event_trackers)



class NotificationService(object):
    def __init__(self, **params):
        self.params = params

    def setup(self):
        if hasattr(self, 'active') and self.active == 'N':
            self.active = False
        else:
            self.active = True
        self._setup()

    def notify(self, messages):
        if self.active and len(messages) > 0:
            self._notify(messages)

    def short_msg(self, messages):
        short_msg = "Event signaled: "
        m = " || ".join(messages)
        return short_msg + m
        
    def long_msg(self, messages):
        long_msg = "_"*50 + "\n{nb} Events signaled:\n\t{msgs}" + "\n" + "_"*50 + "\n\n"
        m = "\n\t- " + "\n\t- ".join(messages)
        return long_msg.format(nb=len(messages), msgs=m) 

    def _setup(self):
        pass
    def _notify(messages):
        pass

class ConsolNotificationService(NotificationService):
    def notify(self, messages):
        if len(messages) == 0:
            print("No messages signaled at {}".format(datetime.now()))
        else:
            #print("Short message--> " + self.short_msg(messages))
            print("Long message-->\n" + self.long_msg(messages))

class EmailNotificationService(NotificationService):
    # TODO: à finir...
    def _setup(self):
        self.smtp = self.params['smtp']
        self.to =  self.params['to'] 
        # self.server = smtplib.SMTP(self.smtp, 587)
        # self.server.starttls()
        # self.server.login(sender_email, password)
        # self.message = 'Subject: {subject}\n\n{msg}'.format(subject="ddd")

    def _notify(self, messages):
        m = "\n".join(messages)
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

class CryptoDataFeed(DataFeedService):
    # metadata from bitstamp: high->Last 24 hours price high, low->Last 24 hours price low, last->Last BTC price, open->First price of the day, bid->Highest buy order, ask->Lowest sell order, volume-> Last 24 hours volume, vwap->Last 24 hours volume weighted average price, timestamp->Unix timestamp date and time
    #   {"high": "1.1", "last": "1.2", "timestamp": "..", "bid": "1", "vwap": "85", "volume": "8.35", "low": "1", "ask": "1", "open": "1"}
    # metadata from bitfinex: mid-> (bid+ask)/2, bid:bid, ask:ask, last_price:last order price, low:lowest since 24hrs, high:highest since 24hrs, volume:vol last 24h, timestamp:timestamp
    #   {"mid":"244.755", "bid":"244.75", "ask":"244.76", "last_price":"244.82", "low":"244.2", "high":"248.19", "volume":"7842.11542563", "timestamp":"1444253422.348340958"
    # metadata from kraken: a-> ask array, bid:bid array, c:last price array, v:vol array, p:vol weighted array, t:nb of trade array, l:low array, h:high array, o:today's opening price
    # {"error":[],"result":{"XXBTZUSD":{"a":["6912.00000","1","1.000"],"b":["6910.70000","3","3.000"],"c":["6911.00000","0.00014487"],"v":["6494.33137885","9652.06497642"],"p":["7043.85496","7125.73011"],"t":[15946,24979],"l":["6861.10000","6861.10000"],"h":["7304.50000","7363.00000"],"o":"7294.10000"}}}

    def request(self, request_params):
        #lower case all param values..
        lower_values = {k:v.lower() for k,v in request_params.items()}
        complete_url = self.url.format(**lower_values)

        try:
            # r = requests.get(complete_url)
            # mock-up for test..
            import random
            p = str(random.uniform(1.0, 2.0))
            t = str(datetime.now().timestamp())
            r = {"last": p, "timestamp": t, "volume": "8000", "open": "1.5"}
        except requests.exceptions.RequestException as e:
            print(e)
        print("Sending request using url: {}".format(complete_url))
        return r


class EventTracker(object):
    """AbstractClass for EventTracker implementations that track/return events as a list of messages.
    The datafeed_service is loosely coupled, so the Bot knows which datafeed_service to use and 
    provides the request_params if specified by the EventTracker.   
    """
    def __init__(self, datafeed_service, request_params=None):
        self.datafeed_service = datafeed_service
        self.request_params = request_params

    def setup(self):
        pass

    def signal_events(self, response):
        """Signal events and return them as list.
        Call by Bot using the response obtained from datafeed_service.request(self.request_params)
        """
        pass

    def __str__(self):
        atts = ", ".join(['{}:{}'.format(k,v) for k,v in self.__dict__.items()])
        return "'{}' with attributes {}".format(self.__class__.__name__, atts) 

    def __repr__(self):
        return self.__str__()


class TickerEventTracker(EventTracker):
    event_range_msg = "Pair {t.symbol} at {t.current:.3f} (prev={prev.current:.3f}) {a} ({dir}) the range [{low:.3f}-{high:.3f}]"
    event_change_msg = "Pair {t.symbol} at {t.current:.3f} changes {t.day_change:.2f}% from open value ({t.open})"

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
        evts = []
        self.prev_ticker = self.ticker
        self.ticker = Ticker(self.symbol, response)

        range_evt = self.signal_range_event()
        if range_evt:
            di = self.ticker.direction(self.prev_ticker)
            rkey = range_evt + "_" + di
            last_rtime = self.lastime_rangevents[rkey]
            if self.ticker.timestamp - last_rtime > self.wait_time:
                msg_r = self.event_range_msg.format(t=self.ticker, prev=self.prev_ticker, a=range_evt, dir=di, low=self.lo, high=self.hi)
                evts.append(msg_r)
                self.lastime_rangevents[rkey] = self.ticker.timestamp

        change_evt = self.signal_change_event()
        if change_evt:
            last_ctime = self.lastime_changevents[change_evt]
            if self.ticker.timestamp - last_ctime > self.wait_time:
                msg_c = self.event_change_msg.format(t=self.ticker)
                evts.append(msg_c)
                self.lastime_changevents[change_evt] = self.ticker.timestamp
        return evts

        
class Ticker(object):
    def __init__(self, symbol, values):
        self.symbol = symbol
        # OHLC values and 
        self.set_float_values(values, ('last','open','high','low','volume','bid','ask','vwap'))
        self.timestamp = float(values['timestamp'])
        # current price is last
        self.current = self.last 
        
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
 
def prepare_bot(yaml_file):
    """Main entry to configure and return a Bot based on YAML config file.
    It registers yaml classes, load YAML file and setup bot.
    """
    def register_classes(classes):
        for c in classes:
            yaml.register_class(c)

    yaml = ruamel.yaml.YAML()
    register_classes((Bot, NotificationService, ConsolNotificationService, EmailNotificationService, 
                      DataFeedService, CryptoDataFeed, 
                      EventTracker, TickerEventTracker))
    with open(yaml_file) as yf:
        bot = yaml.load(yf)
    bot.setup()
    return bot


def get_args():
    parser = argparse.ArgumentParser(description="Bot managing EventTracker(s) and sending their messages to NotificationService(s) based on yaml config file")
    parser.add_argument("-y", "--yaml", default="./tracker_conf.yaml", help="Yaml config file")
    return parser.parse_args()
              
if __name__ == '__main__':
    args = get_args()
    if not os.path.exists(args.yaml):
        exit("YAML file '{0}' not found, exit program!".format(args.yaml))    
    bot = prepare_bot(args.yaml)
    bot.launch_schedulers()

