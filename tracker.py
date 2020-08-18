# -*- coding: utf-8 -*-
"""
Created on Wed Mar  7 10:28:47 2018

@author: d7loz9
"""

from datetime import datetime, timedelta
from collections import deque
import requests
import ruamel.yaml
import argparse
import importlib
import schedule
import time
import os
import getpass
import smtplib, ssl
from notify_run import Notify
#from pydrive.drive import GoogleDrive
#from pydrive.auth import GoogleAuth


class Bot(object):
    def __init__(self, notification_services, datafeed_services, event_trackers):
        self.notification_services = notification_services
        self.datafeed_services = datafeed_services
        self.event_trackers = event_trackers

    def setup(self):
        if not hasattr(self, 'sleep_period'):
            self.sleep_period = 60

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
        err_msg = None
        try:
            service_response = tracker.datafeed_service.request(request_params=request_p)
            events_msg = tracker.signal_events(service_response)
        except Exception as e:
            err_msg = "run_tracker failed"
            events_msg = Events(one_event=(err_msg, str(e)))

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
        if self.active:
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

    def short_messages(self, events, concat=" || "):
        short_msgs = [e.event_text for e in events]
        return concat.join(short_msgs)

    def long_messages(self, events, concat="\n\t- "):
        long_msgs = [e.event_longtext for e in events]
        m = concat.join(long_msgs)
        long_s = "_"*50 + "\n{nb} Event(s) signaled:{concat}{msgs}" + "\n" + "_"*50 + "\n\n"
        return long_s.format(nb=len(long_msgs), concat=concat, msgs=m)

class ConsolNotificationService(NotificationService):
    def _notify(self, events):
        print("Short message--> " + self.short_messages(events))
        print("Long message-->\n" + self.long_messages(events))


class AndroidPushNotificationService(NotificationService):
    """Use notify.run free service "Web Push API" to push notification to 
    Android phone through a Channel (ok for non-private data --> https://notify.run)
    Pas fiable, bcp de message droppé surtout sur le portable..
    """
    def _setup(self):
        self.notifier = Notify()
        #not to expose my channel unecessarily, Notify is configured in ~/.config/notify-run
        
    def _notify(self, events):
        self.notifier.send(self.short_messages(events))


pwd_saved = None

class EmailNotificationService(NotificationService):
    """Use smtp server with SSL connection to send email notifications
    """
    message = \
"""Subject: {subject}

{message}
"""
    def _setup(self):
        global pwd_saved
        self.smtp = self.params['smtp']
        # port 465 for SSL
        self.port = self.params.get('port',465)
        self.login = self.params['login']
        if pwd_saved:
            self.pwd = pwd_saved
        else:
            pwd_saved = getpass.getpass("For sending emails, enter password of '{}': ".format(self.login))
            self.pwd = pwd_saved
        self.to =  self.params['to']
        # secured SSL content
        self.context = ssl.create_default_context()

    def _notify(self, events):
        # adding "Tracker:" to mark as important on gmail side
        complete_msg = self.message.format(subject="Tracker: "+self.short_messages(events), message=self.long_messages(events))
        # print("Notify email with msg:\n{}".format(complete_msg))
        with smtplib.SMTP_SSL(self.smtp, self.port, context=self.context) as server:
             server.login(self.login, self.pwd)
             # encode() added because sendmail tries to convert str to ascii and fail with special up/down arrow  
             server.sendmail(self.login, self.to, complete_msg.encode('utf-8'))


class DataFeedService(object):
    """Responsible to fetch online live data (ticker, ..) & return a response
    """
    def __init__(self, **params):
        self.params = params

    def setup(self):
        pass

    def request(self, request_params=None):
        """Send a request and return response as dict. It is called by `Bot` which 
        may also provide a request_params when specified by the EventTracker. 
        Return Exception in case of error. 
        """
        pass

    def __str__(self):
        atts = ", ".join(['{}:{}'.format(k,v) for k,v in self.__dict__.items()])
        return "'{}' with attributes {}".format(self.__class__.__name__, atts) 


class SimpleTickerDataFeed(DataFeedService):
    def request(self, request_params):
        complete_url = self.url.format(**request_params)

        r = requests.get(complete_url)
        if r.status_code != requests.codes.ok:
            raise Exception("Request {} response not 200-OK: {}".format(complete_url, r))
        response = r.json()
        # mock-up for test..
        # import random
        # p = str(random.uniform(2.0, 2.2))
        # t = str(datetime.now().timestamp())
        # if self.url.find('bitstamp') > -1:
        #     r = {"last": p, "timestamp": t, "volume": "8000", "open": "1.5", "high": "1", "bid": "1", "vwap":"1", "low":"1", "ask":"1"}
        # elif self.url.find('kraken') > -1:
        #     r = {"error":[],"result":{"XTZUSD":{"a":["1.963400","1135","1135.000"],"b":["1.960200","829","829.000"],"c":[p,"169.08065753"],"v":["162651.08050865","733026.03677556"],"p":["1.929881","1.904369"],"t":["397","1553"],"l":["1.896800","1.894300"],"h":["1.993500","2.007000"],"o":"1.5"}}}
        # response = r
        return response

# class Events(list):
#     """Holds Events related to Ticker tracker as a list of tuple(short_msg, long_msg)
#     """
#     def __init__(self, one_event=None):
#         self.events = []
#         if one_event:
#             if not isinstance(one_event, tuple):
#                 raise Exception("Invalid event {}".format(one_event))
#             self.events.append(one_event)

#     def add_event(self, short_msg, long_msg):
#         self.append((short_msg, long_msg))

#     @property
#     def is_empty(self):
#         return len(self.events) == 0

#     def __iter__(self):
#         return iter(self.events)

class Event(object):
    def __init__(self, format_text, format_longtext=None, **text_values):
        self.format_text = format_text
        if format_longtext:
            self.format_longtext = format_longtext
        else:
            self.format_longtext = format_text

        for v in text_values:
            setattr(self, v, text_values[v])

    @property
    def event_text(self):
        return self.format_text.format(**self.__dict__)

    @property
    def event_longtext(self):
        return self.format_longtext.format(**self.__dict__)



class EventTracker(object):
    """AbstractClass that track events and signal them.
    The datafeed_service is loosely coupled, it's Bot's responsability to call the datafeed_service 
    and to provide the request_params (when specified by the EventTracker)
    """
    def __init__(self, datafeed_service, request_params=None):
        self.datafeed_service = datafeed_service
        self.request_params = request_params

    def setup(self):
        #default is to NEVER wait before signaling same type of event
        self.wait_time = self.params.get('wait_time',0)
        self._setup()

    def signal_events(self, response):
        """Signal events and return them as `Events` object. 
        Call by Bot using the response obtained from datafeed_service.request(self.request_params)
        """
        pass

    def _setup(self):
        pass

    def __str__(self):
        atts = ", ".join(['{}:{}'.format(k,v) for k,v in self.__dict__.items()])
        return "'{}' with attributes {}".format(self.__class__.__name__, atts) 

    def __repr__(self):
        return self.__str__()


class TickerEventTracker(EventTracker):
    dir_symbol = {'down': '\u2193', 'up': '\u2191'}

    def __init__(self, symbol, **params):
        self.symbol = symbol
        self.params = params

    def _setup(self):
        """Supported param, ex :
            - lo: 1.0, hi: 2.0 (track entering/exiting range between 1.0 and 2.0)
            - change_day: 5.0 (track daily change exceeding +/- 5.0%)
            - change_lag: [2.0, -3] (track change exceeding +/- 2.0% between current and last-3 ticker)
        """
        self.lo = float(self.params.get('lo', 0.0))
        self.hi = float(self.params.get('hi', float('inf')))
        if self.lo == 0.0 and self.hi == float('inf'):
            self.lo = None
            self.hi = None
        self.change_day = self.params.get('change_day', None)
        self.change_lag = self.params.get('change_lag', None)
        
        # keeps track of previous 100 ticker 
        self.tickers = deque(maxlen=100)

        # last time specific events took place
        self.lastime_rangevents = dict(enter_up=0, enter_down=0, exit_up=0, exit_down=0, cross_up=0, cross_down=0)
        self.lastime_changeday = 0
        self.lastime_changelag = 0

    def range_event(self):
        #msg_l = "Pair {t.symbol} at {t.current:.3f} (prev={prev.current:.3f}) {a} ({dir}) the range [{r[0]:.3f}-{r[1]:.3f}]"
        # msg_s = "{t.symbol} {t.current:.3f} (prev={prev.current:.3f}) {a}{dir} [{r[0]:.3f}-{r[1]:.3f}]"
        if not self.lo:
            return None

        action = self.current_ticker.range_action(previous_ticker=self.previous_ticker, hi=self.hi, lo=self.lo)
        if action:
            return Event("{symbol} at {price:.3f} (prev={prev_price:.3f}) {action}{dir} [{range[0]:.3f}-{range[1]:.3f}]",
                          symbol=self.current_ticker.symbol,
                          price=self.current_ticker.current,
                          prev_price=self.previous_ticker.current,
                          action=action,
                          dir=self.dir_symbol[self.current_ticker.direction(self.previous_ticker)],
                          range=(self.lo, self.hi) )


    def changeday_event(self):
        # msg_l = "Pair {t.symbol} at {t.current:.3f} changes {t.day_change:.2f}% from open value {t.open}"
        # msg_s = "{t.symbol} {t.current:.3f} changes {t.day_change:.2f}% from open {t.open}"
        if not self.change_day:
            return None

        if not (-self.change_day < self.current_ticker.open_change < self.change_day):
            return Event("{symbol} at {price:.3f} changes {open_change:.2f}% from open {open}",
                         symbol=self.current_ticker.symbol,
                         price=self.current_ticker.current,
                         open_change=self.current_ticker.open_change,
                         open=self.current_ticker.open)


    def changelag_event(self):
        # msg_l = "Pair {t.symbol} at {t.current:.3f} changes {change:.2f}% from lag-{lag} value {value}"
        # msg_s = "{t.symbol} {t.current:.3f} changes {change:.2f}% from lag-{lag}"
        if not self.change_lag or len(self.tickers) <= self.change_lag[1]*-1:
            return None
        
        lag_value = self.tickers[self.change_lag[1]-1].current
        change = (self.current_ticker.current - lag_value) / lag_value * 100.0
        if not (-self.change_lag[0] < change < self.change_lag[0]):
            return  Event("{symbol} at {price:.3f} changes {change:.2f}% from lag{lag}",
                          symbol=self.current_ticker.symbol,
                          price=self.current_ticker.current,
                          change=change,
                          lag=self.change_lag[1])

    def signal_events(self, response):
        self.tickers.append(ticker_response_adapter(response, self.symbol, self.datafeed_service.url))
        evts = list()

        self.current_ticker = self.tickers[-1]
        self.previous_ticker = self.tickers[-2] if len(self.tickers) > 1 else None

        range_evt = self.range_event()
        if range_evt:
            rkey = range_evt.action + "_" + self.current_ticker.direction(self.previous_ticker)
            last_rtime = self.lastime_rangevents[rkey]
            if self.current_ticker.timestamp - last_rtime > self.wait_time:
                evts.append(range_evt)
                self.lastime_rangevents[rkey] = self.current_ticker.timestamp

        changeday_evt = self.changeday_event()
        if changeday_evt:
            if self.current_ticker.timestamp - self.lastime_changeday > self.wait_time:
                evts.append(changeday_evt)
                self.lastime_changeday = self.current_ticker.timestamp

        changelag_evt = self.changelag_event()
        if changelag_evt:
            if self.current_ticker.timestamp - self.lastime_changelag > self.wait_time:
                evts.append(changelag_evt)
                self.lastime_changelag = self.current_ticker.timestamp

        if len(evts) == 0:
            print("No event signaled for {}".format(self.current_ticker))
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
    
    # Adapt custom responses
    values = None
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
        if len(response['result'].keys()) != 1:
            raise Exception("Adapter received Unexpected response '{}' from  {}".format(response['result'], service_url))
        else:
            symbol_key = list(response['result'].keys())[0]
            values = dict(current=  response['result'][symbol_key]['c'][0],
                      open=     response['result'][symbol_key]['o'],
                      high=     response['result'][symbol_key]['h'][0],
                      low=      response['result'][symbol_key]['l'][0],
                      volume=   response['result'][symbol_key]['p'][0],
                      bid=      response['result'][symbol_key]['b'][0],
                      ask=      response['result'][symbol_key]['a'][0],
                      vwap=     response['result'][symbol_key]['p'][1])
    else:
        raise Exception("Adapter does not support this service '{}'".format(service_url))
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
    def open_change(self):
        return (self.current - self.open) / self.open * 100.0

    def change(self, prev_ticker):
        return (self.current - prev_ticker.current) / prev_ticker.current * 100.0

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

    def range_action(self, previous_ticker, hi, lo):
        action = None
        if not previous_ticker:
            return action
        elif not previous_ticker.inside(lo, hi) and self.inside(lo, hi):
            action = 'enter'
        elif previous_ticker.inside(lo, hi) and not self.inside(lo, hi):
            action = 'exit'
        elif not previous_ticker.inside(lo, hi) and not self.inside(lo, hi):
            if previous_ticker.current <= lo and self.current >= hi or \
               previous_ticker.current >= hi and self.current <= lo:
                action = 'cross'
        return action

    def __str__(self):
        n = datetime.fromtimestamp(self.timestamp)
        return "Ticker {t.symbol} at {t.current:.3f} (open={t.open:.3f}, vol={t.volume:.1f}, utc-time={now})".format(t=self, now=n)



# gdrive = None
# def setup_gdrive():
#     global gdrive
#     if not gdrive:
#         gauth = GoogleAuth()
#         print("Need to authenticate Google-drive access")
#         #this opens a web client for authentication, impossible to use on headless server
#         gauth.LocalWebserverAuth()
#         #this could work, but probably beed to change my OAuth from Web app to 'Other client' (redo the pydrive init)
#         #gauth.CommandLineAuth()
#         gdrive = GoogleDrive(gauth)

# gdrive_file = None
# def setup_gdrive_file(file_id):
#     setup_gdrive()
#     global gdrive_file
#     gdrive_file = gdrive.CreateFile({'id': file_id})
#     return gdrive_file
        
def get_yaml_content(args):
    if args.local_yaml:
        with open(args.local_yaml) as yf:
            y_content = yf.read()
    elif args.gdrive_yaml:
        setup_gdrive_file(args.gdrive_yaml)
        y_content = gdrive_file.GetContentString()
    else:
        raise Exception("Unsupported args '{}'".format(args))
    print("Tracker running with Yaml content: {}".format(y_content))
    return y_content

def get_yaml_modified_date(args):
    if args.local_yaml:
        return os.path.getmtime(args.local_yaml)
    elif args.gdrive_yaml:
        gdrive_file.FetchMetadata(fields='modifiedDate')
        modified_date_s = gdrive_file['modifiedDate']
        # print("je suis le gdrive date=" + modified_date_s)
        modified_date = datetime.strptime(modified_date_s, '%Y-%m-%dT%H:%M:%S.%fZ')
        return modified_date.timestamp()

def setup_bot(yaml_content):
    """Main entry to configure and return a Bot based on YAML config file.
    It registers yaml classes, load YAML file and setup bot.
    """
    def register_classes(classes):
        for c in classes:
            yaml.register_class(c)

    yaml = ruamel.yaml.YAML()
    register_classes((Bot, NotificationService, 
                    ConsolNotificationService, EmailNotificationService, AndroidPushNotificationService,
                    DataFeedService, SimpleTickerDataFeed, 
                    EventTracker, TickerEventTracker))
    bot = yaml.load(yaml_content)
    bot.setup()
    #print("Finished setting up Bot--> {}".format(bot))
    return bot
    
def get_args():
    parser = argparse.ArgumentParser(description="Bot to launch EventTracker(s) (defined in yaml config)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-l", "--local_yaml", nargs="?", const="./yaml_conf/tracker_conf.yaml", help="Local Yaml filepath")
    group.add_argument("-g", "--gdrive_yaml", nargs="?", const="1Ac8hQrAkM5OdozdQ_JiS8IXIx7mwb1Dr", help="Google drive Yaml file-Id")
    return parser.parse_args()
              
if __name__ == '__main__':
    args = get_args()
    yaml_content = get_yaml_content(args)
    bot = setup_bot(yaml_content)
    last_yaml_update = get_yaml_modified_date(args)
    print("Running schedules, press Ctrl-C to stop!")
    try:
        while True:
            m_date = get_yaml_modified_date(args)
            if last_yaml_update == m_date:
                schedule.run_pending()
            else:
                del(bot)
                print("Yaml config file was modified, resetting the Bot!")
                yaml_content = get_yaml_content(args)
                bot = setup_bot(yaml_content)
                last_yaml_update = m_date
            time.sleep(bot.sleep_period)
    except KeyboardInterrupt:
        pass
    except:
        raise
        
    
