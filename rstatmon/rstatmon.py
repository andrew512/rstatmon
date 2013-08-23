import sys
import math
import argparse
import threading
import subprocess
import Queue
import signal
import time
import datetime
import logging
import curses
import cjson


FORMAT = '%(name)-20s - %(asctime)s - %(levelname)8s - %(message)s'
logging.basicConfig(format=FORMAT,level=logging.DEBUG, filename='rstatmon.log')
logger = logging.getLogger('rstatmon')


class Statistics(object):
    def __init__(self, item):
        self.items = [float(item) for item in item]

    def min(self):
        if not len(self.items):
            return None

        return min(self.items)

    def max(self):
        if not len(self.items):
            return None

        return max(self.items)

    def avg(self):
        if not len(self.items):
            return None

        return sum(self.items) / len(self.items)

    def count(self):
        return len(self.items)

    def sum(self):
        if not len(self.items):
            return None

        return sum(self.items)

    def percentile(self, percent):
        if not self.items:
            return None

        self.items.sort()

        k = (len(self.items) - 1) * percent
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return self.items[int(k)]

        d0 = self.items[int(f)] * (c-k)
        d1 = self.items[int(c)] * (k-f)
        return d0 + d1


class HostStatCollection(object):
    def __init__(self, hostname):
        self.hostname = hostname
        self.sample_count = 0
        self.stats = {}
        self.aggregates = {}
        self.lock = threading.Lock()

    def incr_sample_count(self):
        self.sample_count += 1

    def add_stat(self, key, value):
        if not self.stats.has_key(key):
            self.stats[key] = []
        self.stats[key].append(value)

    def compute_aggregates(self):
        self.lock.acquire()
        try:
            self.aggregates = {}
            for key, values in self.stats.iteritems():
                if not self.aggregates.has_key(key):
                    self.aggregates[key] = {}
                stats = Statistics(values)
                self.aggregates[key]['min'] = stats.min()
                self.aggregates[key]['max'] = stats.max()
                self.aggregates[key]['avg'] = stats.avg()
                self.aggregates[key]['90percentile'] = stats.percentile(0.9)
        finally:
            self.lock.release()

    def get_aggregates(self):
        self.lock.acquire()
        try:
            return self.aggregates
        finally:
            self.lock.release()

    def to_dict(self):
        return {
            'hostname': self.hostname,
            'sample_count': self.sample_count,
            'stats': self.stats,
            'aggregates': self.get_aggregates()
        }

    def __str__(self):
        return str(self.to_dict())


class StatCollection(object):
    def __init__(self, name):
        self.name = name
        self.start_time = datetime.datetime.utcnow()
        self.end_time = None
        self.host_collections = {}

    def get_host_collection(self, hostname):
        """
        :param hostname:
        :return:
        :rtype: HostStatCollection
        """
        if not self.host_collections.has_key(hostname):
            self.host_collections[hostname] = HostStatCollection(hostname)
        return self.host_collections[hostname]

    def incr_sample_count(self, hostname):
        self.get_host_collection(hostname).incr_sample_count()

    def add_stat(self, hostname, key, value):
        self.get_host_collection(hostname).add_stat(key, value)

    def compute_aggregates(self, hostname):
        self.get_host_collection(hostname).compute_aggregates()

    def get_all_aggregates(self):
        aggregates = {}
        for host_collection in self.host_collections.values():
            aggregates[host_collection.hostname] = host_collection.get_aggregates()
        return aggregates

    def to_dict(self):
        host_collections = {}
        for hostname, host_collection in self.host_collections.iteritems():
            host_collections[hostname] = host_collection.to_dict()
        return {
            'name': self.name,
            'start_time': str(self.start_time),
            'end_time': str(self.end_time),
            'host_collections': host_collections,
            'aggregates': self.get_all_aggregates()
        }

    def __str__(self):
        return str(self.to_dict())


class StatCollector(threading.Thread):
    def __init__(self, queue):
        """
        :param queue:
        :type queue: Queue
        :return:
        """

        super(StatCollector, self).__init__()
        self.queue = queue
        self.current_collection_name = "Default collection started @ %s" % datetime.datetime.utcnow()
        self.collections = {}
        self.value_map = None
        self.collecting = True
        logger.info('Initialized StatCollector thread')


    @property
    def current_collection(self):
        """
        :return:
        :rtype: StatCollection
        """
        # Init bucket if necessary:
        if not self.collections.has_key(self.current_collection_name):
            self.collections[self.current_collection_name] = StatCollection(self.current_collection_name)

        return self.collections[self.current_collection_name]


    def reset_collection(self):
        try:
            del self.collections[self.current_collection_name]
        except Exception:
            pass


    def toggle_collection(self):
        self.collecting = not self.collecting
        return self.collecting


    def new_collection(self, collection_name):
        # End current collection:
        self.current_collection.end_time = datetime.datetime.utcnow()

        # Start new collection:
        logger.debug('New collection: %s', collection_name)
        self.current_collection_name = collection_name


    def run(self):
        while True:
            try:
                msg = self.queue.get()
                if msg == 'STOP':
                    logger.info('Received STOP message.')
                    break

                if not self.collecting:
                    continue

                hostname = msg.get('hostname')
                line = str(msg.get('line'))
                source = msg.get('source')

                if not line:
                    continue

                # ignore header rows
                if line.find('----') > -1:
                    continue
                elif not self.value_map and line.find('cache') > -1:
                    self.value_map = [element for element in line.split(' ') if element]
                    logger.info('[%s] Initialized value map: %s', hostname, self.value_map)
                    continue
                elif line.find('cache') > -1:
                    continue

                if not self.value_map:
                    logger.warn('Received data but have not yet received value map.')
                    continue

                elements = [element for element in line.split(' ') if element]

                # Increment sample count:
                self.current_collection.incr_sample_count(hostname)

                # Add parsed stats:
                custom_stats = {}
                for i in range(len(self.value_map)):
                    try:
                        if source == "vmstat":
                            if not custom_stats.has_key('totalfreemem'):
                                custom_stats['totalfreemem'] = 0
                            if not custom_stats.has_key('usedcpu'):
                                custom_stats['usedcpu'] = 100
                            if elements[i]:
                                if self.value_map[i] in ['free','buff','cache']:
                                    custom_stats['totalfreemem'] += int(elements[i])
                                if self.value_map[i] in ['id','wa']:
                                    custom_stats['usedcpu'] -= min(int(elements[i]),100)
                        self.current_collection.add_stat(hostname, self.value_map[i], elements[i])
                    except IndexError:
                        logger.warn('Ignoring malformed input line: %s', line)

                # Add any custom stats:
                for key, value in custom_stats.iteritems():
                    self.current_collection.add_stat(hostname, key, value)

                self.current_collection.compute_aggregates(hostname)

            except KeyboardInterrupt:
                pass
            except Exception, e:
                logger.exception(e)

        logger.info('Collector stopped.')


class VmStatWorker(threading.Thread):
    def __init__(self, hostname, queue, interval=None):
        """
        :param hostname:
        :param queue:
        :type queue: Queue
        :return:
        """

        super(VmStatWorker, self).__init__()
        self.hostname = hostname
        self.queue = queue
        self.buffer_size = 1
        self.stopping = False
        self.interval = interval or 1
        logger.info('Initialized VmStatWorker thread (%s @ %ss intervals)', self.hostname, self.interval)

    def run(self):
        def preexec_function():
            # Ignore the SIGINT signal by setting the handler to the standard
            # signal handler SIG_IGN.
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        p = subprocess.Popen('ssh %s vmstat %s' % (self.hostname, self.interval), shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=self.buffer_size, preexec_fn=preexec_function)

        while p.returncode is None:
            if self.stopping:
                logger.info('Stopping worker.')
                break

            try:
                line = p.stdout.readline().rstrip('\n')
                if line:
                    self.queue.put({'hostname': self.hostname, 'source':'vmstat', 'line': line})

            except KeyboardInterrupt:
                pass
            except Exception, e:
                logger.exception(e)

        logger.info('Worker stopped.')

    def stop(self):
        self.stopping = True


class RemoteStatMonitor(object):
    """
        TODOs:
        * better error handling
            * invalid hosts
            * host connection issues/auto-recovery
        * dynamically remove host

    """

    X_OFFSET = 0

    output_map = [
        {'type': 'hostname', 'header2': 'hostname', 'width':50 },
        {'type': 'num_samples', 'header1': '', 'header2': 'smpls', 'width':6, 'nohighlight':True },

        {'type': 'agg', 'header1': 'free mem', 'header2': 'avg', 'width':10, 'agg_key': 'totalfreemem', 'agg_type': 'avg', 'biggerisbetter':True },

        {'type': 'last', 'header1': 'cpu %', 'header2': 'last', 'width':7, 'key': 'usedcpu' },
        {'type': 'agg', 'header1': 'cpu %', 'header2': 'avg', 'width':7, 'agg_key': 'usedcpu', 'agg_type': 'avg' },

        {'type': 'last', 'header1': '|----', 'header2': 'last', 'width':5, 'key': 'us' },
        {'type': 'agg', 'header1fill': '-', 'header2': 'min', 'width':5, 'agg_key': 'us', 'agg_type': 'min' },
        {'type': 'agg', 'header1': 'user', 'header2': 'max', 'width':5, 'agg_key': 'us', 'agg_type': 'max' },
        {'type': 'agg', 'header1fill': '-', 'header2': 'avg', 'width':5, 'agg_key': 'us', 'agg_type': 'avg', 'warnabove': 75 },
        {'type': 'agg', 'header1': '----|', 'header2': '90th', 'width':5, 'agg_key': 'us', 'agg_type': '90percentile', 'warnabove': 75 },

        {'type': 'last', 'header1': '|----', 'header2': 'last', 'width':5, 'key': 'sy', 'warnabove': 75 },
        {'type': 'agg', 'header1fill': '-', 'header2': 'min', 'width':5, 'agg_key': 'sy', 'agg_type': 'min' },
        {'type': 'agg', 'header1': 'sys', 'header2': 'max', 'width':5, 'agg_key': 'sy', 'agg_type': 'max' },
        {'type': 'agg', 'header1fill': '-', 'header2': 'avg', 'width':5, 'agg_key': 'sy', 'agg_type': 'avg', 'warnabove': 75 },
        {'type': 'agg', 'header1': '----|', 'header2': '90th', 'width':5, 'agg_key': 'sy', 'agg_type': '90percentile', 'warnabove': 75 },

        {'type': 'last', 'header1': '|----', 'header2': 'last', 'width':5, 'key': 'id', 'biggerisbetter':True },
        {'type': 'agg', 'header1fill': '-', 'header2': 'min', 'width':5, 'agg_key': 'id', 'agg_type': 'min', 'biggerisbetter':True },
        {'type': 'agg', 'header1': 'idle', 'header2': 'max', 'width':5, 'agg_key': 'id', 'agg_type': 'max', 'biggerisbetter':True },
        {'type': 'agg', 'header1fill': '-', 'header2': 'avg', 'width':5, 'agg_key': 'id', 'agg_type': 'avg', 'warnunder': 20, 'biggerisbetter':True },
        {'type': 'agg', 'header1': '----|', 'header2': '90th', 'width':5, 'agg_key': 'id', 'agg_type': '90percentile', 'warnunder': 20, 'biggerisbetter':True },

        {'type': 'last', 'header1': '|----', 'header2': 'last', 'width':5, 'key': 'wa' },
        {'type': 'agg', 'header1fill': '-', 'header2': 'min', 'width':5, 'agg_key': 'wa', 'agg_type': 'min' },
        {'type': 'agg', 'header1': 'wait', 'header2': 'max', 'width':5, 'agg_key': 'wa', 'agg_type': 'max' },
        {'type': 'agg', 'header1fill': '-', 'header2': 'avg', 'width':5, 'agg_key': 'wa', 'agg_type': 'avg', 'warnabove': 5 },
        {'type': 'agg', 'header1': '----|', 'header2': '90th', 'width':5, 'agg_key': 'wa', 'agg_type': '90percentile', 'warnabove': 5 },
    ]

    def __init__(self):
        parser = argparse.ArgumentParser(description='Record and calculate statistics for remote metrics')

        parser.add_argument('hostname', nargs="+", help="the hostname(s) to monitor")
        parser.add_argument('-i', '--interval', type=int, default=1, help="the number of seconds between remote metric reports")
        parser.add_argument('-r', '--refresh_rate', type=int, default=1, help="the number of seconds between console refreshes")
        parser.add_argument('-f', '--filename', default="rstatmon.dmp", help="the name of the file in which to dump the raw results")

        self.options = parser.parse_args()

        self.hostnames = []
        self.shutting_down = False
        self.collector = None
        self.workers = []
        self.prev_values = {}
        self.queue = Queue.Queue()
        self.window = None
        self.content_window = None
        self.status_window = None


    def stop(self, *args, **kw):
        if self.shutting_down:
            return

        self.update_status('Stopping workers and saving results...')
        self.shutting_down = True

        logger.info('Calling stop on workers...')
        for worker in self.workers:
            worker.stop()

        logger.info('Sending STOP to collector...')
        self.queue.put('STOP')

        logger.info('Waiting for all threads to complete...')
        for thread in threading.enumerate():
            if thread is threading.currentThread():
                continue
            thread.join(5.0)


        logger.info('Writing results to %s...', self.options.filename)
        with open(self.options.filename, 'a') as f:
            lines = []
            now = datetime.datetime.utcnow()
            lines.append("=== START results dump @ %s =====================" % now)
            lines.append("")
            for collection in self.collector.collections.itervalues():
                lines.append("--- START collection: %s --------------------" % collection.name)
                # noinspection PyUnresolvedReferences
                lines.append(cjson.encode(collection.to_dict()))
                lines.append("--- END collection: %s --------------------" % collection.name)
                lines.append("")
            lines.append("=== END results dump @ %s =======================" % now)
            lines.append("\n")
            f.write("\n".join(lines))

        logger.info('Finished!')
        sys.exit()


    def add_host(self, hostname):
        w = VmStatWorker(hostname, self.queue, interval=self.options.interval)
        w.start()
        self.hostnames.append(hostname)
        self.workers.append(w)
        logger.debug('Added host: %s', hostname)


    def update_status(self, message, attrs=curses.A_NORMAL, bg_color_pair=None):
        bg_color_pair = bg_color_pair or curses.color_pair(4)
        self.status_window.bkgd(' ', bg_color_pair)
        self.status_window.erase()
        self.status_window.addstr(0, 1, message, attrs)
        self.status_window.refresh()


    def show_help(self):
        self.update_status('q: quit     r or ENTER: refresh     c: toggle collection on/off     n: new collection     h: add host     t: reset current collection')


    def refresh_content(self):
        self.content_window.erase()

        current_collection = self.collector.current_collection
        y_offset = 0
        x_offset = self.X_OFFSET

        self.content_window.addstr(y_offset, x_offset, "Current collection: %s" % current_collection.name, curses.color_pair(2) | curses.A_BOLD)

        hostname_width = min(max([len(host) for host in self.hostnames]), 75)

        y_offset += 1
        h1 = " ".join([item.get('header1', '').rjust(item['width'] if item['type'] != 'hostname' else hostname_width, item.get('header1fill', ' ')) for item in self.output_map])
        self.content_window.addstr(y_offset, x_offset, h1)

        y_offset += 1
        h2 = " ".join([item.get('header2', '').rjust(item['width'] if item['type'] != 'hostname' else hostname_width) for item in self.output_map])
        self.content_window.addstr(y_offset, x_offset, h2)

        y_offset += 1
        h3 = " ".join([''.rjust(item['width'] if item['type'] != 'hostname' else hostname_width, '=') for item in self.output_map])
        self.content_window.addstr(y_offset, x_offset, h3)

        for host in self.hostnames:
            host_collection = current_collection.get_host_collection(host)
            aggregates = host_collection.get_aggregates() if host_collection else {}
            y_offset += 1
            line_x_offset = x_offset
            i = 0
            for item in self.output_map:
                attrs = curses.A_NORMAL

                numeric_val = None
                if item['type'] == "hostname":
                    value = host
                    attrs = curses.color_pair(5) | curses.A_BOLD
                elif item['type'] == "num_samples":
                    value = host_collection.sample_count if host_collection else ""
                elif item['type'] == "agg":
                    value = "%.1f" % aggregates.get(item['agg_key'], {}).get(item['agg_type']) if aggregates else ""
                    try:
                        numeric_val = float(value)
                    except Exception:
                        pass
                elif item['type'] == "last":
                    if host_collection and host_collection.stats.get(item['key'], []):
                        value = "%.0f" % int(host_collection.stats.get(item['key'], [])[-1])
                    else:
                        value = ""
                    try:
                        numeric_val = int(value)
                    except Exception:
                        pass
                else:
                    value = ""

                if numeric_val is not None and item.get('warnunder') and item.get('warnunder') > numeric_val:
                    attrs = attrs | curses.A_REVERSE
                elif numeric_val is not None and item.get('warnabove') and item.get('warnabove') < numeric_val:
                    attrs = attrs | curses.A_REVERSE

                width = item['width']
                # size hostname dynamically:
                if item['type'] == 'hostname':
                    width = hostname_width
                prev_value_key = "|".join([host, str(i)])
                new_value = str(value).rjust(width)
                current_value = self.prev_values.get(prev_value_key, None)
                if current_value != new_value:
                    if current_value is not None and not item.get('nohighlight'):
                        try:
                            increased_attr = curses.color_pair(3) if not item.get('biggerisbetter') else curses.color_pair(1)
                            decreased_attr = curses.color_pair(1) if not item.get('biggerisbetter') else curses.color_pair(3)
                            if float(new_value) > float(current_value):
                                attrs = attrs | increased_attr | curses.A_BOLD
                            else:
                                attrs = attrs | decreased_attr | curses.A_BOLD
                        except:
                            attrs = attrs | curses.color_pair(3) | curses.A_BOLD
                    self.prev_values[prev_value_key] = new_value
                self.content_window.addstr(y_offset, line_x_offset, new_value, attrs)
                line_x_offset += (width + 1)
                i += 1

        y_offset += 2
        self.content_window.addstr(y_offset, x_offset, "rstatmon> ")
        self.content_window.refresh()


    def new_collection(self):
        try:
            self.content_window.nodelay(0)
            curses.echo()
            y, x = self.content_window.getyx()
            self.content_window.addstr(y, self.X_OFFSET, 'new collection name (empty to cancel)> ')
            self.content_window.refresh()
            collection_name = self.content_window.getstr()
            return ''.join(c for c in collection_name if ord(c) >= 32)

        finally:
            self.content_window.nodelay(1)
            curses.noecho()


    def new_hostname(self):
        try:
            self.content_window.nodelay(0)
            curses.echo()
            y, x = self.content_window.getyx()
            self.content_window.addstr(y, self.X_OFFSET, 'add hostname (empty to cancel)> ')
            self.content_window.refresh()
            hostname = self.content_window.getstr()
            return ''.join(c for c in hostname if ord(c) >= 32)

        finally:
            self.content_window.nodelay(1)
            curses.noecho()


    def handle_console(self):
        self.show_help()

        last_refresh = None
        while True:
            try:
                now = datetime.datetime.utcnow()
                if not last_refresh or now - last_refresh > datetime.timedelta(seconds=self.options.refresh_rate):
                    last_refresh = now
                    self.refresh_content()

                cury, curx = self.content_window.getyx()
                ch = self.content_window.getch()
                if ch == ord('q'):
                    self.content_window.addstr('quit')
                    self.content_window.refresh()
                    break
                elif ch == ord('t'):
                    self.collector.reset_collection()
                    self.refresh_content()
                elif ch == ord('c'):
                    collecting = self.collector.toggle_collection()
                    if not collecting:
                        self.update_status('Collection paused... press "c" to resume', bg_color_pair=curses.color_pair(6))
                    else:
                        self.show_help()
                        self.content_window.refresh()
                elif ch in [ord('r'), 10, 13]:
                    last_refresh = now
                    self.refresh_content()
                elif ch == ord('n'):
                    collection_name = self.new_collection()
                    if collection_name:
                        self.collector.new_collection(collection_name)
                    self.refresh_content()
                elif ch == ord('h'):
                    hostname = self.new_hostname()
                    if hostname:
                        self.add_host(hostname)
                    self.refresh_content()
                elif ch == ord('?'):
                    self.show_help()
                elif ch == curses.KEY_RESIZE:
                    logger.debug('resize event - new dimensions: %s', self.window.getmaxyx())
                    rows, cols = self.window.getmaxyx()
                    self.content_window.mvwin(0, 0)
                    self.content_window.resize(rows - 1, cols)
                    self.status_window.mvwin(rows - 1, 0)
                    self.status_window.refresh()
                    self.content_window.refresh()

                self.content_window.move(cury, curx)


            except KeyboardInterrupt:
                break
            except Exception, e:
                logger.exception(e)
            finally:
                time.sleep(0.1)

        self.stop()


    def start(self):
        """ Start the main process.
        """

        # Hook stop handlers:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGABRT, self.stop)

        # Init stat collector thread:
        self.collector = StatCollector(self.queue)
        self.collector.start()

        # Add hosts/init worker threads:
        for hostname in self.options.hostname:
            self.add_host(hostname)

        # Init terminal environment:
        self.window = curses.initscr()
        try:
            curses.noecho()
            curses.cbreak()
            self.window.keypad(1)
            try:
                curses.start_color()
                curses.use_default_colors()
            except:
                pass

            # init color map:
            # number, fg, bg
            curses.init_pair(1, curses.COLOR_GREEN, -1)
            curses.init_pair(2, curses.COLOR_BLUE, -1)
            curses.init_pair(3, curses.COLOR_RED, -1)
            curses.init_pair(4, curses.COLOR_BLUE, curses.COLOR_WHITE)
            curses.init_pair(5, curses.COLOR_CYAN, -1)
            curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_BLUE)

            rows, cols = self.window.getmaxyx()
            self.content_window = curses.newwin(rows - 1, cols, 0, 0)
            self.content_window.nodelay(1)
            self.status_window = curses.newwin(1, cols, rows - 1, 0)
            self.status_window.bkgd(' ', curses.color_pair(4))

            # Start main terminal handler:
            self.handle_console()

        finally:
            # Set everything back to normal:
            self.window.keypad(0)
            curses.echo()
            curses.nocbreak()
            curses.endwin()


if __name__ == '__main__':
    rsm = RemoteStatMonitor()
    rsm.start()
