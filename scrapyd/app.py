import multiprocessing
import os

from twisted.application.service import Application
from twisted.application.internet import TimerService, TCPServer
from twisted.web import server
from twisted.python import log

import traceback
import socket
import psutil
import redis
import requests
import json

from scrapy.utils.misc import load_object

from .interfaces import IEggStorage, IPoller, ISpiderScheduler, IEnvironment
from .eggstorage import FilesystemEggStorage
from .scheduler import SpiderScheduler
from .poller import QueuePoller
from .environ import Environment


def application(config):
    app = Application("Scrapyd")
    http_port = config.getint('http_port', 6800)
    bind_address = config.get('bind_address', '127.0.0.1')
    poll_interval = config.getfloat('poll_interval', 5)

    poller = QueuePoller(config)
    eggstorage = FilesystemEggStorage(config)
    scheduler = SpiderScheduler(config)
    environment = Environment(config)

    app.setComponent(IPoller, poller)
    app.setComponent(IEggStorage, eggstorage)
    app.setComponent(ISpiderScheduler, scheduler)
    app.setComponent(IEnvironment, environment)

    laupath = config.get('launcher', 'scrapyd.launcher.Launcher')
    laucls = load_object(laupath)
    launcher = laucls(config, app)

    webpath = config.get('webroot', 'scrapyd.website.Root')
    webcls = load_object(webpath)

    timer = TimerService(poll_interval, poller.poll)
    webservice = TCPServer(http_port, server.Site(
        webcls(config, app)), interface=bind_address)
    log.msg(format="Scrapyd web console available at http://%(bind_address)s:%(http_port)s/",
            bind_address=bind_address, http_port=http_port)

    launcher.setServiceParent(app)
    timer.setServiceParent(app)
    webservice.setServiceParent(app)

    host = get_host_ip(config)
    redis_host = config.get('redis_host', 'localhost')
    redis_port = config.get('redis_port', 6379)
    redis_db = config.get('redis_db', 0)
    redis_pool = redis.ConnectionPool(
        host=redis_host,
        port=redis_port,
        db=redis_db
    )
    register_to_redis(config, redis_pool)
    log.msg('Registering scrapyd [{}] to redis {}:{} at db {}'.format(host, redis_host, redis_port, redis_db))
    # log.msg('2018-11-03 10:10 am')
    redis_interval = config.getfloat('redis_interval', 5)
    register_timer = TimerService(
        redis_interval, register_to_redis, config, redis_pool)
    register_timer.setServiceParent(app)

    return app


failure_count = 0


def register_to_redis(config, redis_pool):
    global failure_count
    try:
        redis_key = config.get('redis_key', 'scrapyd:nodes')
        host_ip = get_host_ip(config)
        if host_ip is None:
            host_name = socket.gethostname()
            message = '"host_ip" is not configured, scrapyd [{}] not registered'.format(
                host_name)
            log.msg(message)
            if config.get('notify', False):
                notify(config, message)
            return
        host_port = config.get('http_port', 6800)
        host = '{}:{}'.format(host_ip, host_port)
        mem_free = int(psutil.virtual_memory().available / 1048576)
        cpu_load = os.getloadavg()[0]
        n_cpu = multiprocessing.cpu_count()
        value = f"{mem_free}|{cpu_load}|{n_cpu}"

        r = redis.Redis(connection_pool=redis_pool)
        if r.hset(redis_key, host, value):
            log.msg('Scrapyd [{}] registered to redis again.'.format(host))
        failure_count = 0
    except Exception as err:
        failure_count += 1
        log.msg(err)
        message = traceback.format_exc()
        if failure_count < 10:
            notify(config, message)


def get_host_ip(config):
    _ip = None
    try:
        _ip = [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(
            ('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]
    except Exception as err:
        log.msg(err)
    return config.get('host_ip', _ip)


def notify(config, message):
    key = config.get('notify_key', '')
    if key == '':
        return
    url = 'https://hooks.slack.com/services/{}'.format(key)
    headers = {'content-type': 'application/json'}
    payload = {'text': message}
    requests.post(url, data=json.dumps(payload), headers=headers)
