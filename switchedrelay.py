import sys
import time
import threading
import logging
import traceback
import functools

from select import poll
from select import POLLIN, POLLOUT, POLLHUP, POLLERR, POLLNVAL

from pytun import TunTapDevice, IFF_TAP, IFF_NO_PI


from limiter import RateLimitingState

import asyncio
import aio_pika

import json


FORMAT = '%(asctime)-15s %(message)s'
RATE = 40980.0 #unit: bytes
BROADCAST = '%s%s%s%s%s%s' % (chr(0xff),chr(0xff),chr(0xff),chr(0xff),chr(0xff),chr(0xff))
PING_INTERVAL = 30

logger = logging.getLogger('relay')


macmap = {}

async def delay_future(t, callback):
    timestamp = time.time()
    if timestamp < t:
        return
    else:
        callback(t)

class TunThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(TunThread, self).__init__(*args, **kwargs)
        self.running = True
        self.tun = TunTapDevice(name="tap0", flags= (IFF_TAP | IFF_NO_PI))
        self.tun.addr = '10.5.0.1'
        self.tun.netmask = '255.255.0.0'
        self.tun.mtu = 1500
        self.tun.up()

    def write(self, message):
        self.tun.write(message)

    def run(self):
        p = poll()
        p.register(self.tun, POLLIN)
        try:
            while(self.running):
                pollret = p.poll(1000)
                for (f,e) in pollret:
                    if f == self.tun.fileno() and (e & POLLIN):
                        buf = self.tun.read(self.tun.mtu+18)
                        if len(buf):
                            mac = buf[0:6]
                            if mac == BROADCAST or (mac[0] & 0x1) == 1:
                                for socket in macmap.values():
                                    def send_message(socket):
                                        try:
                                            socket.rate_limited_downstream(buf)
                                        except:
                                            pass

                                    # loop.add_callback(functools.partial(send_message, socket))

                            elif macmap.get(mac, False):
                                def send_message():
                                    try:
                                        macmap[mac].rate_limited_downstream(buf)
                                    except:
                                        pass

                                # loop.add_callback(send_message)
        except Exception as e:
            raise e
            logger.error('closing due to tun error')
        finally:
            self.tun.close()


class RabbitMQConsumer:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

        # self.remote_ip = self.request.headers.get('X-Forwarded-For', self.request.remote_ip)
        self.remote_ip = '127.0.0.1'
        logger.info('%s: connected.' % self.remote_ip)
        self.thread = None
        self.mac = ''
        self.allowance = RATE
        self.last_check = time.time()
        self.upstream = RateLimitingState(RATE, name='upstream', clientip=self.remote_ip)
        self.downstream = RateLimitingState(RATE, name='downstream', clientip=self.remote_ip)

    async def start(self):
        connection = await aio_pika.connect_robust(self.host)
        channel = await connection.channel()

        queue = await channel.declare_queue(self.queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    # data = json.loads(message.body.decode())
                    data = message.body.decode()
                    await self.handle_message(data)

    async def handle_message(self, data):
        print("DATA: ", data)

        if self.mac != data[6:12]:
            if macmap.get(self.mac, False):
                del macmap[self.mac]

            self.mac = data[6:12]

            macmap[self.mac] = self

        dest = data[0:6]
        try:
            if dest == BROADCAST or (dest[0] & 0x1) == 1:
                if self.upstream.do_throttle(data):
                    for socket in macmap.values():
                        try:
                            socket.write_message(str(data),binary=True)
                        except:
                            pass

                    tunthread.write(data)
            elif macmap.get(dest, False):
                if self.upstream.do_throttle(data):
                    try:
                        macmap[dest].write_message(str(data),binary=True)
                    except:
                        pass
            else:
                if self.upstream.do_throttle(data):
                    tunthread.write(data)

        except:
            tb = traceback.format_exc()
            # logger.error('%s: error on receive. Closing\n%s' % (self.remote_ip, tb))
            try:
                self.close()
            except:
                pass

if __name__ == "__main__":

    tunthread = TunThread()
    tunthread.start()

    host = "amqp://localhost"
    queue_name = "VPN"

    consumer = RabbitMQConsumer(host, queue_name)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(consumer.start())

    tunthread.running = False



# if __name__ == '__main__':

#     tunthread = TunThread()
#     tunthread.start()
 
#     args = sys.argv
#     tornado.options.parse_command_line(args)
#     application.listen(8080)
#     loop = tornado.ioloop.IOLoop.instance()
#     try:
#         loop.start()
#     except:
#         pass

#     tunthread.running = False

