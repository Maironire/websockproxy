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
import pika

import json


FORMAT = '%(asctime)-15s %(message)s'
RATE = 40980.0 #unit: bytes
BROADCAST = '%s%s%s%s%s%s' % (chr(0xff),chr(0xff),chr(0xff),chr(0xff),chr(0xff),chr(0xff))
PING_INTERVAL = 30

logger = logging.getLogger('relay')


macmap = {}

def format_rate(bytesCount, interval):
    if bytesCount > 2**17: return f"{bytesCount / interval / 2**20 * 8:.1f} Mbit/sec"
    elif bytesCount > 2**7: return f"{bytesCount /interval / 2**10 * 8:.1f} Kbit/sec"
    else: return f"{bytesCount:.1f} bit/sec"

class TunThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(TunThread, self).__init__(*args, **kwargs)
        self.running = True
        self.tun = TunTapDevice(name="tap0", flags= (IFF_TAP | IFF_NO_PI))
        self.tun.addr = '10.5.0.1'
        self.tun.netmask = '255.255.0.0'
        self.tun.mtu = 1500
        self.tun.up()
        self.ingressBytes = 0
        self.egressBytes = 0

    def write(self, data):
        self.tun.write(data)
        self.egressBytes += len(data)

    def run(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        p = poll()
        p.register(self.tun, POLLIN)
        reportTimestamp = time.time()
        try:
            while(self.running):
                pollret = p.poll(1000)
                for (f,e) in pollret:
                    if f == self.tun.fileno() and (e & POLLIN) and len(data := self.tun.read(self.tun.mtu+18)):
                        mac = data[0:6]
                        if mac == BROADCAST or (mac[0] & 0x1) == 1:
                            for client in macmap.values():
                                channel.basic_publish('ingress_eth', client, data)
                                self.ingressBytes += len(data)

                        elif macmap.get(mac, False):
                            channel.basic_publish('ingress_eth', macmap[mac], data)
                            self.ingressBytes += len(data)
                connection.process_data_events()
                if (delta := time.time() - reportTimestamp) > 0.5:
                    print(f"\rTX: {format_rate(self.egressBytes, delta):<15} RX: {format_rate(self.ingressBytes, delta):<15}", end='')
                    self.egressBytes = self.ingressBytes = 0
                    reportTimestamp = time.time()
        except Exception as e:
            logger.error('closing due to tun error')
            raise e
        finally:
            self.tun.close()


class HiveConnection:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

        # self.upstream = RateLimitingState(RATE, name='upstream', clientip=self.remote_ip)
        # self.downstream = RateLimitingState(RATE, name='downstream', clientip=self.remote_ip)

    async def start(self):
        connection = await aio_pika.connect_robust(self.host)
        channel = await connection.channel()

        self.ingress_exchange = await channel.declare_exchange('ingress_eth', aio_pika.ExchangeType.DIRECT)
        self.egress_exchange = await channel.declare_exchange('egress_eth', aio_pika.ExchangeType.FANOUT)
        queue = await channel.declare_queue(self.queue_name, auto_delete=True)
        await queue.bind(self.egress_exchange)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    # data = json.loads(message.body.decode())
                    data = message.body
                    await self.receive_message(data, message.reply_to)
                    # await message.ack()

    async def receive_message(self, data, source):
        # print("TX: ", data)

        mac = data[6:12]
        if mac not in macmap:
            macmap[mac] = source
            logger.info('%s: connected.' % source)

        dest = data[0:6]
        try:
            if dest == BROADCAST or (dest[0] & 0x1) == 1:
                # if self.upstream.do_throttle(data):

                    for client in macmap.values():
                        try: await self.send_message(client, data)
                        except: pass

                    tunthread.write(data)
            elif client := macmap.get(dest):
                # if self.upstream.do_throttle(data):
                    try: await self.send_message(client, data)
                    except: pass
            else:
                # if self.upstream.do_throttle(data):
                    tunthread.write(data)

        except:
            tb = traceback.format_exc()
            logger.error('%s: error on receive\n%s' % (source, tb))

    async def send_message(self, target, data):
        # print("RX: ", data)
        # print("TO:", target)
        await self.ingress_exchange.publish(aio_pika.Message(data), target)

    async def destroy(self):
        await self.ingress_exchange.delete()
        await self.egress_exchange.delete()

if __name__ == "__main__":

    host = "amqp://localhost"
    queue_name = "egress_eth"

    hiveConnection = HiveConnection(host, queue_name)

    tunthread = TunThread()
    tunthread.start()

    loop = asyncio.get_event_loop()
    try: loop.run_until_complete(hiveConnection.start())
    finally:
        tunthread.running = False
        loop.run_until_complete(hiveConnection.destroy())




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

