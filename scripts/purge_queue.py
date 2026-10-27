"""Empty the orders queue so each demo starts clean."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import QUEUE, connect, declare_queue

conn = connect()
ch = conn.channel()
declare_queue(ch)
ch.queue_purge(QUEUE)
conn.close()
