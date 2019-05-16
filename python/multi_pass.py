#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import Queue
import threading
import time
import subprocess
import logging
reload(sys)
sys.setdefaultencoding("utf-8")

logging.basicConfig(
    level=logging.NOTSET,
    format=
    '%(asctime)s %(process)d %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
)


class Producter(threading.Thread):
    """生产者线程"""

    def __init__(self, t_name, part_queue, line_queue):
        self.part_queue = part_queue
        self.line_queue = line_queue
        threading.Thread.__init__(self, name=t_name)

    def run(self):
        logging.info('(%s) started', self.name)

        num_parts = 0  # 处理过的part文件总数
        num_lines = 0  # 输出的总行数
        while True:
            try:
                part_file = self.part_queue.get(block=True, timeout=10)
            except Queue.Empty:
                logging.info('(%s) no part files in queue', self.name)
                break

            num_parts += 1
            logging.info('(%s) get %sth part file: %s', self.name, num_parts,
                         part_file)
            logging.info('(%s) remaining part files: %s', self.name,
                         self.part_queue.qsize())

            hdfs_text = subprocess.Popen(
                ["hadoop", "fs", "-text", part_file], stdout=subprocess.PIPE)

            nline = 0
            for line in hdfs_text.stdout:
                nline += 1
                self.line_queue.put(line)

            num_lines += nline
            logging.info('(%s) extract %s line from part file: %s', self.name,
                         nline, part_file)
            logging.info('(%s) extract %s part file, %s lines', self.name,
                         num_parts, num_lines)
            logging.info('(%s) lines in queue: %s', self.name,
                         self.line_queue.qsize())

        logging.info('(%s) finished: %s part file, %s lines', self.name,
                     num_parts, num_lines)


class Consumer(threading.Thread):
    """消费者线程"""

    def __init__(self, t_name, part_queue, line_queue):
        self.part_queue = part_queue
        self.line_queue = line_queue
        threading.Thread.__init__(self, name=t_name)

    def run(self):
        logging.info('(%s) started', self.name)
        time.sleep(10)

        nline = 0
        while True:
            try:
                line = self.line_queue.get(block=True, timeout=10)
            except Queue.Empty:
                if self.part_queue.empty():
                    logging.info(
                        '(%s) get lines overtime and part_queue is empty',
                        self.name)
                    break
                else:
                    logging.info(
                        '(%s) get lines overtime but part_queue is NOT empty',
                        self.name)
                    continue

            line = line.strip()
            if not line:
                continue

            print line
            nline += 1

            if nline % 200000 == 0:
                logging.info('(%s) output %s lines', self.name, nline)
                logging.info('(%s) remaining lines in queue: %s', self.name,
                             self.line_queue.qsize())

        logging.info('(%s) finished: %s lines', self.name, nline)


def main():
    iter_num = 1
    if len(sys.argv) > 1:
        iter_num = int(sys.argv[1])
    logging.info('num of epochs: %s', iter_num)

    part_files = []
    for line in sys.stdin:
        line = line.strip()
        part_files.append(line)
    logging.info('num of part files: %s', len(part_files))

    part_queue = Queue.Queue()
    for i in range(iter_num):
        for line in part_files:
            part_queue.put(line)
    logging.info('length of part queue: %s', part_queue.qsize())

    line_queue = Queue.Queue(200000)

    num_producter = 5
    producters = []
    for i in range(num_producter):
        producters.append(
            Producter('producter_%s' % i, part_queue, line_queue))

    consumer = Consumer('consumer', part_queue, line_queue)

    consumer.start()
    for prod in producters:
        prod.start()

    for prod in producters:
        prod.join()
    consumer.join()


if __name__ == '__main__':
    main()
