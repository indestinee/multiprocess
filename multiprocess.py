import numpy as np
import time, os, pickle


class MP(object):
    def __init__(self, thread_num, func, args, # {{{
            batch_size=10, random_shuffle=True, keep_order=False,
            show_percentage=1, object_type='process', worker_prepare=None,
            shutdown_continue=None, queue_max_size=1024):
        '''
        args:
            thread_num: number of threads
            func: function, which needs to be excuted
            args: arguments, which are needed by func
                1. list of dict [{args0: xxx, args1: xxx, ...}, ...]
                2. list of list [[args0, args1, args2, ...], ...]
                3. generator
            batch_size: number of max tasks each thread will get everytime
            random_shuffle: whether need to shuffle the args 
            keep_order: whether the output need to be sorted 
                to be the same order as input
            show_percentage: show time and eta every (show_percentage)%
            object_type: only allowed in ['process', 'thread']
                process stands for multiprocessing
                thread stands for threading
            worker_prepare: the function needs to excuse by each process
            shutdown_continue: whether save process or load from process
                None for not, or path of cache data
            queue_max_size: max size of assigining and finishing queue
                for save the memory, when IO is much slower than func
        '''
        if object_type == 'process':
            from multiprocessing import Process, Queue
        elif object_type == 'thread':
            from threading import Thread as Process
            from queue import Queue
        else:
            assert object_type in ['process', 'thread']
        
        #   copy the args of the class
        self.worker_prepare = worker_prepare
        self.thread_num = thread_num
        self.func = func
        self.args = args
        self.keep_order = keep_order
        self.random_shuffle = random_shuffle
        self.batch_size = batch_size
        self.shutdown_continue = shutdown_continue
        self.show_percentage = show_percentage

        if self.shutdown_continue:
            if os.path.isfile(self.shutdown_continue):
                print('[OPR] cache found in %s, loading ..' % \
                    self.shutdown_continue)
                with open(self.shutdown_continue, 'rb') as f:
                    self.finish = pickle.load(f)
            else:
                self.finish = set()
                print('[LOG] no cache found ..')
        else:
            self.finish = set()
            
        #   initialize the order list of args
        if isinstance(self.args, list):
            self.order = [i for i in range(len(self.args)) \
                if i not in self.finish]
            self.task_num = len(self.order)
            if self.random_shuffle:
                np.random.shuffle(self.order)
        else:
            self.task_num = None
        
        #   initialize the queue for task-sending and result-sending
        self.q_task = Queue(queue_max_size)
        self.q_finish = Queue(queue_max_size)

        #   initialize contractor and workers
        self.contractor = Process(target=self.contractor, args=())
        self.workers = [Process(target=self.worker, args=(i, )) \
            for i in range(self.thread_num)]
        #   initialize result and receiver
        self.result, self.receiver = [], []
    # }}}
    def done(self, index):# {{{
        if isinstance(index, list):
            self.finish.add(index[0])
    # }}}
    def save(self):# {{{
        assert self.shutdown_continue
        with open(self.shutdown_continue, 'wb') as f:
            pickle.dump(self.finish, f)
    # }}}
    def worker(self, index):# {{{
        if self.worker_prepare:
            print('[OPR] workers prepare to work ..')
            self.worker_prepare(index=index)
        print('[OPR] worker #%d starts working ..' % index)
        while True:
            #   ask for task
            packs = self.q_task.get()
            result = []
            if len(packs) == 0:
                #   no task left
                break
            
            #   do each task from the task-packs
            for pack_id in packs:
                if isinstance(pack_id, int):
                    #   receive package id go to pack the package
                    data = self.args[pack_id]
                elif isinstance(pack_id, list):
                    #   receive package
                    data = pack_id[1]
                else:
                    assert isinstance(pack_id, int) and \
                        isinstance(pack_id, list)
                res = self.func(**data) if isinstance(data, dict) \
                        else self.func(*data)
                result.append([pack_id, res])
            #   send back the result
            self.q_finish.put(result)
        #   tell boss job finished
        self.q_finish.put([])
        print('[SUC] worker #%d finishes jobs and goes home ..' % index)
    # }}}
    def contractor(self):# {{{
        #   assign tasks
        if isinstance(self.args, list):
            print('[OPR] contractor starts assigining %d jobs ..'\
                    % self.task_num)
            for i in range(0, self.task_num, self.batch_size):
                self.q_task.put(self.order[i: i+self.batch_size])
        else:
            print('[OPR] contractor starts assigining jobs (generator) ..')
            pack = []
            for i, data in enumerate(self.args):
                pack.append([i, data])
                if i % self.batch_size == 0:
                    self.q_task.put(pack)
                    pack.clear()
            if len(pack) > 0:
                self.q_task.put(pack)

        for i in range(max(1, self.thread_num)):
            self.q_task.put([])
        print('[SUC] contractor\'s jobs done ..')
    # }}}
    def work_start(self):# {{{
        print('[OPR] work starts ..')
        print('-' * 48)
        #   start contrator and workers
        self.contractor.start()
        self.t0 = time.time()
        for worker in self.workers:
            worker.start()
    # }}}
    def work_finish(self):# {{{
        #   wait all process done
        self.contractor.join()
        for worker in self.workers:
            worker.join()
        #   sort the orders of receive
        if self.keep_order:
            self.receiver.sort()
        #   get the results
        self.result = [data[1] for data in self.receiver]
        print('[SUC] all work done ..')
        print('-' * 48)
    # }}}
    def run_receiver(self):# {{{
        ####    receiver    ####
        #   initialize counters
        if self.thread_num == 0:
            self.worker(0)
            
        working_workers = max(1, self.thread_num)
        if self.task_num:
            finish, task_num, rate = 0, self.task_num, 0.0
        while working_workers > 0:
            #   receive results from workers
            data = self.q_finish.get()
            if len(data) == 0:
                #   a worker has done all jobs and leaves
                working_workers -= 1
                if working_workers == 0:
                    break
            else:
                for each in data:
                    yield each
            #   print logs
            if self.thread_num == 0:
                continue
            if self.task_num:
                finish += len(data)
                assert task_num > 0, '[ERR] no tasks left ..'
                tmp = finish * 100.0 / task_num
                if tmp - rate >= self.show_percentage:
                    rate = tmp
                    t1 = time.time() - self.t0
                    t2 = t1 / rate * 100.0 - t1
                    print('[LOG] done %.2f%% (%d/%d),\
TIME: %.2f, ETA: %.2f' % (rate, finish, task_num, t1, t2))
    
        ####    receiver    #####
    # }}}
    def generator(self):# {{{
        self.work_start()
        for data in self.run_receiver():
            yield data
        self.work_finish()
    # }}}
    def work(self):# {{{
        self.work_start()
        for data in self.run_receiver():
            self.receiver += data
        self.work_finish()
    # }}}

if __name__ == '__main__':# {{{
    from IPython import embed
    def add(a, b):
        time.sleep(0.01)
        return [a+b]
    list_input = []
    dict_input = []
    for i in range(100):
        dict_input.append({'a': i, 'b': i+i})
    for i in range(100):
        list_input.append([i, i+i])
    generator = ([i, i + 1] for i in range(100))
    mp = MP(thread_num=4, func=add, args=generator,\
        batch_size=3, random_shuffle=True, keep_order=True,\
        object_type='thread', shutdown_continue='./save.bin', queue_max_size=10)
    
    if True:
        #   save memory, get from generator
        for i, [pack_id, data] in enumerate(mp.generator()):
            print('Data:', data)
            mp.done(pack_id)
            time.sleep(0)
    else:   
        #   run as default
        mp.work()
        #   answer in mp.result
        # print(mp.result[-10:])

    print('\n')
    print('-' * 64)
    input('Press Enter to continue ..\n')

    pass
# }}}
