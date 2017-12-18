import numpy as np
import time


class MP(object):
    def __init__(self, thread_num, func, args, # {{{
            batch_size=10, random_shuffle=True, keep_order=False,\
            show_percentage=1.0, object_type='process'):
        '''
        args:
            thread_num: number of threads
            func: function, which needs to be excuted
            args: arguments, which are needed by func
                [{func_args1: xxx, func_args2: xxx, ...}, ...]
            batch_size: number of max tasks each thread will get everytime
            random_shuffle: whether need to shuffle the args 
                to make the loads of every process almost equal
            keep_order: whether the output need to be the same order of input
                it will make the output same order as the input
            show_percentage: show time and eta every (show_percentage)%
            object_type: only allowed in ['process', 'thread']
                process stands for multiprocessing
                thread stands for threading
        '''
        if object_type == 'process':
            from multiprocessing import Process, Queue
        elif object_type == 'thread':
            from threading import Thread as Process
            from queue import Queue
        else:
            assert object_type in ['process', 'thread']

        #   copy the args of the class
        self.thread_num = thread_num
        self.func = func
        self.args = args
        self.keep_order = keep_order
        self.random_shuffle = random_shuffle
        self.batch_size = batch_size
        self.task_num = len(self.args)
        data_type = type(self.args[0])
        assert data_type in [dict, list],\
            'type of each args must be in [\'dict\', \'list\'], but currently {}'\
            .format(data_type)
        self.if_dict = data_type == dict
        #   initialize the order list of args
        self.order = [i for i in range(len(self.args))]
        if self.random_shuffle:
            np.random.shuffle(self.order)
        
        #   initialize the queue for task-sending and result-sending
        self.q_task, self.q_finish = Queue(), Queue()

        #   initialize contractor and workers
        self.contractor = Process(target=self.contractor, args=())
        self.workers = [Process(target=self.worker, args=(i, )) \
                for i in range(self.thread_num)]
        
        #   initialize result and receiver
        self.result, self.receiver = [], []
    # }}}
    def worker(self, index):# {{{
        print('[OPR] worker #%d starts working ..' % index)
        while True:
            #   ask for task
            packs = self.q_task.get()
            result = []

            if len(packs) == 0:
                #   no task left
                print('[OPR] worker #%d gets salary ..' % index)
                break
            
            #   do each task from the task-packs
            for pack_id in packs:
                res = self.func(**(self.args[pack_id])) if self.if_dict \
                        else self.func(*(self.args[pack_id]))
                result.append([pack_id, res])

            #   send back the result
            self.q_finish.put(result)

        #   tell boss job finished
        self.q_finish.put([])
        print('[SUC] worker #%d finishes jobs and goes home ..' % index)
# }}}
    def contractor(self):# {{{
        print('[OPR] contractor starts assigining jobs ..')
        #   assign tasks
        for i in range(0, self.task_num, self.batch_size):
            self.q_task.put(self.order[i: i+self.batch_size])

        print('[OPR] contractor starts paying salaries ..')
        #   pay salary (tell workers no jobs any more)
        for i in range(self.thread_num):
            self.q_task.put([])
        print('[SUC] contractor finishes jobs and goes home ..')
# }}}
    def work(self):# {{{
        print('[OPR] work starts ..')
        print('-' * 48)
        self.result.clear()
        self.receiver.clear()
        #   start contrator and workers
        self.contractor.start()
        t0 = time.time()
        for worker in self.workers:
            worker.start()

        ####    receiver    ####
        
        #   initialize counters
        working_workers = self.thread_num
        finish, task_num, rate = 0, self.task_num, 0.0

        while working_workers > 0:
            #   receive results from workers
            data = self.q_finish.get()
            if len(data) == 0:
                #   a woker has done all jobs and leaves
                working_workers -= 1
                if working_workers == 0:
                    break
            else:
                self.receiver += data

            #   print logs
            finish += len(data)
            tmp = finish * 100.0 / task_num
            if tmp - rate >= 1:
                rate = tmp
                t1 = time.time() - t0
                t2 = t1 / rate * 100.0 - t1
                print('[LOG] done %.2f%% (%d/%d), TIME: %.2f, ETA: %.2f' %\
                    (rate, finish, task_num, t1, t2))
        ####    receiver    #####
        
        #   wait all process done
        self.contractor.join()
        for worker in self.workers:
            worker.join()

        #   sort the orders of receive
        if self.random_shuffle and self.keep_order:
            self.receiver.sort()

        #   get the results
        self.result = [data[1] for data in self.receiver]

        print('[SUC] all work done ..')
        print('-' * 48)
# }}}

if __name__ == '__main__':# {{{
    from IPython import embed
    def add(a, b):
        return a + b
    data = []
    # for i in range(100):
        # data.append({'a': i, 'b': i+i})
    for i in range(100):
        data.append([i, i+i])
    mp = MP(thread_num=4, func=add, args=data,\
            batch_size=3, random_shuffle=True, keep_order=True, object_type='thread')
    mp.work()
    print(mp.order[-10:])
    print(mp.receiver[-10:])
    print(mp.result[-10:])
    pass
# }}}
