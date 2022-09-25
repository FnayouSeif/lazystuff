from threading import Thread, Event, Timer
from time import sleep,time
import logging
import traceback


logger = logging.getLogger("Gilfoyle")

class Job():
    """A Class that wraps a thread activity to be scheduled.
    
    Once instantiated, the input function will run in a separate thread along with the given schedule. 

    Arguments:
        every_seconds : the frequency of executing the function in seconds. Set to 0 for one time run.
        function (function) : the function to be threaded 
        kwargs : key word arguments of the function 
        max_crashes: the max number of consecutive crashes the app is allowed to have. Defaults to 10. Do not set this to a large number since the exceptions are collected in a list. Make sure to clean the list using reset exection method.
        max_number_of_runs: The max number of overall successful runs for the function before stopping. Set to -1 for infinite cycles. Defaults to -1.

    Attributes:
        status : Can be polled and values are
            INITIATED: function is in initial definition stage 
            SCHEDULED: function is scheduled to run 
            RUNNING: function is currently running
            COMPLETED: function completed successfully
            FAILED: function failed and will not rerun
            TERMINATED: function is controllably terminated (by an external non kill signal)

        exception: is None when there are no exceptions. Returns list of exceptions otherwise


    Methods: 
        __init__(self,every_seconds,function,kwargs,max_crashes)
        stop(): use to terminate the running thread. Will wait for function execution if it is running and will not schedule any further ones.
        clear_exception: clears the list containing the exceptions

    Example usage: 

    >>> def parallel_function(url):
            data = requests.post(url, payload=something)
            save_to_db(data)

    >>> my_jobs_to_monitor = dict()
    >>> for url in list_of_urls:
            job = Job(every_second=10, function=parallel_function, kwargs={'url':url},max_crashes=3,max_runs=-1)
            my_jobs_to_monitor[url] = job 

        ##now the program continues without waiting for all those functions to finish. Every url is fetched every 10 seconds in parallel (python "parallelism", not a true one)

        #check status 

    >>> for url,job in my_jobs_to_monitor.iteritems():
            print("The job working on {url} is in status {job.status} and has crashed {job.crashes} and run {job.runs} times successfully)```



    """
    def __init__(self,every_seconds,function,kwargs={},max_crashes=10,max_runs=-1):
        self.function = function 
        self.kwargs = kwargs 
        self.every_seconds = every_seconds
        self._stop_event = Event()
        self.job = None
        self.crashed = 0
        self.runs = 0
        self.max_runs = max_runs
        self.max_crashes = max_crashes
        self.status = "INITIATED"
        self.next_execution_at = 0
        self.loop_thread = Thread(target=self._loop)
        self.loop_thread.start()
        self._exception_list = []
        

        
    def _wrapper(self):
        "a wrapper for the function. Used to run the function in a threaded loop and permitting updating statuses"
        self.status = "RUNNING"
        try:
            #run the function. Mark the next execution time
            self.next_execution_at = time() + self.every_seconds
            self.function(**self.kwargs)
            self.crashed = 0
            self.runs += 1
            if self.every_seconds==0 or self.runs==self.max_runs:
                self.stop()
        except:
            #failure
            logger.error(traceback.format_exc())
            self._exception_list.append(traceback.format_exc())
            self.crashed += 1
        finally:
            if self.crashed > self.max_crashes:
                self.status = "FAILED"
                self.stop()
                raise Exception(f"Thread {str(self.function.__name__)} has crashed {self.crashed} times. Aborting...")
            #even if failed, as long as retrial is possible, status is also completed.
            self.status = "COMPLETED"
        
    def _loop(self):
        "a forever loop that checks if the function needs to be scheduled"
        terminate = False
        while True and not(terminate):
            if self._stop_event.is_set():
                #received a termination signal based on Event threading class
                logger.warning("Thread received termination signal.")
                self.status = "TERMINATED" if self.status != "FAILED" else self.status
                terminate=True

            elif self.status in ["RUNNING","SCHEDULED"]:
                #nothing to do here
                logger.debug(f"Thread {self.function.__name__} is {self.status}")

            elif self.status in ["COMPLETED","INITIATED"]:
                #function is completed, check if the run time exceeded the expected schedule. Run immediately is that is true
                
                execute_in_seconds = round(self.next_execution_at - time()) if time() < self.next_execution_at else 1
                self.job = Timer(execute_in_seconds,function=self._wrapper)
                self.job.start()
                self.status = "SCHEDULED"

            sleep(1)
    
    def stop(self):
        "stops the running thread. Will wait for the function to finish if it is running when this method is called"
        self._stop_event.set()

    @property
    def exception(self):
        "gets the exception list that occured before crashing. Returns None if no exceptions occured"
        if len(self._exception_list)==0:
            return None 
        else:
            return self._exception_list

    def clear_exception(self):
        "cleans exception from this class. Usage is recommended to lower memory usage"
        self._exception_list = []