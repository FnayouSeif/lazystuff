# lazystuff
A repository where I share some useful things I have done before out of curiosity, boredom or mainly, laziness. Contributions and helpful notes/recommendations are very welcomed <3



# Content 

## I. PARALLELISM 
Use classes or methods for parallelisation of python. 

1. `thread_scheduler.py` : 

    *  The class `Job` allows the scheduling of a thread in a python program. A job can be created and a new thread is launched enabled thus performing different tasks in parallel. It is only effecient if the threads are performing IO tasks. 

