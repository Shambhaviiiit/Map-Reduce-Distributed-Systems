# mapreduce.py

from mpi4py import MPI
import os
from task import task
# from store import store
import tags

class map_input_handler:

    def __init__ (self,store,num_mappers):
        self.__tasks = []
        self.__make_tasks_array(store,num_mappers)
        self.__task_ptr = -1

    def __make_tasks_array(self,store,num_mappers):
        keys = store.get_keys()
        no_lines = len(keys)
        each_mapper = max(1,int(no_lines/num_mappers)+1)
        # print("each mapper:", each_mapper)

        # LOad balancing to give approx equal number of keys/lines to each task
        # Initialize a list to hold arrays
        balanced_arrays = [[] for _ in range(num_mappers)]

        # Initialize a variable to keep track of the current mapper
        current_mapper = 0

        # Iterate over keys and distribute them into balanced_arrays
        for key in keys:
            balanced_arrays[current_mapper].append(key)
            # Check if the current array for the mapper has reached each_mapper
            if len(balanced_arrays[current_mapper]) >= each_mapper:
                # Move to the next mapper
                current_mapper += 1

        # If there are keys remaining, store them in the last array
        if(len(balanced_arrays) * each_mapper < no_lines):
            remaining_keys = keys[len(balanced_arrays) * each_mapper:]
            balanced_arrays.append(remaining_keys)

        for key_array in balanced_arrays:
            values_arr = []
            for key in key_array:  
                value = store.get_key_values(key)
                values_arr.append(value)
            task_ = task(key_array,values_arr)
            # print(key_array,values_arr)
            self.__tasks.append(task_)
        # print("tasks:",self.__tasks)
    
    def get_next_task(self):
        # print("on task pointer ",self.__task_ptr)
        self.__task_ptr+=1
        if(len(self.__tasks) > self.__task_ptr):
            return self.__tasks[self.__task_ptr]
        else:
            return None
    
    def get_all_tasks(self):
        return self.__tasks
    
 
        
        





