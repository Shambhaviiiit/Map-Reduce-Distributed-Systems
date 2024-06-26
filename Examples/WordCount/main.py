from mpi4py import MPI
import sys
import multiprocessing
import argparse
import time
# sys.path.insert(0, '/home/shambhavi/Documents/3-2/DS/project/our-version/Map-Reduce/Library')
import sys
import os

# Add the parent directory (Library) to the Python module search path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../Library"))
sys.path.append(parent_dir)

# # Now you can import the module from the Library directory
# from Library.storage import store

from storage import store
from job import job

# from Map-Reduce.Library.storage import store

class Map:
    def execute(self, keys, values, output_store):
        for value in values:
            for word in value.split(' '):
                output_store.emit(word, 1)

class Reduce:
    def execute(self, key, values, output_store):
        output_store.emit(key,sum(values))
        

class Combine:
    def execute(self, key, values, output_store):
        # print("key , values recieved in combine.execute:" , key , values)

        count=0
        for value in values:
            count+=int(value)
        output_store.emit(key, count)
        # print("emitted to output store from combine:",key,count)
        


if __name__ == "__main__":
    mpi_comm = MPI.COMM_WORLD
    rank = mpi_comm.Get_rank()
    size = mpi_comm.Get_size()
    if rank == 0:
        print("MapReduce Example: Wordcount")
        start_time = time.time()  # Record the start time

    default_num_workers = multiprocessing.cpu_count()
    parser = argparse.ArgumentParser(description='Options')
    parser.add_argument('--directory', '-d', type=str, help='directory containing text files for word count')
    parser.add_argument('--num-map-workers', '-m', type=int, default=default_num_workers, help='number of workers for map task')
    parser.add_argument('--num-reduce-workers', '-r', type=int, default=default_num_workers, help='number of workers for reduce task')
    args = parser.parse_args()

    if not args.directory:
        if rank == 0:
            print("no input directory provided")
        sys.exit(1)

    if rank == 0:
        print("Configuration:")
        print("source directory:", args.directory)
        print("number of map workers:", args.num_map_workers)
        print("number of reduce workers:", args.num_reduce_workers)

    input_store = store(args.directory)
    # for key in input_store.get_keys():
        # print(key,input_store.get_key_values(key))
    output_store = store()

    map_fn = Map()
    combiner_fn = Combine()
    reducer_fn = Reduce()
    job = job(args.num_map_workers, args.num_reduce_workers)
    
    job.run(map_fn, combiner_fn, reducer_fn, mpi_comm, input_store, output_store)

    if rank == 0:
        print("FINAL OUTPUT")
        keys = output_store.get_keys()
        # print(len(keys))
        for key in keys:
            print(key,output_store.get_key_values(key)[0])
        end_time = time.time()  # Record the end time
        execution_time = end_time - start_time  # Calculate the execution time
        print("Execution time:", execution_time, "seconds")


        


