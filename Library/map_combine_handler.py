from typing import TypeVar, Generic
from mpi4py import MPI
import heapq
from storage import store
import tags
from functools import reduce

class DefaultCombiner:
    # Define a custom function to dynamically select the appropriate operation
    def __custom_reduce(self,iterable):
        if all(isinstance(item, int) for item in iterable):
        # If all elements are integers, use addition
            return reduce(lambda x, y: x + y, iterable)
        elif all(isinstance(item, float) for item in iterable):
        # If all elements are floats, use addition
            return reduce(lambda x, y: x + y, iterable)
        # Add more elif blocks for other datatypes and corresponding operations
        else:
        # Default case: use the first element as the initial value and concatenate strings
            return reduce(lambda x, y: str(x) + str(y), iterable)
        
    def execute(self, key, values, store):
        store.emit(key, self.__custom_reduce(values))

class map_combine_handler:

    def __init__(self, intermediate_store, comm, specs):
        self.__istore = intermediate_store
        self.__comm = comm
        self.__specs = specs

    def run(self, combine_fn = DefaultCombiner()):
        self.run_combine_phase(combine_fn)
        self.__comm.barrier()
        self.run_shuffle_phase()
        return self.__istore
        
    def run_combine_phase(self, combine_fn):
          
        # if not isinstance(combiner_t, DefaultCombiner[IntermediateStore.key_t, IntermediateStore.value_t]):
        combiner_istore = store()
        
        # print(self.__istore.get_keys(),len(self.__istore.get_keys()))
        for key in self.__istore.get_keys():
            values = self.__istore.get_key_values(key)
            
            # print(key,values)
            combine_fn.execute(key, values, combiner_istore)
        
        self.__istore = combiner_istore
            

    def run_shuffle_phase(self):
        # workers has the list of ranks of the map processes in `comm`
        spec = self.__specs
        comm = self.__comm
        istore = self.__istore
       
        map_workers = list(range(1, spec.get_num_mappers() + 1))
        assert comm.size >= spec.get_num_mappers() + 1
    
        # workers has the list of ranks of the map processes in `comm`
        reduce_workers = list(range(1, spec.get_num_reducers() + 1))
        assert comm.size >= spec.get_num_reducers() + 1
    
        if comm.rank == 0:
            global_counts = {}
            for p in map_workers:
                counts = comm.recv(source=p, tag=tags.ShuffleIntermediateCounts)
                # print("count",counts)
                for key, c in counts.items():
                    # print("key , c",key ,c)
                    global_counts[key] = global_counts.get(key, 0) + c
            # print(global_counts)
    
            key_counts = [(value, key) for key, value in global_counts.items()]
            key_counts.sort()
    
            load_balancer_pq = [(0, i) for i in range(len(reduce_workers))]
            heapq.heapify(load_balancer_pq)
    
            process_map = {}
            for count, key in key_counts:
                min_makespan, min_reduce_worker_idx = heapq.heappop(load_balancer_pq)
                process_map[key] = reduce_workers[min_reduce_worker_idx]
                heapq.heappush(load_balancer_pq, (min_makespan + count, min_reduce_worker_idx))
    
            for p in map_workers:
                comm.send(process_map, dest=p, tag=tags.ShuffleDistributionMap)
        else:
            new_istore = store()
            if comm.rank in map_workers:
                counts = self.__istore.get_key_counts()
                # print("COUNTS: ")
                # for key in istore.get_keys():
                    # print(key,istore.get_key_values(key))
                    
                comm.send(counts, dest=0, tag=tags.ShuffleIntermediateCounts)
    
                process_map = comm.recv(source=0, tag=tags.ShuffleDistributionMap)
    
                for key, p in process_map.items():
                    if not istore.is_key_present(key):
                        continue
                    
                    values = istore.get_key_values(key)
                    if p != comm.rank:
                        comm.send((key, values), dest=p, tag=tags.ShufflePayloadDelivery)
                    else:
                        new_istore.emit(key, values)
    
                for p in reduce_workers:
                    # if p != comm.rank:
                    comm.send("shuffle_done", dest=p, tag=tags.ShufflePayloadDeliveryComplete)
    
            if comm.rank in reduce_workers:
                awaiting_completion = len(map_workers)
                while awaiting_completion:
                    status = MPI.Status()
                    if comm.probe():
                        msg= comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                         # Get the tag of the received message from the status object
                        tag=status.Get_tag()
                        # print("msg tag ",tag)
                        # print(msg is None)
                        # print("Received message:", msg)  # Add this line for debugging
                        if msg is not None:  # Check if msg is not None before accessing its attributes
                            if tag == tags.ShufflePayloadDelivery:
                                key, values = msg
                                # print("recieved message : " , key , values)
                                new_istore.emit(key, values)
                            elif tag == tags.ShufflePayloadDeliveryComplete:
                                # print("here")
                                # msg = comm.recv(source=msg.source, tag=tags.ShufflePayloadDeliveryComplete)
                                # print("recieved shufflepay complete msg")
                                awaiting_completion -= 1
                            else:
                                assert 0
                        else:
                            pass
                            # print("Received None message, skipping processing.")  # Add this line for debugging

            
            self.__istore = new_istore
            # print("PRINTING")
            # for key in self.__istore.get_keys():
            #     print(key,self.__istore.get_key_values(key))
    



