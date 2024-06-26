from storage import store
from Mapper.map_input_handler import map_input_handler
from Mapper.map_handler import map_handler
from map_combine_handler import map_combine_handler
from reduce_gather_handler import reduce_gather_handler
from specs import specs
from task import task
from mpi4py import MPI

class job:

    def __init__(self, num_mappers, num_reducers):
        
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        
    def run(self, mapper_fn, combiner_fn, reducer_fn, comm, input_store, output_store):

        specs_ = specs(self.num_mappers, self.num_reducers)
        intermediate_store = store()

        assert(specs_.get_num_mappers() >= 1)
        assert(specs_.get_num_reducers() >= 1)
        
        # if comm.rank ==0:
        #     for key in input_store.get_keys():
        #         print(key, input_store.get_key_values(key))
        
        map_handler(input_store, intermediate_store, specs_, comm).run(mapper_fn)
        # print("completing map handler")
        comm.barrier()
        # print("completing barrier")
        # print("ONE")
        # for key in intermediate_store.get_keys():
            # print(key, intermediate_store.get_key_values(key))
        intermediate_store = map_combine_handler(intermediate_store, comm, specs_).run(combiner_fn)
        # print("completing map combine handler")
        comm.barrier()
        # print("TWO")
        # for key in intermediate_store.get_keys():
            # print(key, intermediate_store.get_key_values(key))
        # print("completing barrier")
        reduce_gather_handler(specs_, comm, intermediate_store, output_store).run(reducer_fn)
        # print("completing reduce gather handler")
        comm.barrier()
        # print("THREE")
        # for key in intermediate_store.get_keys():
            # print(key, intermediate_store.get_key_values(key))
        # print("completing barrier")
