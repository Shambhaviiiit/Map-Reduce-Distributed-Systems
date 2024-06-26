from typing import List  # Required for type hints
from mpi4py import MPI  
import tags

class reduce_gather_handler:
    def __init__(self, spec, comm, intermediate_store, output_store):
        self.__spec = spec
        self.__comm = comm
        # self.output_store = istore.get_store()
        self.__output_store = output_store
        self.__istore = intermediate_store

    def run(self, reduce_fn):
        self.run_reduce_phase(reduce_fn)
        self.__comm.barrier()
        self.run_gather_phase()

    def run_reduce_phase(self, reduce_fn):
        if self.__comm.rank != 0:
            for key in self.__istore.get_keys():
                values = self.__istore.get_key_values(key)
                reduce_fn.execute(key, values, self.__output_store)
                #print(self.__comm, f"exec reducefn on key \"{key}\" with total of {len(values)} values.")

    def run_gather_phase(self):

        comm = self.__comm
        
        if not self.__spec.get_gather_on_master():
            return

        reduce_workers = list(range(1, self.__spec.get_num_reducers() + 1))

        if comm.rank == 0:
            awaiting_completion = len(reduce_workers)
            while awaiting_completion:
                msg = MPI.Status()
                data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=msg)
                # #print("STATUSSS")
                #print(msg.Get_tag())
                if msg.Get_tag() == tags.GatherPayloadDelivery:
                    # key, values = comm.recv(msg.Get_source(), tags.GatherPayloadDelivery)
                    key, values = data
                    self.__output_store.emit(key, values)
                    #print(comm, "recvd GatherPayloadDelivery with key", key, "from", msg.Get_source())
                
                elif msg.Get_tag() == tags.GatherPayloadDeliveryComplete:
                    # comm.recv(msg.Get_source(), tags.GatherPayloadDeliveryComplete)
                    #print(comm, "recvd GatherPayloadDeliveryComplete from", msg.Get_source())
                    awaiting_completion -= 1
                
                elif msg.Get_tag() == tags.MapPhasePing:
                    # comm.recv(msg.Get_source(), tags.MapPhasePing)
                    #print(msg.Get_tag())
                    pass
                
                else:
                    pass
                    # assert 0

        elif comm.rank in reduce_workers:
            for key in self.__output_store.get_keys():
                values = self.__output_store.get_key_values(key)
                comm.send((key, values), dest=0, tag=tags.GatherPayloadDelivery )
                #print(comm, "sent GatherPayloadDelivery with key", key, "to master")

            comm.send(None, dest = 0, tag = tags.GatherPayloadDeliveryComplete)
            #print(comm, "sent GatherPayloadDeliveryComplete to master")
