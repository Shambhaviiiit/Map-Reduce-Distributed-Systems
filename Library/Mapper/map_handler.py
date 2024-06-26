from mpi4py import MPI
from Mapper.map_input_handler import map_input_handler
# from Library.storage import store
from datetime import datetime, timedelta
import threading
import time
from enum import Enum
from task import task
import tags
import pickle
       
class map_handler:

    def __init__(self, input_store, intermediate_store, specs, comm):
        self.__map_tasks = []
        self.__intermediate_store = intermediate_store 
        self.__input_store = input_store
        self.__specs = specs
        self.__comm = comm
        self.__failed_works = []

        # all outputs will be appended to the intermediate storage
        # this will be the input for reducer and output of mapper

    def run(self, mapper_fn):
        comm = self.__comm
        rank = comm.Get_rank()
        # print("this is process     ",rank)
        # assert(comm.Get_size() >= self.__specs.get_num_mappers()+1)

        # root process
        map_workers = range(1,self.__specs.get_num_mappers()+1)
        if(rank == 0):

            for i in range(1, self.__specs.get_num_mappers()+1):
                # print("sending map phase begin to", i)
                try:
                    # comm.send(tags.StartMapPhase, dest=i)
                    comm.send(None, dest=i, tag=tags.StartMapPhase)
                except MPI.Exception as e:
                    print("Error occurred while sending message to process", i)
                    print("Error message:", e)
                # else:
                    # print("Message sent successfully to process", i)

            map_tasks_pending = 0
            failed_tasks = 0
            input_tasks = map_input_handler(self.__input_store, self.__specs.get_num_mappers())
            # print("all tasks in input_tasks")
            # print(input_tasks.get_all_tasks())

            # print("right till map input handler")
            # assigns task to a node with rank=rank and if reassign = true, then reassigns the task
            def assign_task(rank, task, map_tasks_pending, reassign=False):
                if task == None:
                    return

                task.worker = rank
                task.status = "assigned"
                task.start_time = datetime.now()
                task.last_ping_time = datetime.now()

                if not reassign:
                    task.id = len(self.__map_tasks)
                    self.__map_tasks.append(task)
                    map_tasks_pending += 1

                # print("assigned task", task.id, "that is line : ",task.value, "to process", rank)
                serialized_task = pickle.dumps(task)  
                comm.send(serialized_task, dest=rank, tag=tags.AssignedMapTask)
                return map_tasks_pending
            
            # Initially assign work to all nodes
            for i in range(1, self.__specs.get_num_mappers()+1):
                task = input_tasks.get_next_task()
                if(task == None):
                    break
                # print("Initially assigning task ", task.id, "thai is line", task.value, "to process", i)
                map_tasks_pending = assign_task(i,task,map_tasks_pending)


            while map_tasks_pending:
             
                status = comm.Iprobe()
                # print("Printing from root again")
                # print(status.Get_source())
                # print(status)
                if not status:
                    # No incoming messages, sleep for ping frequency 
                    # print(self.__specs.get_ping_frequency())
                    # print("ping")
                    time.sleep(self.__specs.get_ping_frequency().total_seconds())  
                            
                    # Check for failures
                    # print("no of tasks" , len(self.__map_tasks))
                    for task in self.__map_tasks:
                        cur_time = datetime.now()
                        last_ping_time = task.last_ping_time
                        if ((cur_time - last_ping_time) > self.__specs.get_ping_failure_time() and task.worker != -1 and task.status != "completed"):
                            # print((cur_time - last_ping_time).total_seconds() * 1000, "ms delay")
                            print(comm, "worker", task.worker, "has failed; saving tasks for re-execution")
                            
                            for t2 in self.__map_tasks:
                                if t2.worker == task.worker:
                                    t2.worker = -1
                                    failed_tasks += 1
                                    
                                self.__failed_works.append(task.worker)
                                # map_tasks_pending-=1
                else:
                    
                    msg_status_root = MPI.Status()
                    data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=msg_status_root)
                    # print("receiving some message in root")
                    if msg_status_root.Get_tag() == tags.MapPhasePing:
                        task_id = data
                        # comm.recv([task_id, MPI.INT], source=msg.source, tag=tags.MapPhasePing)
                        # task_id = msg_status_root.Get_source()
                        if task_id is not None:
                            self.__map_tasks[task_id].last_ping_time = datetime.now()
                        # print(comm, "received MapPhasePing from", msg_status_root.Get_source(), "for task_id", task_id)
                    
                    elif msg_status_root.Get_tag() == tags.CompletedMapTask:

                        task_id = data
                        # comm.recv([task_id, MPI.INT], source=msg.source, tag=tags.CompletedMapTask)
                        # print(comm, "received MapTaskCompletion from", msg_status_root.Get_source(), "for task_id", task_id)
                        
                        map_tasks_pending -= 1
                        self.__map_tasks[task_id].status = "completed"
                        self.__map_tasks[task_id].end_time= datetime.now()

                        next_task = input_tasks.get_next_task()
                        if next_task:
                            map_tasks_pending = assign_task(msg_status_root.Get_source(), next_task, map_tasks_pending)

                        else:
                            if failed_tasks:
                                for i in range(len(self.__map_tasks)):
                                    task = self.__map_tasks[i]

                                    # re assign tasks
                                    if task.worker == -1:
                                        map_tasks_pending = assign_task(msg_status_root.Get_source(), task, map_tasks_pending, True)
                                        failed_tasks -= 1

                    else:
                        pass
                        #print()
                        # print("received: ")
                        # print(msg_status_root.Get_tag())
                        
            # after finishing all map tasks, send MapPhaseEnd message to all workers
            for i in range(1, self.__specs.get_num_mappers() + 1):
                # print(comm, "sending MapPhaseEnd to", i)
                comm.send(None, dest=i, tag=tags.MapPhaseEnd)

        # non root processes
        elif comm.rank in map_workers:
            msg = comm.recv(source=0, tag = tags.StartMapPhase)
            # print("Received StartMapPhase")
            # print(msg)
            # mapper_fn.execute(my_task.key, my_task.value, self.__intermediate_store)
            ping_flag = True
            current_task_id = -1
            current_task = None

            def ping_root():
                while ping_flag != False:
                #    if current_task_id != -1:
                    # print("Sending Pinggggg")
                    comm.send(current_task_id, dest=0, tag = tags.MapPhasePing)
                #   print(self.__specs.get_ping_frequency())
                #   print("ping")
                    time.sleep(self.__specs.get_ping_frequency().total_seconds()) 

            ping_thread = threading.Thread(target = ping_root)
            ping_thread.start()

            # receive task
            # comm.recv(current_task, 0, tags.StartMapPhase)
            # mapper_fn.execute(current_task.key, current_task.value, self.__intermediate_store)

            while True:
                # Probe for a message
                msg_status = MPI.Status()
                msg = comm.recv(source=0, tag=MPI.ANY_TAG, status=msg_status)
                # print(msg)
                # Check message tag
                if msg_status.Get_tag() == tags.AssignedMapTask:
                    # print("message printing here")
                    # print(msg)
                    task = pickle.loads(msg)
                    # print(task)
                    # print(task.id)
                    # task_id, inputs = msg
                    # task_id = msg.id

                    # print("")
                    current_task_id = task.id

                    # print("Process", rank, "received tags.AssignedMapTask with task id", task.id)
                    # for i in range(0, len(task.key)):
                        # print("Process", rank, "executing map function on key", task.key[i])
                        # , "with", len(values), "values.")
                    mapper_fn.execute(task.key, task.value, self.__intermediate_store)
                    
                    # for key, values in inputs.items():
                    #     print("Process", rank, "executing map function on key", key, "with", len(values), "values.")
                    #     for value in values:
                    #         mapper_fn.execute(key, value, self.__input_store)

                    current_task_id = -1
                    # print("Process", rank, "sent MapTaskCompletion with task id", task.id)
                    comm.send(task.id, dest=0, tag=tags.CompletedMapTask)
                    
                elif msg_status.Get_tag() == tags.MapPhaseEnd:
                    # print("Process", rank, "received MapPhaseEnd")
                    break

                else:
                    assert(0)

            ping_flag = False
            ping_thread.join()
        
        else:
            pass
                    