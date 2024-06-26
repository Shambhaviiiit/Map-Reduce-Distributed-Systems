# Map-Reduce
Implemented map reduce functionality as python libraries. Handles node failure and implements load balancing while assigning tasks to reducers

## Specifications for Use:

- Input file must be .txt files in a single directory.
- The first mapper will receive input as key value pairs with key as line number and value as the line (line belongs to lines from all the files.)
- The restof the mappers will recieve input key value pairs as emitted by the reducer.
- To run examples
    - Go to Examples folder and go to either wordcount or inverted index, whichever you want to run.
    - Run the below command:
        
        `mpiexec -np <no_processes> python3 main.py --directory <input_dir> --num-map-workers <num_mappers> --num-reduce-workers <num_reducers>` 
    - If you want to run it for more mappers, and it throws an error, try using the below command:
        `mpiexec -np <no_processes> --use-hwthread-cpus --oversubscribe python3 main.py --directory <input_dir> --num-map-workers <num_mappers> --num-reduce-workers <num_reducers>` 
    - The output will be printed in the terminal

## Dependencies:
- mpi4py
- pickle
- threading 
- datetime
