#%%
import osmnx as ox
from tqdm import tqdm 
import ray
from ray.util import ActorPool
from ray_trajectory_generator import TrajectoryGenerator
import os
import gzip
from pathlib import Path
from math import ceil 

from config import *
#%%




ox.config(use_cache=True, log_console=True)
G = ox.graph_from_place(place, network_type='drive')

# impute missing edge speed and add travel times
G = ox.add_edge_speeds(G)
G = ox.add_edge_travel_times(G)




#%%
os.environ['RAY_SCHEDULER_EVENTS'] = '0'

# local:
ray.init(ignore_reinit_error=True)

# for cluster
# ray.init(address='auto', _redis_password='#######',ignore_reinit_error=True)

CPUs = int(min(150, ray.available_resources()['CPU']))
actors = [TrajectoryGenerator.remote(G) for _ in range(CPUs)]
pool = ActorPool(actors)


folder = Path(output_dir)
if os.path.exists(folder):
    raise RuntimeError('Folder with the same name exists. The generator stopped to prevent overwritting any existing files. Choose another name or remove the previous file')
else:
    os.mkdir(folder)



file_number = 0
trj_id = 0
batch_size = 100
with tqdm(total=count) as pbar:
    
    # will use batches to make it faster. However, it may generate slightly larger than required if the batch size (e.g. 100)
    # is not a divider of the required amount. (count mod batch_size != 0). Also count per file may be slightly larger than required.
    batches = pool.map_unordered(lambda actor, _: actor.gnerate_trajectory_batch.remote(batch_size), range(ceil(count/batch_size)))
    while trj_id < (count-1):

        filename = folder / f'trajectories_{file_number}.csv.gz'
        pbar.set_description(f"Generating file # {file_number}")
        count_in_this_file = 0

        with gzip.open(filename, 'wt', compresslevel=6) as out:
        # with open(filename, 'wt') as out:

            print('trj_id,pickup_lon,pickup_lat,dropoff_lon,dropoff_lat,lons,lats', file=out)
            for trj_batch in batches:
                for trj in trj_batch:
                    print(
                        trj_id,
                        trj['pickup_lon'],
                        trj['pickup_lat'],
                        trj['dropoff_lon'],
                        trj['dropoff_lat'],
                        ' '.join(map(str,trj['lons'])),
                        ' '.join(map(str,trj['lats'])),
                        sep=',',
                        file=out
                    )
                    trj_id +=1
                    count_in_this_file+=1

                pbar.update(batch_size)

                if count_in_this_file >= per_file:
                    file_number+=1
                    break