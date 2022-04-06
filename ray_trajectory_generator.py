import ray 
from geopandas import GeoSeries
from shapely.geometry import Point, LineString
import numpy as np
import networkx as nx
from random import sample, choice, shuffle

@ray.remote
class TrajectoryGenerator(object):
    
    def __init__(self, G ):
        self.G = G
        self.nodes = list(G)

    def get_route_linestring(self, orig, dest):
        route = nx.shortest_path(self.G, orig, dest, 'travel_time')
        route_ls = LineString([Point(p['x'],p['y']) for p in map(lambda p: self.G.nodes[p], route)])
        return route_ls

    def discritize_line_string_to_points(self, linestring, every=range(30,70)):
        points = [
            Point(*linestring.coords[0]),
            Point(*linestring.coords[-1])
        ]
        distance = 0
        length = linestring.length
        while distance < length:
            distance+=choice(every)
            points.insert(-1, linestring.interpolate(distance))

        return points

    
    def add_gaussian_noise_to_points(self, points, scale=5, mean = 1):
        x = [p.x for p in points]
        y = [p.y for p in points]
        x = np.random.normal(loc=mean, scale=scale, size=len(x)) + x
        y = np.random.normal(loc=mean, scale=scale, size=len(x)) + y
        return [Point(x,y) for x,y in zip(x,y)]
                    

    def project_linestring(self, linestring, from_crs, to_crs):
        return GeoSeries(linestring).set_crs(from_crs).to_crs(to_crs).values[0]
        
    
    @ray.method(num_returns=1)
    def gnerate_trajectory(self):
        while True:
            try:
                shuffle(self.nodes); 
                orig, dest = list(zip(*[iter(self.nodes)]*2))[0]
                
                route = self.get_route_linestring(orig, dest)
                to_meters = self.project_linestring(route, from_crs=4326, to_crs=26992)
                points = self.discritize_line_string_to_points(to_meters)
                points = self.add_gaussian_noise_to_points(points)
                route_ls = LineString(points)
                route_ls = self.project_linestring(route_ls, from_crs=26992, to_crs=4326)
                x,y = route_ls.xy 

                return {
                    'pickup_lon': self.G.nodes[orig]['x'],
                    'pickup_lat': self.G.nodes[orig]['y'],
                    'dropoff_lon': self.G.nodes[dest]['x'],
                    'dropoff_lat': self.G.nodes[dest]['y'],
                    'lons': list(x),
                    'lats': list(y)
                }
            except Exception as e:
                pass
                # print('will try another pair because of error', e )

    @ray.method(num_returns=1)
    def gnerate_trajectory_batch(self, count):
        return [self.gnerate_trajectory() for _ in range(count)]
