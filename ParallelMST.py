import os
import time
from pyspark import SparkContext, SparkConf
from random import random
from MST import kruskal, prim
from DisjointSet import DisjointSet



# os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home'
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.5'


def shuffle(seq):
    """
    to shuffle a sequence

    :param seq: a sequence.
    :return: a sequence after shuffling.
    """
    return sorted(seq, key=lambda k: random())


def edge_partitioning(sc, nodes, edges, num_partition=4):
    """
    implementation of edge partitioning Kruskal's algorithm

    :param nodes: nodes for input.
    :param edges: edges for input.
    :param num_partition: number of partitions.
    :return:
    """
    # parallelize
    edges = sc.parallelize(shuffle(edges), num_partition)

    # define function for calculating local MSTs
    def local_kruskal(iterator):
        for edge in kruskal(nodes=nodes, edges=iterator):
            yield edge
    # calculate local MSTs
    subtrees = edges.mapPartitions(local_kruskal).collect()

    # calculate the global MST
    return kruskal(nodes=nodes, edges=subtrees)


def vertex_partitioning(sc, nodes, edges, num_partition=4):
    """
    implementation of vertex partitioning Kruskal's algorithm

    :param nodes: nodes for input.
    :param edges: edges for input.
    :param num_partition: number of partitions.
    :return:
    """
    # define function for calculating combinations of different vertex partitions
    def combine(iterator):
        for i in iterator:
            if i[0] < i[1]:
                yield i[0] + i[1]
    vertices = sc.parallelize(shuffle(nodes), num_partition).glom()
    vertices = vertices.cartesian(vertices) \
                       .mapPartitions(combine, preservesPartitioning=False)

    # parallelize
    vertices = sc.parallelize(vertices.collect(), num_partition)

    # define function for calculating local MSTs
    def local_kruskal(iterator):
        for subset in iterator:
            for edge in kruskal(nodes=set(subset), edges=edges):
                yield edge
    # calculate local MSTs
    subtrees = vertices.mapPartitions(local_kruskal).distinct().collect()

    # calculate the global MST
    return kruskal(nodes=nodes, edges=subtrees)


def parallel_prim(sc, nodes, edges, num_partition=4):
    """
    implementation of parallel Prim's algorithm

    :param nodes: nodes for input.
    :param edges: edges for input.
    :param num_partition: number of partitions.
    :return:
    """
    # edges of the minimum spanning tree
    mst = []

    # initialize a forest for all nodes
    forest = DisjointSet(nodes)

    # define function for generating graph
    def generate_graph(iterator):
          for edge in iterator:
              for i in range(2):
                  yield (edge[i], (edge[1-i], edge[2]))
    # store the graph in an adjacency list
    adjacent = sc.parallelize(edges, num_partition) \
                 .mapPartitions(generate_graph, preservesPartitioning=True) \
                 .groupByKey(numPartitions=num_partition) \
                 .mapValues(lambda x: sorted(x, key=lambda y: y[1])) \
                 .persist()

    # candidate edges of the global MST
    candidates = [None]
    # loop until there is no candidate
    while len(candidates) != 0:
        # broadcast the forest to each machine
        connection = sc.broadcast(forest)

        # define function for finding minimum edges leaving each disjoint set
        def find_minimum(iterator):
            for group in iterator:
                src = group[0]
                for (dst, weight) in group[1]:
                    if connection.value.find(src) != connection.value.find(dst):
                        yield (src, dst, weight) if src < dst else (dst, src, weight)
                        break
        # obtain the list of minimum edges leaving each disjoint set
        candidates = sorted(adjacent.mapPartitions(find_minimum).distinct().collect(), key=lambda x: x[2])

        # calculate the global MST
        for candidate in candidates:
            # find the parents of src and dst respectively
            if forest.unite(candidate[0], candidate[1]):
                # add the current edge into the minimum spanning tree if it doesn't make a circuit
                mst.append(candidate)

    # return the global MST
    return mst


def get_nodes_edges(file):
    nodes, edges = set(), list()
    with open(file, "r") as f:
        for line in f.readlines():
            line = line.strip().split()
            # insert nodes
            nodes.add(line[0])
            nodes.add(line[1])
            # insert edges
            edges.append((line[0], line[1], line[2] if len(line) is 3 else 1.0))
        
    return nodes, edges, len(nodes), len(edges)

def save_result(file, lists):
    lists = [str(line) + "\n" for line in lists]
    with open(file, "w") as f:
        f.writelines(lists)



def main():
    # nodes_0 = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
    # edges_0 = [('A', 'B', 10), ('A', 'F', 11),
    #            ('B', 'C', 18), ('B', 'G', 16), ('B', 'I', 12),
    #            ('C', 'D', 22), ('C', 'I', 8),
    #            ('D', 'E', 20), ('D', 'H', 16), ('D', 'I', 21),
    #            ('E', 'F', 26), ('E', 'G', 7), ('E', 'H', 19),
    #            ('F', 'G', 17),
    #            ('G', 'H', 18)]

    # nodes_1 = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    # edges_1 = [('A', 'B', 7), ('A', 'D', 5),
    #            ('B', 'C', 8), ('B', 'D', 9), ('B', 'E', 7),
    #            ('C', 'E', 5),
    #            ('D', 'E', 15), ('D', 'F', 6),
    #            ('E', 'F', 8), ('E', 'G', 9),
    #            ('F', 'G', 11)]
    
    # data_set = "data/netscience.net" 
    # http://vlado.fmf.uni-lj.si/pub/networks/data/collab/netscience.htm
    # 1589 nodes and 2742 edges
    # Prim                      0.7 (s)
    # Kruskal                   0.1 (s)
    # Edge_partitioning         8.1 (s)
    # Vertex_partitioning       28.1 (s)
    # Parallel_prim             16.9 (s)

    # data_set = "data/cora.cites" 
    # https://s3.us-east-2.amazonaws.com/dgl.ai/dataset/cora_raw.zip
    # 2708 nodes and 5429 edges 
    # Prim                      2.6 (s)
    # Kruskal                   0.2 (s)
    # Edge_partitioning         8.4 (s)
    # Vertex_partitioning       28.8 (s)
    # Parallel_prim             21.1 (s)

    
    # data_set = "data/com-dblp.ungraph.txt" 
    # http://snap.stanford.edu/data/com-DBLP.html
    # 317,080 nodes and 1,049,866 edges
    # Edge partitioning         21.0 (s)         
    # Vertex partitioning       72.1 (s)
    # Parallel prim             63.5 (s)

    # data_set = "data/web-NotreDame.txt" 
    # http://snap.stanford.edu/data/web-NotreDame.html
    # 325,729 nodes and 1,497,134 edges
    # Edge partitioning          17.9 (s)         
    # Vertex partitioning        72.6 (s)
    # Parallel prim              46.1 (s)

    # data_set = "data/web-Stanford.mtx" 
    # 281,903 nodes and 2,312,497 edges
    # Edge_partitioning         38.0 (s)
    # Vertex_partitioning       95.8 (s)
    # Parallel_prim             117.6 (s)   

    # data_set = "data/web-Google.txt" 
    # http://snap.stanford.edu/data/web-Google.html
    # 875,713 nodes and 5,105,039 edges
    # Edge partitioning         57.9 (s)         
    # Vertex partitioning       180.9 (s)
    # Parallel prim             195.2 (s)


    # data_set = "data/web-BerkStan.txt" 
    # http://snap.stanford.edu/data/web-BerkStan.html
    # 685,230 nodes and 7,600,595 edges
    # Edge partitioning         69.4 (s)         
    # Vertex partitioning       231.8 (s)
    # Parallel prim             334.7 (s)

    # data_set = "data/roadNet-PA.txt" 
    # http://snap.stanford.edu/data/roadNet-PA.html
    # 1,088,092 nodes and 1,541,898 edges
    # Edge partitioning         62.7 (s)         
    # Vertex partitioning       185.8 (s)
    # Parallel prim             202.7 (s)

    # data_set = "data/as-skitter.txt" 
    # http://snap.stanford.edu/data/as-Skitter.html
    # 1,696,415nodes and  11,095,298 edges
    # Edge partitioning          (s)         
    # Vertex partitioning        (s)
    # Parallel prim              (s)

    # Convert file to nodes and edges
    print("Convert file to nodes and edges %s" % data_set)
    t0 = time.time()
    nodes, edges, num_nodes, num_edges = get_nodes_edges(data_set)
    t1 = time.time()
    print("Convert finish %s (s)" % str(t1 - t0))
    print("%s nodes and %s edges" % (str(num_nodes), str(num_edges)))

    # Prim
    # t2 = time.time()
    # prim_result = prim(nodes, edges, 5)
    # t3 = time.time()
    # print("Prim %s (s)" % str(t3 - t2))
    # print("%s edges" % len(prim_result))
    # save_result("result/prim_result.txt", prim_result)

    # Kruskal
    # t4 = time.time()
    # kruskal_result = kruskal(nodes, edges)
    # t5 = time.time()
    # print("Kruskal %s (s)" % str(t5 - t4))
    # print("%s edges" % len(kruskal_result))
    # save_result("result/kruskal_result.txt", kruskal_result)

    # Spark configure
    conf = SparkConf()
    conf.set('spark.executor.memory', '8g')
    sc = SparkContext("local", conf=conf)
    sc.setLogLevel('ERROR')

    # Edge partitioning
    t6 = time.time()
    edge_partitioning_result = edge_partitioning(sc, nodes, edges, num_partition=4)
    t7 = time.time()
    print("Edge_partitioning %s (s)" % str(t7 - t6))
    print("%s edges" % len(edge_partitioning_result))
    save_result("result/edge_partitioning_result.txt", edge_partitioning_result)

    # Vertex partitioning
    t8 = time.time()
    vertex_partitioning_result = vertex_partitioning(sc, nodes, edges, num_partition=4)
    t9 = time.time()
    print("Vertex_partitioning %s (s)" % str(t9 - t8))
    print("%s edges" % len(vertex_partitioning_result))
    save_result("result/vertex_partitioning_result.txt", vertex_partitioning_result)

    
    # Parallel prim
    t10 = time.time()
    parallel_prim_result = parallel_prim(sc, nodes, edges, num_partition=4)
    t11 = time.time()
    print("Parallel_prim %s (s)" % str(t11 - t10))
    print("%s edges" % len(parallel_prim_result))
    save_result("result/parallel_prim_result.txt", parallel_prim_result)
    

if __name__ == '__main__':
    main()