import numpy as np
from random import randint
from DisjointSet import DisjointSet


def kruskal(nodes, edges):
    """
    implementation of kruskal's algorithm

    :param nodes: nodes for input.
    :param edges: edges for input.
    :return: edges of the minimum spanning tree.
    """
    # edges of the minimum spanning tree
    mst = []

    # initialize a forest for all nodes
    forest = DisjointSet(nodes)
    # sort the edges by their weights
    edges = sorted(edges, key=lambda x: x[2])
    # calculate the number of edges of the minimum spanning tree
    num_edges = len(nodes)

    # perform kruskal's algorithm
    for (src, dst, weight) in edges:
        # continue if source node or destination node doesn't exist
        if src not in nodes or dst not in nodes:
            continue
        # find the parents of src and dst respectively
        if forest.unite(src, dst):
            # add the current edge into the minimum spanning tree if it doesn't make a circuit
            mst.append((src, dst, weight))
            # terminate early
            if len(mst) == num_edges:
                break

    # return the minimum spanning tree
    return mst


def prim(nodes, edges, pivot=None):
    """
    implementation of prim's algorithm

    :param nodes: nodes for input.
    :param edges: edges for input.
    :param pivot: pivot for prim's algorithm.
    :return: edges of the minimum spanning tree.
    """
    # edges of the minimum spanning tree
    mst = []

    # retrieve the number of nodes
    num_nodes = len(nodes)
    # select a pivot for prim's algorithm randomly
    if pivot is None:
        pivot = randint(0, num_nodes - 1)

    # build a mapping from nodes to corresponding indices in graph matrix
    ref = dict(zip(nodes, range(num_nodes)))
    # calculate graph matrix for prim's algorithm
    graph = np.ones(shape=(num_nodes, num_nodes)) * np.inf
    for (src, dst, weight) in edges:
        # continue if source node or destination node doesn't exist
        if src not in nodes or dst not in nodes:
            continue
        src, dst = ref[src], ref[dst]
        graph[src, dst] = graph[dst, src] = weight

    # build a mapping from corresponding indices in graph matrix to nodes
    ref = dict(zip(range(num_nodes), nodes))
    # record the nearest neighbours of the current subset of vertices
    adjacent = np.ones(shape=(num_nodes, )) * pivot
    # record the lowest costs to different nodes
    cost = graph[pivot]
    cost[pivot] = -np.inf

    # perform prim's algorithm
    for i in range(num_nodes):
        # index of the nearest neighbour and the cost to it
        index = -1
        minimum = np.inf
        # find the nearest neighbour
        for j in range(num_nodes):
            if -np.inf < cost[j] < minimum:
                index, minimum = j, cost[j]
        # prevent making a circuit
        if index != -1:
            # add the nearest neighbour into the current subset of vertices
            cost[index] = -np.inf
            # add the current edge into the minimum spanning tree
            if ref[adjacent[index]] < ref[index]:
                mst.append((ref[adjacent[index]], ref[index], int(minimum)))
            else:
                mst.append((ref[index], ref[adjacent[index]], int(minimum)))
            # update the nearest neighbours of the current subset of vertices
            adjacent = np.where(graph[index] < cost, index, adjacent)
            # update the lowest costs to different nodes
            cost = np.where(graph[index] < cost, graph[index], cost)

    # return the minimum spanning tree
    return mst
