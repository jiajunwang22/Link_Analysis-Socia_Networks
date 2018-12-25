# Link_Analysis-Socia_Networks
part of Data Mining HW

This file includes two parts:

1. Link Analysis - HITS

    Implemented HITS algorithm in spark: hits.py
    using parallel implementation logic from pagerank.py to calculate the hub and authority on the given
    input graph in Wikipedia vote network data - Wiki-Vote.txt. Data is taken from
    http://snap.stanford.edu/data/wiki-Vote.html. The network contains all the Wikipedia voting data from
    the inception of Wikipedia till January 2008. Nodes in the network represent Wikipedia users and a
    directed edge from node i to node j represents that user i voted on user j.

2. Social Networks - spectral clustering

    Implemented spectral clustering algorithm, where a graph is
    continuously partitioned to get a desired number of clusters in Python –
    FirstName_LastName_spectral.py using graph library – networkx. Read about networkx here -
    https://networkx.github.io/documentation/stable/index.html# about installation, how to use etc. With
    networkx, you can:
        1. Construct a graph from edges
        2. Compute degree of each node that you would require for Degree Matrix
        3. Modify the graph etc.
    You would also need library function to calculate Eigenvalue / Eigenvector of Laplacian Matrix, for which
    you can use numpy.
  
  Spectral Algorithm works in 3 steps: For user specified number of clusters – k (Minimum 2)

    1. Construct Laplacian Matrix L for the given graph
    2. Find Eigenvalues and Eigenvectors for the Matrix L using numpy. Pick the second smallest Eigenvalue and corresponding Eigenvector. Split the Eigenvector at 0 (Each element mapped to each node) into 2 clusters – i.e. place positive elements and negative elements (mapped vertices) into two clusters.
    3. Based on k, repeat steps 1 and 2. The way you pick next cluster to partition is based on which among already identified clusters have more nodes. You disconnect edges that connect nodes from one cluster to another, and build Laplacian Matrix again.

