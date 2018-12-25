#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of PageRank. For more conventional use,
Please refer to PageRank implementation provided by graphx

Example Usage:
bin/spark-submit examples/src/main/python/pagerank.py data/mllib/pagerank_data.txt 10
"""
from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession
import os

def computeContribs(rank, urls):
    """Calculates URL contributions to the rank of other URLs."""
    for url in urls:
        yield (url, rank)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def parseNeighbors_trans(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[1], parts[0]

def node_cmp(x, y):
    if int(x[0]) < int(y[0]):
        return -1
    return 1



if __name__ == "__main__":
    input_file = sys.argv[1] #"Wiki-Vote.txt"#
    iterations = int(sys.argv[2]) #5 #
    outout_dir = sys.argv[3] #"output" #
    try:
        os.makedirs(outout_dir)
    except OSError:
        if not os.path.isdir(outout_dir):
            raise

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text(input_file).rdd.map(lambda r: r[0])


    # Loads all URLs from input file and initialize their neighbors.
    all_urls = lines.map(lambda urls: parseNeighbors(urls)).distinct()
    links = all_urls.groupByKey().cache()
    links_transpose = lines.map(lambda urls: parseNeighbors_trans(urls)).distinct().groupByKey().cache()
    result_links = links.collect()
    result_transition = links_transpose.collect()
    hubs = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    authority = None
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(iterations):
        # Calculates URL contributions to the rank of other URLs.
        # calculate authority
        authority = links.join(hubs).flatMap(
            lambda x: computeContribs(x[1][1], x[1][0])).reduceByKey(add)

        max_value_a = authority.map(lambda x: x[1]).max()
        authority = authority.mapValues(lambda x: x / max_value_a)

        hubs = links_transpose.join(authority).flatMap(
            lambda x: computeContribs(x[1][1], x[1][0])).reduceByKey(add)  
        max_value_h = hubs.map(lambda x: x[1]).max()
        hubs = hubs.mapValues(lambda x: x / max_value_h)

    # Collects all URL ranks and dump them to console.
    result_hubs = sorted(hubs.collect(), cmp=node_cmp)

    text_file = open(outout_dir + "/hub.txt", "w")
    for _ in result_hubs:
        text_file.write(str(_[0]) + ",%.5f" % _[1] + "\n")
    text_file.close()

    result_authority = sorted(authority.collect(), cmp = node_cmp)

    text_file = open(outout_dir + "/authority.txt", "w")
    for _ in result_authority:
        text_file.write(str( _[0]) + ",%.5f" % _[1] + "\n")
    text_file.close()

    spark.stop()
