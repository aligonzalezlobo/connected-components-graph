# Finding Connected Components in a Graph

**Authors**: Vincenzo Albano, Alvaro Calafell, Alicia Gonzalez

## Introduction

Finding connected components in a graph is a fundamental problem in graph theory with applications in various domains such as social networks, web analysis, and bioinformatics. This project explores the Connected Components Framework (CCF) algorithm for finding connected components in a graph using MapReduce. We implement the CCF algorithm using the Resilient Distributed Datasets (RDD) API in both Python and Scala, as well as the DataFrames API. Additionally, an experimental analysis is conducted to compare the scalability of the algorithm using RDD and DataFrames.

## Background

### Description of Graph Algorithms

Graph algorithms are essential techniques used to analyze and manipulate graphs, which consist of vertices/nodes and edges/links connecting them. These algorithms have diverse applications, including finding shortest paths, identifying connected components, and clustering vertices based on similarity.

### MapReduce and Its Significance

MapReduce is a widely used programming model for processing large-scale datasets. It involves two primary functions: Map and Reduce. Map takes input data and converts it into key-value pairs, while Reduce combines data with the same key to produce the final result. MapReduce is known for its scalability and fault tolerance, making it suitable for distributed computing environments.

### CCF Algorithm and Its Implementation in MapReduce

The CCF (Connected Components Framework) algorithm is an iterative graph processing algorithm used to find connected components. It leverages the MapReduce model for distributed processing and consists of two main steps: CCF-Iterate and CCF-Dedup.

#### CCF-Iterate

This step processes input graph data and generates key-value pairs. The key is the vertex ID, and the value is either the vertex's component ID or a list of its neighbors. The Map function emits key-value pairs, and the Reduce function identifies connected components by merging component IDs of neighboring vertices.

#### CCF-Dedup

After CCF-Iterate, duplicate component IDs are removed, and vertex component IDs are updated. This ensures that each connected component has a unique component ID.

The CCF algorithm continues iterating between these steps until no more changes occur in vertex component IDs.

## Implementation

The project implements the CCF algorithm using four different codes:

1. Python Spark version using RDDs (Code 1 in Appendix)
2. Python Spark version using DataFrames (Code 2 in Appendix)
3. Scala version using RDDs (Code 3 in Appendix)
4. Scala version using DataFrames (Code 4 in Appendix)

The code for each version can be found in the Appendix of this report. Each notebook includes small-size graph examples, allowing you to check if the code works correctly in different scenarios. These notebooks serve as a starting point for understanding the codebase and its functionality.

## Experimental Analysis


### Google Cloud Integration

The core of this project involves deploying the code on Google Cloud and analyzing how computing times change in different scenarios. The project explores the scalability and performance of the CCF algorithm when running on a Google Cloud Dataproc cluster with large-scale datasets.

### RDDs and DataFrames

RDDs (Resilient Distributed Datasets) and DataFrames are both abstractions in Apache Spark for distributed data processing. RDDs offer flexibility and control, while DataFrames provide optimization and schema features.

**Advantages of RDDs:**

- Flexibility: RDDs can store any data type and support custom operations.
- Control: RDDs offer fine-grained control over data processing.
- Low-level API: RDDs allow advanced use cases.

**Advantages of DataFrames:**

- Optimization: DataFrames are optimized for performance.
- Schema: DataFrames have metadata for easier data manipulation.
- Interoperability: DataFrames can work with various data sources.

In general, RDDs are suitable for complex, custom data processing, while DataFrames excel in standard operations. The choice between them depends on specific project requirements.

### Datasets Used

The project generated graphs of varying sizes to test algorithm performance. The sizes used are as follows:

| Nodes       | Edges        |
| ----------- | ------------ |
| 100         | 70           |
| 1,000       | 700          |
| 10,000      | 7,000        |
| 100,000     | 70,000       |
| 1,000,000   | 700,000      |
| 10,000,000  | 7,000,000    |

The graphs were generated using the SNAP library in Python to ensure a wide range of test cases.

### Results and Additional Information

For detailed results, more information about the experimental setup, or to explore the slightly modified code used for Google Cloud integration, please feel free to contact us. We are open to collaboration, discussions, and providing further insights into our research. Your feedback and inquiries are welcome.

We hope you find this repository valuable for your graph analysis and distributed computing projects. Enjoy exploring and experimenting with the code!
