# MapReduce-Framework-Simulation-Project

## Introduction
This repository contains the implementation of a basic MapReduce framework simulation as part of the Operating Systems course project. The project demonstrates key operating system concepts, including concurrency, synchronization, and parallel processing, through a single-machine simulation of the MapReduce algorithm.

MapReduce is a programming model designed for processing and generating large datasets in a distributed and parallel manner. This project focuses on breaking down a computational task into three main phases—Map, Shuffle, and Reduce—and simulating their functionalities using fundamental operating system principles.

## Project Overview
### Map Phase:
      Processes input data in parallel, splits it into smaller chunks, and outputs intermediate key-value pairs.
      
### Shuffle Phase:
      Groups the intermediate key-value pairs by their keys, ensuring all occurrences of a particular key are together.

### Reduce Phase:
      Aggregates and computes final results for each key from the grouped data.

## Example Workflow
### Input:
"pizza burger pasta pasta pizza"

### After Shuffle:
("pizza", [1, 1]), ("burger", [1]), ("pasta", [1, 1])

### Output:
("pizza", 2), ("burger", 1), ("pasta", 2)

## Features
1. Simulates parallel processing by handling input data in smaller chunks.
2. Implements synchronization and thread management to ensure efficiency and correctness in concurrent computation.
3. Provides a scalable framework for learning and experimenting with distributed data processing concepts.

### Deliverables
1. A complete C/C++ program implementing the MapReduce framework.
2. A flow diagram or activity diagram illustrating the approach, along with a detailed explanation of the code.
3. Test cases showcasing the functionality of the framework.
4. 
This project aims to provide a hands-on experience with core operating system principles and offers a simplified perspective on how MapReduce operates in distributed systems. It emphasizes understanding over completion, encouraging students to engage deeply with the problem-solving process.
