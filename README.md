## Project Overview
Used Google's [Flatbuffers](https://github.com/google/flatbuffers) and Python's [shared memory](https://docs.python.org/3/library/multiprocessing.shared_memory.html) libraries to pass serialized Dataframes between notebook sessions, and perform various operations (`head`, `groupby`, `map`) directly on the serialized Dataframes.

## Project Motivation

A motivating use case for Flatbuffers is illustrated in the `host_nb.ipynb` and `guest_nb.ipynb` notebooks. Suppose you want to pass a dataframe (`climatechange_tweets_all.csv`) from `host_nb.ipynb` to `guest_nb.ipynb`. A standard method would to be use Python's [shared memory](https://docs.python.org/3/library/multiprocessing.shared_memory.html): the shared memory is accessible from any Python process on the same machine, with the limitation that it can only hold raw bytes, that is, the dataframe will need to be serialized before it can be placed in shared memory. 

Therefore, to pass the dataframe, the `host_nb` will serialize the dataframe with `Pickle`, Python's de-facto serialization protocol, put the serialized bytes into shared memory, then `guest_nb` will deserialize the bytes back (again using `Pickle`) into a dataframe to perform the desired operations (e.g., `head`). Below is an illustration of this process: To experience this process yourself, run the first 3 cells in `host_nb` followed by the first 3 cells in `guest_nb`.

![Screenshot 2024-03-25 at 3 00 45 PM](https://github.com/illinoisdata/CS598-MP1-OLA/assets/31910858/09beb749-4bf9-4f3f-8c94-dfbae3914619)

The problem with message passing via serialization/deserialization with `Pickle` is that in `guest_nb`, the dataframe must be entirely deserialized first before any operations can be performed, even if the operation only accesses a small part of the dataframe (e.g., `head` accessing first 5 rows). This is significant time and space overhead - the user will have to wait for the deserialization to complete, while their namespace will also contain a copy of the dataframe from `host_nb`.

This is where Flatbuffers comes in. A main selling point of Flatbuffers is that **individual fields can be accessed without deserializing the Flatbuffer**; that is, if the dataframe was serialized as a Flatbuffer in `host_nb`, the head of the dataframe can be computed in `guest_nb` by only reading the relevant rows - no deserialization of the entire dataframe needed:

![image](https://github.com/illinoisdata/CS598-MP1-OLA/assets/31910858/5c56a4fb-55a5-4424-9c13-7790f3a34322)

## Tasks

Tasks are as follows:
- Writing the Flatbuffers definition file for Dataframes and code for serializing Dataframes into Flatbuffers (30%)
- Performing the `head()` operation directly on a Flatbuffer-serialized dataframe (10%)
- Performing the `sum(x) group by y` operation directly on a Flatbuffer-serialized dataframe (10%)
- Modifying a column in Flatbuffer-serialized dataframe in-place via `map` (30%)
- Integrating your Flatbuffer functions with shared memory (20%)

## Serializing Dataframes into Flatbuffers

The Flatbuffer definition follows a *columnar layout* as illustrated below:
```
+-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
| DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
+-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
```

## Performing the HEAD Operation on Flatbuffer-Serialized Dataframes

## Performing Grouped Aggregation on Flatbuffer-serialized Dataframes

## Modifying Flatbuffer-serialized Dataframes In-place via map
