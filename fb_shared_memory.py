import dill
import hashlib
import pandas as pd
import types
import os
from flatbuffers.python.flatbuffers import Builder
from multiprocessing import shared_memory

from fb_dataframe import to_flatbuffer, fb_dataframe_head, fb_dataframe_group_by_sum, fb_dataframe_map_numeric_column


class FbSharedMemory:
    """
        Class for managing the shared memory for holding flatbuffer dataframes.
    """
    def __init__(self):
        self.df_shared_memory = None
        self.offset_file_path = "./shared_mem_log.txt"
        self.df_offsets = {}
        self.offset = 0

        try:
            self.df_shared_memory = shared_memory.SharedMemory(name="CS598")
            self._load_offsets()
        except FileNotFoundError:
            # Shared memory is not created yet, create it with size 200M.
            self.df_shared_memory = shared_memory.SharedMemory(name="CS598", create=True, size=200000000)

    def _load_offsets(self):
        if os.path.exists(self.offset_file_path):
            with open(self.offset_file_path, 'rb') as f:
                self.df_offsets = dill.load(f)

    def _save_offsets(self):
        with open(self.offset_file_path, 'wb') as f:
            dill.dump(self.df_offsets, f)

    def add_dataframe(self, name: str, df: pd.DataFrame) -> None:
        """
        Adds a dataframe into the shared memory. Does nothing if a dataframe with 'name' already exists.

        @param name: name of the dataframe.
        @param df: the dataframe to add to shared memory.
        """
        if name in self.df_offsets:
            print(f"DataFrame '{name}' already exists in shared memory.")
            return

        # Convert DataFrame to FlatBuffer
        
        fb_buf = to_flatbuffer(df)
        self.df_shared_memory.buf[self.offset:self.offset+len(fb_buf)] = fb_buf
        self.df_offsets[name] = self.offset
        self.offset += len(bytearray(fb_buf))
        self._save_offsets()

    def _get_fb_buf(self, df_name: str) -> memoryview:
        """
        Returns the section of the buffer corresponding to the dataframe with df_name.

        @param df_name: name of the DataFrame.
        """
        if df_name not in self.df_offsets:
            raise ValueError(f"DataFrame '{df_name}' not found.")

        offset = self.df_offsets[df_name]
        return self.df_shared_memory.buf[offset:]


    def dataframe_head(self, df_name: str, rows: int = 5) -> pd.DataFrame:
        """
            Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
            similar to df.head(). If there are less than n rows, returns the entire Dataframe.

            @param df_name: name of the Dataframe.
            @param rows: number of rows to return.
        """
        fb_bytes = bytes(self._get_fb_buf(df_name))
        return fb_dataframe_head(fb_bytes, rows)

    def dataframe_group_by_sum(self, df_name: str, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
        """
            Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
            and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.
    
            @param df_name: name of the Dataframe.
            @param grouping_col_name: column to group by.
            @param sum_col_name: column to sum.
        """
        fb_bytes = bytes(self._get_fb_buf(df_name))
        return fb_dataframe_group_by_sum(fb_bytes, grouping_col_name, sum_col_name)

    def dataframe_map_numeric_column(self, df_name: str, col_name: str, map_func: types.FunctionType) -> None:
        """
            Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.

            @param df_name: name of the Dataframe.
            @param col_name: name of the numeric column to apply map_func to.
            @param map_func: function to apply to elements in the numeric column.
        """
        fb_dataframe_map_numeric_column(self._get_fb_buf(df_name), col_name, map_func)


    def close(self) -> None:
        """
            Closes the managed shared memory.
        """
        try:
            self.df_shared_memory.close()
        except:
            pass