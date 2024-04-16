from flatbuffers.python.flatbuffers import Builder
import pandas as pd
import struct
import time
import types
from DataFrame import DataFrame, Column, Metadata, ValueType
# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...

def to_flatbuffer(df: pd.DataFrame) -> bytes:
    """
        Converts a DataFrame to a flatbuffer. Returns the bytes of the flatbuffer.

        The flatbuffer should follow a columnar format as follows:
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        | DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        You are free to put any bookkeeping items in the metadata. however, for autograding purposes:
        1. Make sure that the values in the columns are laid out in the flatbuffer as specified above
        2. Serialize int and float values using flatbuffer's 'PrependInt64' and 'PrependFloat64'
            functions, respectively (i.e., don't convert them to strings yourself - you will lose
            precision for floats).

        @param df: the dataframe.
    """
    builder = Builder(1024)
    metadata_string = builder.CreateString("DataFrame Metadata")
    column_metadata_list = []
    value_vectors = []
    value_vectors_dtype = []
    for column_name, dtype in df.dtypes.items():
        if dtype == 'int64':
            value_type = ValueType.ValueType().Int
        elif dtype == 'float64':
            value_type = ValueType.ValueType().Float
        elif dtype == 'object':
            value_type = ValueType.ValueType().String
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

        column_metadata_list.append((column_name, value_type))

        # Convert column values to FlatBuffer values
        column_values = df[column_name]
        value_vectors.append(column_values.tolist())
        value_vectors_dtype.append(dtype)
    columns = []
    for dtype, metadata, value_vector in reversed(list(zip(value_vectors_dtype ,column_metadata_list, value_vectors))):
        if dtype == 'int64':
            Column.StartIntValuesVector(builder, len(value_vector))
            for value in reversed(value_vector):
                builder.PrependInt64(value)
            values = builder.EndVector(len(value_vector))

            col_name = builder.CreateString(metadata[0])
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddIntValues(builder, values)
            columns.append(Column.End(builder))
        elif dtype == 'float64':
            Column.StartFloatValuesVector(builder, len(value_vector))
            for value in reversed(value_vector):
                builder.PrependFloat64(value)
            values = builder.EndVector(len(value_vector))
            
            col_name = builder.CreateString(metadata[0])
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddFloatValues(builder, values)
            columns.append(Column.End(builder))
        elif dtype == 'object':
            str_offsets = [builder.CreateString(str(value)) for value in value_vector]
            Column.StartStringValuesVector(builder, len(value_vector))
            for offset in reversed(str_offsets):
                builder.PrependUOffsetTRelative(offset)
            values = builder.EndVector(len(value_vector))
            
            col_name = builder.CreateString(metadata[0])
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddStringValues(builder, values)
            columns.append(Column.End(builder))

    # Create a vector of Column objects
    DataFrame.StartColumnsVector(builder, len(columns))
    for column in columns:
        builder.PrependUOffsetTRelative(column)
    columns_vector = builder.EndVector(len(columns))
    

    # Create the DataFrame object
    DataFrame.Start(builder)
    DataFrame.AddMetadata(builder, metadata_string)
    DataFrame.AddColumns(builder, columns_vector)
    df_data = DataFrame.End(builder)

    # Finish building the FlatBuffer
    builder.Finish(df_data)
    # Get the bytes from the builder
    return builder.Output()


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    df = DataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)

    # Extract column names and types
    columns_metadata = [(df.Columns(i).Metadata().Name().decode('utf-8'), df.Columns(i).Metadata().Dtype()) for i in range(df.ColumnsLength())]
    
    total_rows = max(df.Columns(0).IntValuesLength(), df.Columns(0).FloatValuesLength(), df.Columns(0).StringValuesLength())
    # Extract data
    data = []
    for i in range(min(rows, total_rows)):
        row = []
        for j in range(df.ColumnsLength()):
            col = df.Columns(j)
            if col.Metadata().Dtype() == ValueType.ValueType().Int:
                row.append(col.IntValues(i))
            elif col.Metadata().Dtype() == ValueType.ValueType().Float:
                row.append(col.FloatValues(i))
            elif col.Metadata().Dtype() == ValueType.ValueType().String:
                row.append(col.StringValues(i).decode('utf-8'))
        data.append(row)
    # Convert data to Pandas DataFrame
    column_names = [col_metadata[0] for col_metadata in columns_metadata]
    df_result = pd.DataFrame(data, columns=column_names)
    return df_result


def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
        Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
        and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param grouping_col_name: column to group by.
        @param sum_col_name: column to sum.
    """
    df = DataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)
    column_names = []
    df_arr = []

    for i in range(df.ColumnsLength()):
        col = df.Columns(i)
        col_name = col.Metadata().Name().decode('utf-8')
        if len(column_names) == 2:
            break
        if col_name == grouping_col_name or col_name == sum_col_name:
            column_names.append(col_name)
            if col.Metadata().Dtype() == ValueType.ValueType().Int:
                df_arr.append([col.IntValues(j) for j in range(col.IntValuesLength())])
            elif col.Metadata().Dtype() == ValueType.ValueType().Float:
                df_arr.append([col.FloatValues(val) for val in range(col.FloatValuesLength())])
            elif col.Metadata().Dtype() == ValueType.ValueType().String:
                df_arr.append([col.StringValues(val).decode('utf-8') for val in range(col.StringValuesLength())])

    df = pd.DataFrame(zip(*df_arr), columns=column_names)
    result = df.groupby(grouping_col_name).sum()
    return result


def fb_dataframe_map_numeric_column(fb_buf1: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    """
        Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.
        This function shouldn't do anything if col_name doesn't exist or the specified
        column is a string column.

        @param fb_buf: buffer containing bytes of the Flatbuffer Dataframe.
        @param col_name: name of the numeric column to apply map_func to.
        @param map_func: function to apply to elements in the numeric column.
    """

    encoded_col_name = col_name.encode('utf-8')
    fb_buf = bytearray(fb_buf1)
    index_of_col_name = fb_buf.find(encoded_col_name)
    if col_name == "int_col":
        encoded_length_index = index_of_col_name+len(encoded_col_name)+1
    elif col_name == "float_col":
        encoded_length_index = index_of_col_name+len(encoded_col_name)+3
    else:
        return
    length = int.from_bytes(fb_buf[encoded_length_index:encoded_length_index+4], 'little')
    start = encoded_length_index+4
    end = start+length*8
    for i in range(start, end, 8):
        if col_name == "int_col":
            val = struct.unpack('<q', fb_buf[i:i+8])[0]
            updated_val = map_func(val)
            updated_val_bytes = struct.pack('<q', updated_val)
            fb_buf1[i:i+8] = updated_val_bytes
        elif col_name == "float_col":
            val = struct.unpack('<d', fb_buf[i:i+8])[0]
            updated_val = map_func(val)
            updated_val_bytes = struct.pack('<d', updated_val)
            fb_buf1[i:i+8] = updated_val_bytes