import argparse
import tempfile
from builtins import int

import numpy as np
import pandas as pd
import time


def get_col_delimiter(item):
    """
    This function takes in a list of column delimiters and returns a string of those delimiters
    :param item: list of column delimiters
    :return: string of column delimiters
    """
    item = list(item)
    return ''.join([f'\{i}' for i in item])


def get_default_header(size):
    """
    This function returns a list of default headers for the dataframe
    :param size: number of columns in the dataframe
    :return: list of default headers
    """
    headers = []
    for i in range(1, size + 1):
        headers.append('column' + str(i))
    return headers


def process_header_in_df(df, headers):
    """
    Add comment here
    :param df:
    :param headers:
    :return:
    """
    df.replace(np.nan, '', inplace=True)
    if headers == 'in_file':
        df.columns = df.iloc[0]
        df = df[1:]
    elif headers == 'default' or headers is None:
        df.columns = get_default_header(df.shape[1])
    elif type(headers) == list:
        df.columns = headers
    return df


def get_start_of_data(temp, encoding):
    """
    This function filters the file for START-OF-DATA comment and gets the fields/data
    :param temp:
    :param encoding:
    :return: number of lines to skip before parsing the data

    """
    skip = -1
    while True:
        skip = skip + 1
        line = temp.readline()
        if line.startswith(bytes('START-OF-DATA', encoding)):
            temp.seek(0)
            return skip + 1
        if not line:
            break
    temp.seek(0)
    return -1


def delimiter_file_parse():
    parser = argparse.ArgumentParser(description='Parse delimiter-separated file')
    parser.add_argument('input_path', type=str, help='Path to input file')
    parser.add_argument('out_dir', type=str, help='Directory for output file')
    parser.add_argument('-headers', type=str, default='default', help='Headers for the dataframe')
    parser.add_argument('-row_delimiter', type=str, default='', help='Row delimiter')
    parser.add_argument('-column_delimiter', type=str, default='', help='Column delimiter')
    parser.add_argument('-skip_lines', type=int, default=0, help='Number of lines to skip')
    parser.add_argument('-skip_footers', type=int, default=0, help='Number of footer lines to skip')
    args = parser.parse_args()

    start_time = time.time()
    out_name = args.input_path.rsplit('/', 1)[-1].split('.')[0]
    try:
        with open(args.input_path, 'r') as file:
            data = file.read()
            data = data.replace(args.row_delimiter, '\n')

        """Add comment here"""
        temp = tempfile.TemporaryFile()
        temp.write(bytes(data, file.encoding))
        temp.seek(0)

        """Add comment here"""
        skip = get_start_of_data(temp, file.encoding)
        skip_lines = skip if skip != -1 else args.skip_lines

        """Add comment here"""
        col_del = get_col_delimiter(args.column_delimiter) if len(args.column_delimiter) > 1 else args.column_delimiter
        df = pd.read_csv(temp, header=None, sep=col_del, engine='python', skiprows=args.skip_lines, encoding=file.encoding,
                         skipfooter=args.skip_footers)
        temp.close()

        """Add comment here"""
        df = process_header_in_df(df, args.headers)
        print("--- %s seconds ---" % (time.time() - start_time))
        print(df)
        #df.to_parquet(args.out_dir + out_name + '.parquet', engine='fastparquet')
    except Exception as e:
        print(f'Exception occurred as {e}')
    finally:
        file.close()
    return 'Parsing complete'



