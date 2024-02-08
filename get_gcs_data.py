"""
 _____      _     _____  _____  _____  ______      _
|  __ \    | |   |  __ \/  __ \/  ___| |  _  \    | |
| |  \/ ___| |_  | |  \/| /  \/\ `--.  | | | |__ _| |_ __ _
| | __ / _ \ __| | | __ | |     `--. \ | | | / _` | __/ _` |
| |_\ \  __/ |_  | |_\ \| \__/\/\__/ / | |/ / (_| | || (_| |
 \____/\___|\__|  \____/ \____/\____/  |___/ \__,_|\__\__,_|

Have a lot of data in a Google Cloud Storage (GCS) directory? Want to read it locally? Use this.

CAVEAT: This currently only works with csv file types.

Author: Dan
Email: okeeffed090@gmail.com
github: dokeeffe87

V1.0.0.0

"""

# import packages
import os
import pandas as pd
import dask.dataframe as dd
from tqdm import tqdm
from time import gmtime, strftime


def save_file(df, save_path=None, output_filename=None) -> bool:
    """
    Function to save data read from GCS to a local file. Currently, only supports saving to csv.

    :param df: Input pandas DataFrame that has the read and formatted data from read_from_gcs
    :param save_path: The path to the directory where you want to save your data. This is optional. If None, the data will be stored to the current working directory
    :param output_filename: The name you want to give to your read GCS data. Only supports csv file type currently. If you don't end the file name with the .csv extension,
                            it will be added for you automatically.  If you don't supply a value here, the file will be saved as read_from_gcs_on_{CURRENT_TIME}.csv
    :return: True
    """

    current_time = strftime('%Y-%m-%d_%H%M%S', gmtime())

    if save_path is None:
        save_path = os.getcwd()

    if output_filename is None:
        file_name = 'read_from_gcs_on_{0}.csv'.format(current_time)
    else:
        if not output_filename.endswith('.csv'):
            file_name = output_filename + '.csv'
        else:
            file_name = output_filename

    save_path = os.path.join(save_path, file_name)
    df.to_csv(save_path)

    # Verify that the file exists
    verify_ = os.path.isfile(save_path)
    if not verify_:
        print('Failed to save data to: {0}'.format(save_path))
    else:
        print('Data saved as: {0}'.format(save_path))

    return True


def read_from_gcs(path_to_gcs_data: str, file_type: str = 'csv', save_path: str = None, output_filename: str = None, date_columns: tuple = ()) -> pd.DataFrame:
    """
    Function to read data from Google Cloud Storage (GCS).  Currently, only supports csv files. You can read multiple csv files from the same GCS directory by just
    passing the directory name to path_to_gcs_data, all files will read in as one DataFrame object

    :param path_to_gcs_data: Path in GCS where your csv files are stored. There can be many files in here, all will be read
    :param file_type: Defaults to csv. This the type of file you have in GCS that you want to read. This is the only file type supported currently
    :param save_path: The path to the directory where you want to save your data. This is optional. If None, the data will be stored to the current working directory
    :param output_filename: The name you want to give to your read GCS data. Only supports csv file type currently. If you don't end the file name with the .csv extension,
                            it will be added for you automatically.  If you don't supply a value here, the file will be saved as read_from_gcs_on_{CURRENT_TIME}.csv
    :param date_columns: Optional tuple of column names that contain date/datetime/timestamp data types. These need to be processed separately or else they won't be read c
                         correctly
    :return: A pandas DataFrame with the read data
    """

    # Dask will read datetime objects strangely from GCS. We need to format them as objects at read time
    if date_columns is None:
        date_columns = []
    dtypes_dict = {x: 'object' for x in date_columns}

    if path_to_gcs_data[-1] != '/':
        path_to_gcs_data = path_to_gcs_data + '/*.{0}'.format(file_type)
    else:
        path_to_gcs_data = path_to_gcs_data + '*.{0}'.format(file_type)

    # Read data:
    print("Reading data as: {0}".format(path_to_gcs_data))
    df_dd = dd.read_csv(path_to_gcs_data, dtype=dtypes_dict)
    df = df_dd.compute()

    print("Read {0} rows and {1} columns".format(*[x for x in df.shape]))

    # Format date columns after data has been read:
    print('Formatting date columns if any:')
    time_cols = [x for x in date_columns]

    for col_ in tqdm(time_cols):
        df[col_] = pd.to_datetime(df[col_], infer_datetime_format=True)

    # Save file
    print('Saving read data')
    save_file(df=df, save_path=save_path, output_filename=output_filename)

    return df
