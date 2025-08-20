import pandas as pd
import os
import wget
from Download_data.utils_download import bar_progress


################# Functions to create file and directory names #################

def get_remote_dir_OMNI(date, remote_root_dir, res, typ):

    '''
    Construct the remote URL for downloading OMNI data files.

    Args:
        - date (datetime.date): The date corresponding to the desired OMNI data file.
        - remote_root_dir (str): The root URL of the OMNI data repository (must include a trailing '/').
        - res (str): The time resolution of the data file ('1h', '5min' or '1min')
        - typ (str): The type of OMNI data file (hro or hro2).

    Returns:
        - remote_dir (str): The complete remote URL for downloading the OMNI data file.
    '''

    if res != "1h":
        remote_dir  = remote_root_dir + '%s_%s/%s/' % (typ, res, date.year)
    else:
        remote_dir  = remote_root_dir + 'hourly/' + '%s/' % (date.year)

    return remote_dir


def get_filename_OMNI(date, res, typ):

    '''
    Construct the filename for an OMNI data file based on the specified date, resolution, and type.

    Args:
        - date (datetime.date): The date corresponding to the desired OMNI data file.
        - res (str): The time resolution of the data file ('1h', '5min' or '1min').
        - typ (str): The type of OMNI data file ('hro' or 'hro2').

    Returns:
        - filename (str): The constructed filename for the OMNI data file.
    '''

    if res != "1h":
        filename = "omni_%s_%s_%s_v01.cdf" %(typ, res, date.strftime('%Y%m%d'))
    else:
        filename = "omni2_h0_mrg1hr_%s_v01.cdf" %(date.strftime('%Y%m%d'))
    return filename


def get_local_dir_OMNI(date, local_root_dir, res, typ):

    '''
    Constructs the local directory path for storing OMNI data files, based on
    the specified date, resolution, and file type.

    Args:
        - date (datetime.date): The date for which the local directory path is being constructed.
        - local_root_dir (str): The root directory where OMNI data files will be stored locally.
        - res (str): The time resolution of the data file ('1h', '5min' or '1min').
        - typ (str): The type of OMNI data file ('hro' or 'hro2').

    Returns:
        - local_dir (str): The full local directory path for storing OMNI data files. The path is
          structured as:
            - '<local_root_dir>/hourly/<year>/' (if res='1h')
            - '<local_root_dir>/<typ>/<year>/' (if res='5min' or '1min')
    '''

    if res != "1h":
        local_dir = os.path.join(local_root_dir, '%s' %(typ), '%s' % (date.year), '')

    else:
        local_dir = os.path.join(local_root_dir,'hourly', '%s' % (date.year), '')
    #print('LOCAL DIR', local_dir)

    return local_dir


###################### Functions to download data ##############################

def get_file_OMNI(filename, remote_dir, local_dir):

    '''
    Downloads an OMNI CDF data file from the specified remote directory (URL)
    and saves it to the given to a local directory.

    If the specified local directory does not exist, it will be created. The function checks
    whether the file already exists locally before downloading it. If the file is not found
    in the remote directory, an error message will be displayed.

    Args:
        - filename (str): The name of the file to download.
        - remote_dir (str): The URL of the remote directory containing the file (must include a trailing '/').
        - local_dir (str): The local directory path where the file will be saved (must include a trailing '/').

    Returns:
        - None
    '''

    print(f"Retrieving OMNI {filename} to {local_dir}")
#    print(local_dir)
    # Si el directorio local no existe, lo creamos
    if not os.path.exists(os.path.dirname(local_dir)):
        try:
            os.makedirs(os.path.dirname(local_dir))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    # Si el archivo no existe en el directorio local, lo descargamos
    # si no existe el link, printeamos un mensaje de aviso
    if not os.path.exists(local_dir+filename):
        try:
            wget.download(url=remote_dir+filename, out=local_dir,
                          bar=bar_progress)
            print()
            print(f"File downloaded: {filename}:")
        except Exception as e:
            print(f"File not found {remote_dir+filename}")
            print(e)
    # si el archivo ya existe en el directorio local, printeamos un mensaje avisando.
    else:
        print(f"File already exist: {filename}")
    print()

    return


def download_CDFfiles_OMNI(start_date, end_date, remote_root_dir, local_root_dir, res="1min", type="hro"):

    '''
    Download OMNI CDF data files for a specified date range and configuration.

    This function downloads OMNI data files from a specified remote server and
    saves them to a local directory. It supports customization of data resolution
    and the type of OMNI data file. Additionally, it validates the resolution and
    type combination, invalid options will result in an error message and termination

    Args:
        - start_date (datetime.date): The start date for the range of data to download.
        - end_date (datetime.date): The end date for the range of data to download.
        - remote_root_dir (str): The base URL of the remote server hosting the data files.
        - local_root_dir (str): The root directory on the local machine where files will be saved.
        - res (str, optional): The time resolution of the data files ('1min', '5min'
          or '1h'). Defaults to '1min'.
        - typ (str, optional): The type of OMNI data file ('hro' or 'hro2').
          Defaults to 'hro'.

    Returns:
        - None
    '''

    print('\nPrint para ver si efectivamente se actualiza el paquete Download_data OMNI V3')
    print(f'\nDOWNLOADING OMNI DATA')

    if res == '1min':
        if type == 'hro':
            print("OMNI HRO 1 min resolution")
        elif type == 'hro2':
            print("OMNI HRO2 1 min resolution")
        else:
            print("Please select a valid option for type")
            return
        date_array = pd.date_range(start=start_date, end=end_date, freq='MS')

    elif res == '5min':
        if type == 'hro':
            print("OMNI HRO 5 min resolution")
        elif type == 'hro2':
            print("OMNI HRO2 5 min resolution")
        else:
            print("Please select a valid option for type")
            return
        date_array = pd.date_range(start=start_date, end=end_date, freq='MS')

    elif res == '1h':
        print("OMNI 1 hour resolution")
        date_array = pd.date_range(start=start_date, end=end_date, freq='6MS')

    else:
        print("Please select a valid option for temporal resolution")
        return

    print('---')
    for date in date_array:
        filename = get_filename_OMNI(date, res, type)
        remote_dir = get_remote_dir_OMNI(date, remote_root_dir, res, type)
#        print(remote_dir+filename)
        local_dir = get_local_dir_OMNI(date, local_root_dir, res, type)
        get_file_OMNI(filename, remote_dir, local_dir)

    print('---')
    print("DONE")
    print('---')

    return
