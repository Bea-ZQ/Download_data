import wget
import os
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen, urlretrieve
import ssl
import requests
import fnmatch
import pandas as pd
import glob
import pathlib
import errno
from Download_data.utils_download import bar_progress

"""
Author: Felipe Darmazo
Email: felipe.darmazo@ug.uchile.cl
Adapted from original codes by Victor Pinto
Date: Sept 2025
"""

def read_site_content_EMFISIS(url):

     #Retrieve a list of download links for RBSP EMFISIS data from a given URL.


    remote_files = []
    req = Request(url)
    content = urlopen(req, context = ssl.SSLContext()).read()

    soup = BeautifulSoup(content, 'html.parser')

    existing_links = (soup.find_all('a'))


    for link in existing_links:
        remote_files.append(link.extract().get_text())


    return remote_files


def get_remote_dir_EMFISIS(probe, date, remote_root_dir, level,interval,coordinates):

    '''
    Constructs the remote URL for downloading RBSP EMFISIS data files.

    Args:
        - date (datetime.date): The date of the file to be downloaded.
        - remote_root_dir (str): The root URL for downloading the RBSP emfisis data files (should include trailing '/').
        - probe (str): The probe or satellite identifier ('a' or 'b').
        - interval (str): The time interval between data in seconds (1 or 4).
        - level (str): The data level ('2' or '3').
        -coordinates: the coordinates system for the data(geo,gsm,gei,sm,gse)

    Returns:
        - remote_dir (str): The full remote URL for downloading the RBSP ECT data file.
    '''

    # Así están guardados los dato en la pagina web
    remote_dir = os.path.join(remote_root_dir , 'rbsp{}/l{}/emfisis/magnetometer/{}sec/{}/{}'.format(probe,level,interval,coordinates,date.year))


    return remote_dir



def get_remote_filename_EMFISIS(date, remote_dir):

    '''
    Finds the filename for downloading RBSP EMFISIS data files from the specified directory.

    Args:
        - date (datetime.date): The date of the file to be downloaded.
        - remote_dir (str): The URL for downloading the RBSP ECT data files (must include a trailing '/').
        - probe (str): The probe or satellite identifier ('a' or 'b').
        - level (str): The data level ('2' or '3').

    Returns:
        - filename (str): The name of the data file matching the specified date, to be downloaded.

    Raises:
        - IndexError: If no file matching the date is found in the remote directory.
    '''

    # Obtenemos todos los links de descarga que están en la página web
    files = read_site_content_EMFISIS(remote_dir)

    # Encontramos el link que hace match con la fecha, que es el nombre del archivo a descargar
    filename = fnmatch.filter(files, f"*{date.strftime('%Y%m%d')}*")[0]

    return filename


def get_local_dir_EMFISIS(date, local_root_dir, probe,level):

    '''
    Constructs the local directory path for storing RBSP EMFISIS data files.

    Args:
        - date (datetime.date): The date for which the local directory path is being constructed.
        - local_root_dir (str): The root directory where the RBSP ECT data files will be stored locally.
        - probe (str): The probe or satellite identifier ('a' or 'b').
        - level (str): The data level ('2' or '3').

    Returns:
        - local_dir (str): The full local directory path for storing the RBSP ECT data files,
          structured as:
          '<local_root_dir>/ect/rbsp_<probe>/<instrument>/level<level>/<year>/'.
    '''

    local_dir = os.path.join(local_root_dir, "emfisis", f"rbsp_{probe}", f"level{level}", f"{date.year}", "")
#    print('LOCAL DIR', local_dir)

    return local_dir


def get_file_EMFISIS(filename, remote_dir, local_dir):

    '''
    Downloads an RBSP EMFISIS CDF data file from the specified remote directory (URL)
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

    print(f"Retrieving ECT {filename} to {local_dir}")

    # If the local directory does not exist, create it
    if not os.path.exists(os.path.dirname(local_dir)):
        try:
            os.makedirs(os.path.dirname(local_dir))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

    # Si el archivo no existe en el directorio local, lo descargamos
    # si no existe el link, printeamos un mensaje de aviso
    if not os.path.exists(local_dir+filename):
        try:
#            wget.download(url=remote_dir+filename, out=local_dir,
#                          bar=bar_progress)
            response = requests.get(remote_dir +'/' + filename, verify=False)
            with open(local_dir+filename, 'wb') as file:
                file.write(response.content)
            print()
            print(f"File downloaded: {filename}:")
        except Exception as e:
            print(e)
            print(f"File not found {remote_dir+filename}")

    # si el archivo ya existe en el directorio local, printeamos un mensaje avisando.
    else:
        print(f"File already exist: {filename}")
    print()

    return


def download_CDFfiles_EMFISIS(start_date, end_date, remote_root_dir, local_root_dir, probe, level="3", interval = 4,coordinates = 'geo'):

    '''
    Download RBSP EMFISIS CDF data files for a specified date range and configuration.

    This function retrieves RBSP EMFISIS data files from a remote server and saves them
    to a local directory. It supports downloading data for a specific probe ('a' or 'b')
    or both probes, and allows customization of the data level, instrument, and server.

    Args:
        - start_date (datetime.date): The start date of the range for which to download data.
        - end_date (datetime.date): The end date of the range for which to download data.
        - remote_root_dir (str): The base URL of the remote server hosting the data files.
        - local_root_dir (str): The root directory on the local machine where files will be saved.
        - probe (str): The satellite identifier ('a', 'b', or 'both').
                       If 'both', data for both probes will be downloaded.

    Returns:
        - None
    '''

    print('\nPrint para ver si efectivamente se actualiza el paquete Download_data ECT V3')
    date_array = pd.date_range(start=start_date, end=end_date, freq='D')

    if not probe == 'both':
        print('PROBE ', probe.upper())
        print('---\n')

        # Por cada fecha en el arreglo, generamos el link de descarga y el directorio local
        # donde se van a guardar los datos
        for date in date_array:
            remote_dir = get_remote_dir_EMFISIS(probe, date,remote_root_dir,level,interval,coordinates)
            filename = get_remote_filename_EMFISIS(date, remote_dir)
            local_dir = get_local_dir_EMFISIS(date, local_root_dir, probe, level)

            # Descargamos los datos
            get_file_EMFISIS(filename, remote_dir, local_dir)

    else:
        print('BOTH PROBES')
        print('---\n')
        for date in date_array:
            remote_dirA = get_remote_dir_EMFISIS('a', date,remote_root_dir,level,interval,coordinates)
            filenameA = get_remote_filename_EMFISIS(date, remote_dirA)
            local_dirA = get_local_dir_EMFISIS(date, local_root_dir, 'a',level)
            get_file_EMFISIS(filenameA, remote_dirA, local_dirA)

            remote_dirB = get_remote_dir_EMFISIS('b', date,remote_root_dir,level,interval,coordinates)
            filenameB = get_remote_filename_EMFISIS(date, remote_dirB)
            local_dirB = get_local_dir_EMFISIS(date, local_root_dir, 'b',level)
            get_file_EMFISIS(filenameB, remote_dirB, local_dirB)

    print('---')
    print("DONE")
    print('---')

    return
