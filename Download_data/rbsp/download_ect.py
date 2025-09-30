import wget
import os
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen, urlretrieve
import ssl
import requests
import fnmatch
import pandas as pd
import glob
from Download_data.utils_download import bar_progress


"""
Author: BZQ
Email: beatriz.zenteno@usach.cl
Adapted from original codes by Victor Pinto
Date: Dic 2024
"""

########## Functions to create locar and remote directoris + filenames #########

def read_site_content_ECT(url):

    '''
    Retrieve a list of download links for RBSP ECT data from a given URL.

    This function takes a URL as input and retrieves the HTML content of the
    corresponding web page. It then uses BeautifulSoup's HTML parser to extract
    all the links (URLs) present on the page that are relevant for downloading
    satellite data files. The extracted links are returned as a list.

    Args:
        - url (str): The URL of the web page to scrape for download links.

    Returns:
        - remote_files (list): A list of strings representing the URLs of the download links
            found on the web page.
    '''

    remote_files = []
    req = Request(url)

    # Este objeto tiene todo el contenido de la página
    content = urlopen(req, context = ssl.SSLContext()).read()

    #HTML parsing
    #Link útil para entender qué está sucediendo: https://stackabuse.com/guide-to-parsing-html-with-beautifulsoup-in-python/
    soup = BeautifulSoup(content, 'html.parser')

    # Aquí obtenemos todos los links que se encuentran en la página, todavía en formato HTML
    # El tag 'a' de HTML define un hyperlink
    existing_links = (soup.find_all('a'))

    # Con esto obtenemos los links, en formato string
    for link in existing_links:
        remote_files.append(link.extract().get_text())

    #Retornamos una lista con todos los links de la página, en formato string,
    #que al final son los nombres de los archivos a descargar
    return remote_files


def get_remote_dir_ECT(date, remote_root_dir, probe, instrument, level):

    '''
    Constructs the remote URL for downloading RBSP ECT data files.

    Args:
        - date (datetime.date): The date of the file to be downloaded.
        - remote_root_dir (str): The root URL for downloading the RBSP ECT data files (should include trailing '/').
        - probe (str): The probe or satellite identifier ('a' or 'b').
        - instrument (str): The name of the instrument from the ECT suite ('rept' or 'mageis').
        - level (str): The data level ('2' or '3').

    Returns:
        - remote_dir (str): The full remote URL for downloading the RBSP ECT data file.
    '''

    # Así están guardados los dato en la pagina web
    remote_dir = remote_root_dir+ 'rbsp%s/%s/level%s/pitchangle/%s/' % (probe, instrument, level, date.year)

    return remote_dir


def get_remote_filename_ECT(date, remote_dir, probe, instrument, level):

    '''
    Finds the filename for downloading RBSP ECT data files from the specified directory.

    Args:
        - date (datetime.date): The date of the file to be downloaded.
        - remote_dir (str): The URL for downloading the RBSP ECT data files (must include a trailing '/').
        - probe (str): The probe or satellite identifier ('a' or 'b').
        - instrument (str): The name of the instrument from the ECT suite ('rept' or 'mageis').
        - level (str): The data level ('2' or '3').

    Returns:
        - filename (str): The name of the data file matching the specified date, to be downloaded.

    Raises:
        - IndexError: If no file matching the date is found in the remote directory.
    '''

    # Obtenemos todos los links de descarga que están en la página web
    files = read_site_content_ECT(remote_dir)

    # Encontramos el link que hace match con la fecha, que es el nombre del archivo a descargar
#    print(fnmatch.filter(files, f"*{date.strftime('%Y%m%d')}*"))
    try:
        filename = fnmatch.filter(files, f"*{date.strftime('%Y%m%d')}*")[0]
    except:
        print('No file in remote')
        filename = 0
#    filename = fnmatch.filter(files, f"*{date.strftime('%Y%m%d')}*")

    return filename


def get_local_dir_ECT(date, local_root_dir, probe, instrument, level):

    '''
    Constructs the local directory path for storing RBSP ECT data files.

    Args:
        - date (datetime.date): The date for which the local directory path is being constructed.
        - local_root_dir (str): The root directory where the RBSP ECT data files will be stored locally.
        - probe (str): The probe or satellite identifier ('a' or 'b').
        - instrument (str): The name of the instrument from the ECT suite ('rept' or 'mageis').
        - level (str): The data level ('2' or '3').

    Returns:
        - local_dir (str): The full local directory path for storing the RBSP ECT data files,
          structured as:
          '<local_root_dir>/ect/rbsp_<probe>/<instrument>/level<level>/<year>/'.
    '''

    local_dir = os.path.join(local_root_dir, "ect", f"rbsp_{probe}", instrument, f"level{level}", f"{date.year}", "")
#    print('LOCAL DIR', local_dir)

    return local_dir


###################### Functions to download data ##############################

def get_file_ECT(filename, remote_dir, local_dir):

    '''
    Downloads an RBSP ECT CDF data file from the specified remote directory (URL)
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

    if filename ==0:
        print('No file in remote')
    else:
        # If the local directory does not exist, create it
        if not os.path.exists(os.path.dirname(local_dir)):
            try:
                os.makedirs(os.path.dirname(local_dir))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

        # Si el archivo no existe en el directorio local, lo descargamos
        # si no existe el link, printeamos un mensaje de aviso
        #AQUÍ TIENE QUE IR UN IF FILENAME == 0
        if not os.path.exists(local_dir+filename):
            try:
    #            wget.download(url=remote_dir+filename, out=local_dir,
    #                          bar=bar_progress)
                response = requests.get(remote_dir+filename, verify=False)
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


def download_CDFfiles_ECT(start_date, end_date, remote_root_dir, local_root_dir, probe, instrument, level="3", server='nm'):

    '''
    Download RBSP ECT CDF data files for a specified date range and configuration.

    This function retrieves RBSP ECT data files from a remote server and saves them
    to a local directory. It supports downloading data for a specific probe ('a' or 'b')
    or both probes, and allows customization of the data level, instrument, and server.

    Args:
        - start_date (datetime.date): The start date of the range for which to download data.
        - end_date (datetime.date): The end date of the range for which to download data.
        - remote_root_dir (str): The base URL of the remote server hosting the data files.
        - local_root_dir (str): The root directory on the local machine where files will be saved.
        - probe (str): The satellite identifier ('a', 'b', or 'both').
                       If 'both', data for both probes will be downloaded.
        - instrument (str): The instrument name for which to download data ('rept' o 'mageis')
        - level (str, optional): The data level to download ('2' or '3'). Defaults to '3'.
        - server (str, optional): The server identifier to use for downloading (e.g., 'nm'). Defaults to 'nm'.

    Returns:
        - None
    '''

#    print('\nPrint para ver si efectivamente se actualiza el paquete Download_data ECT V3')
    print(f'\nDOWNLOADING ECT-{instrument.upper()} INSTRUMENT DATA')
    date_array = pd.date_range(start=start_date, end=end_date, freq='D')

    if not probe == 'both':
        print('PROBE ', probe.upper())
        print('---\n')

        # Por cada fecha en el arreglo, generamos el link de descarga y el directorio local
        # donde se van a guardar los datos
        for date in date_array:
            remote_dir = get_remote_dir_ECT(date, remote_root_dir, probe, instrument, level)
            filename = get_remote_filename_ECT(date, remote_dir, probe, instrument, level)
            local_dir = get_local_dir_ECT(date, local_root_dir, probe, instrument, level)

            # Descargamos los datos
            get_file_ECT(filename, remote_dir, local_dir)

    else:
        print('BOTH PROBES')
        print('---\n')
        for date in date_array:
            remote_dirA = get_remote_dir_ECT(date, remote_root_dir, 'a', instrument, level)
            filenameA = get_remote_filename_ECT(date, remote_dirA, 'a', instrument, level)
            local_dirA = get_local_dir_ECT(date, local_root_dir, 'a', instrument, level)
            get_file_ECT(filenameA, remote_dirA, local_dirA)

            remote_dirB = get_remote_dir_ECT(date, remote_root_dir, 'b', instrument, level)
            filenameB = get_remote_filename_ECT(date, remote_dirB, 'b', instrument, level)
            local_dirB = get_local_dir_ECT(date, local_root_dir, 'b', instrument, level)
            get_file_ECT(filenameB, remote_dirB, local_dirB)

    print('---')
    print("DONE")
    print('---')

    return
