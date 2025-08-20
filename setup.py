from setuptools import setup, find_packages

VERSION = '2.0'
DESCRIPTION = 'Download data package'
LONG_DESCRIPTION = 'Package to download data from ECT RBSP and OMNI. It can now download MagEIS data'



# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="Download_data",
        version=VERSION,
        author="BZQ",
        author_email="<beatriz.zenteno@ug.uchile.cl>",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[], # add any additional packages that
        # needs to be installed along with your package. Eg: 'caer'
)
