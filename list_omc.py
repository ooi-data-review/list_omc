import easywebdav as ez
import tarfile
import os
from OMC import ListOMC, configs
import glob

# define local variables for saving files
local_dir = '/Users/michaesm/Documents/omc_data'

# define remote access variables
url = 'rawdata.oceanobservatories.org'
remote_dir = 'dav/'

if not os.path.exists(local_dir):
    os.makedirs(local_dir)

tar_file = 'filelist.tar.gz'

remote_file = os.path.join(remote_dir, tar_file)
local_file = os.path.join(local_dir, tar_file)

config = configs.dav_configs()

rd = ez.connect(url, **config)
rd.download(remote_file, local_file)

if local_file.endswith("tar.gz"):
    tar = tarfile.open(local_file, "r:gz")
    tar.extractall(local_dir)
    tar.close()
elif local_file.endswith("tar"):
    tar = tarfile.open(local_file, "r:")
    tar.extractall(local_dir)
    tar.close()

tmp_dir = os.path.join(local_dir, 'tmp')
matches = glob.glob(tmp_dir + '/*omc.txt')
fList = ListOMC(matches[0])

fList.load_csv()

# Examples on how to navigate through file listings

# list all files that are newer than a certain date
sub_list = fList.get_files_newer_than('2016-09-29 00:00:00')
ListOMC.to_csv(sub_list, os.path.join(local_dir, 'files_newer_than_09-29-2016.csv'))

# list all files per platform and deployment. leave acquisition method unspecified for both telemetered and recovered
sub_list = fList.get_platform_by_deployment('CE01ISSM', 1, 'recovered')
ListOMC.to_csv(sub_list, os.path.join(local_dir, 'CE01ISSM_R00001_file_list.csv'))

# list all files per platform that are newer than a certain date
sub_list = fList.get_platform_newer_than('CE01ISSM', '2016-09-01 00:00:00')
ListOMC.to_csv(sub_list, os.path.join(local_dir, 'CE01ISSM_newer_than_2016-09-01.csv'))

# list all files on the raw data server that were created between two dates
sub_list = fList.get_files_between('2016-07-01 00:00:00', '2016-08-01 00:00:00')
ListOMC.to_csv(sub_list, os.path.join(local_dir, 'files_between_times.csv'))

