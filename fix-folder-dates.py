import sys
import os
import glob
from datetime import datetime
from datetime import timedelta

MIN_NUM_ARGS = 2


def printUsage():
	print "Usage:"
	print "python fix-folder-dates.py <base_folderpath>"

def fix_filename(old_filename):
	old_file_date_str = old_filename.replace('_veiculos','')
	old_file_date = datetime.strptime(old_file_date_str, '%Y_%m_%d')
	new_file_date = old_file_date - timedelta(days=1)
	new_file_date_str = new_file_date.strftime("%Y_%m_%d") + '_veiculos'
	return new_file_date_str


if len(sys.argv) < MIN_NUM_ARGS:
	print "Wrong Usage!"
	printUsage()
	sys.exit(1)

base_folderpath = sys.argv[1]

foldername_pattern = '*_veiculos'

folders = glob.glob(os.path.join(base_folderpath, foldername_pattern))
temp_folders = []
new_folders = []

for folderpath in folders:
	foldername = folderpath.split('/')[-1]
	new_foldername = fix_filename(foldername)
	print "Old Name:", foldername
	print "New Name:", new_foldername
	new_folders.append(os.path.join(base_folderpath,new_foldername))
	temp_folders.append(os.path.join(base_folderpath,new_foldername + '_'))

for i in xrange(len(folders)):
	os.rename(folders[i],temp_folders[i])

for i in xrange(len(folders)):
	os.rename(temp_folders[i],new_folders[i])
