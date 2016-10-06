"""
OMC class for searching filelist.tar.gz that is generated nightly on the rawdata.oceanobservatories.org webdav server.
This file is only accessible with a secure log-in. It lists all of the current files that are on the Rutgers Cyberinfrastructure
omc server that is remotely synchronized from Oregon State University and Woods Hole Oceanographic Institution.
"""
import os, sys
import pandas as pd
from datetime import datetime as dt
import csv


class ListOMC(object):
    def __init__(self, file_name):
        self.file_name = file_name
        self.list_files = []
        self.list_match = []
        self.time_format = '%Y-%m-%d %H:%M:%S'

    def load_csv(self):
        if not self.list_files:
            df = pd.read_csv(self.file_name, sep=' ', header=None, error_bad_lines=False, parse_dates=[[1, 2]])
            df.columns = ['datetime', 'epoch_time', 'size', 'filedir']
            self.list_files = self.iter_df(df)

        else:
            sys.stderr.write('CSV file already loaded')
            sys.stderr.flush()
            return

    def iter_df(self, df):
        file_list = []
        for row in df.itertuples():
            t = row.filedir.split('/')
            # print t
            if len(t) > 5:
                platform = t[4]
                deployment = t[5]

                if deployment[0] is 'D':
                    method = 't'
                else:
                    method = 'r'

                path = '/'.join([e for i, e in enumerate(t) if i > 3])

                file_dict = dict(platform=platform,
                                 deployment=deployment[-1],
                                 acquisition=method,
                                 file_path=path,
                                 file_size=row.size,
                                 datetime=row.datetime,
                                 epoch_time=row.epoch_time)
                file_list.append(file_dict)
        return file_list

    def str_to_datetime(self, time_string):
        return dt.strptime(time_string, self.time_format)

    def get_files_newer_than(self, target_time):
        try:
            dt_target_time = self.str_to_datetime(target_time)
        except ValueError:
            sys.stderr.write("Time data: '%s' does not match the format 'yyyy-mm-dd HH:MM:SS'".format(self.time_format))
            sys.stderr.flush()
            return
        self.list_match = [r for r in self.list_files if r['datetime'] >= dt_target_time]
        return self.list_match

    def get_files_between(self, start_time, end_time):
        try:
            start_time = self.str_to_datetime(start_time)
        except ValueError:
            sys.stderr.write("Time data: '%s' does not match the format 'yyyy-mm-dd HH:MM:SS'".format(self.time_format))
            sys.stderr.flush()
            return

        try:
            end_time = self.str_to_datetime(end_time)
        except ValueError:
            sys.stderr.write("Time data: '%s' does not match the format 'yyyy-mm-dd HH:MM:SS'".format(self.time_format))
            sys.stderr.flush()
            return

        if not start_time < end_time:
            sys.stderr.write('End time is before start time. Start time must always be less than end time')
            sys.stderr.flush()
            return
        self.list_match = [r for r in self.list_files if start_time <= r['datetime'] <= end_time]
        return self.list_match

    def get_platform_newer_than(self, platform, target_time):
        try:
            target_time = self.str_to_datetime(target_time)
        except ValueError:
            sys.stderr.write("Time data: '%s' does not match the format 'yyyy-mm-dd HH:MM:SS'".format(self.time_format))
            sys.stderr.flush()
            return
        self.list_match = [r for r in self.list_files if r['platform'] == platform and r['datetime'] >= target_time]
        return self.list_match

    def get_platform_by_deployment(self, platform, deployment, acquisition=None):
        if acquisition is not None:
            self.list_match = [r for r in self.list_files if r['platform'] == platform and r['deployment'] is str(deployment) and r['acquisition'] == acquisition]
            return self.list_match
        else:
            self.list_match = [r for r in self.list_files if r['platform'] == platform and r['deployment'] is str(deployment)]
            return self.list_match

    @staticmethod
    def to_csv(f_list, outfile=dt.now().strftime('%Y-%m-%dT%H%M00_flist.csv')):
        keys = f_list[0].keys()
        with open(outfile, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            for row in f_list:
                dict_writer.writerow(row)

    # def to_json(self):
    #     file_path = os.path.join(self.output_path, '%s.json' % particle_type)
    #     with open(file_path, 'w') as fh:
    #         json.dump(self.samples[particle_type], fh)

    # def write(self):
    #     option_map = {
    #         'csv': self.to_csv,
    #         'json': self.to_json
    #     }
    #     formatter = option_map[self.formatter]
    #     formatter()

    # def to_datetime(self):
    #     for file in self.file_list:
    #         file['datetime'] = file['datetime'].strftime(self.time_format)
