"""
OMC class for searching filelist.tar.gz that is generated nightly on the rawdata.oceanobservatories.org webdav server.
This file is only accessible with a secure log-in. It lists all of the current files that are on the Rutgers Cyberinfrastructure
omc server that is remotely synchronized from Oregon State University and Woods Hole Oceanographic Institution.
"""
import os, sys
import pandas as pd
from datetime import datetime as dt
import json

class List(object):
    def __init__(self, file_name, formatter):
        self.file_name = file_name
        self.file_list = []
        self._formatter = formatter
        self.time_format = '%Y-%m-%d %H:%M:%S'

    # @property
    # def format_time(self):
    #     return self.time_format
    #
    # @format_time.setter
    # def format_time(self, str_format):
    #     if type(str_format) is str:
    #         self.time_format = str_format
    #     else:
    #         return self.time_format

    def munge_csv(self):
        df = pd.read_csv(self.file_name, sep=' ', header=None, error_bad_lines=False, parse_dates=[[1, 2]])
        df.columns = ['datetime', 'epoch_time', 'size', 'filedir']
        for row in df.itertuples():
            t = row.filedir.split('/')
            platform = t[4]
            deployment = t[5]

            method, d_num = self.get_method(deployment)

            path = '/'.join([e for i, e in enumerate(t) if i > 3])

            file_dict = dict(platform=platform,
                             deployment=d_num,
                             acquisition=method,
                             file_path=path,
                             file_size=row.size,
                             datetime=row.datetime,
                             epoch_time=row.epoch_time)
            self.file_list.append(file_dict)

    @staticmethod
    def get_method(method_string):
        if method_string[0] is 'D':
            method =  'telemetered'
        else:
            method =  'recovered'
        return method, method_string[-1]

    def str_to_datetime(self, time_string):
        return dt.strptime(time_string, self.time_format)

    def get_files_newer_than(self, target_time):
        try:
            dt_target_time = self.str_to_datetime(target_time)
        except ValueError:
            sys.stderr.write("Time data: '%s' does not match the format 'yyyy-mm-dd HH:MM:SS'".format(self.time_format))
            sys.stderr.flush()
            return
        return [r for r in self.file_list if r['datetime'] >= dt_target_time]

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
        return [r for r in self.file_list if start_time <= r['datetime'] <= end_time]

    def get_platform_newer_than(self, platform, target_time):
        try:
            target_time = self.str_to_datetime(target_time)
        except ValueError:
            sys.stderr.write("Time data: '%s' does not match the format 'yyyy-mm-dd HH:MM:SS'".format(self.time_format))
            sys.stderr.flush()
            return
        return [r for r in self.file_list if r['platform'] == platform and r['datetime'] >= target_time]

    def get_platform_by_deployment(self, platform, deployment, acquisition=None):
        if acquisition is not None:
            return [r for r in self.file_list if r['platform'] == platform and r['deployment'] is str(deployment) and r['acquisition'] == acquisition]
        else:
            return [r for r in self.file_list if r['platform'] == platform and r['deployment'] is str(deployment)]

    def to_csv(self, output_path):
        for file in self.file_list:
            file_path = os.path.join(output_path, '%s.csv' % particle_type)
            dataframes[particle_type].to_csv(file_path)

    def to_json(self):
        for particle_type in self.samples:
            file_path = os.path.join(self.output_path, '%s.json' % particle_type)
            with open(file_path, 'w') as fh:
                json.dump(self.samples[particle_type], fh)

    def write(self):
        option_map = {
            'csv': self.to_csv,
            'json': self.to_json
        }
        formatter = option_map[self.formatter]
        formatter()