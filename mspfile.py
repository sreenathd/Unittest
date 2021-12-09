#!/usr/bin/env python2
# pylint: disable=line-too-long,too-many-instance-attributes,too-many-arguments,too-few-public-methods
''' code for class MspFile.
    This class represents the files on the edge switches, which provide the data for mspfwd.
'''
from __future__ import print_function
import copy
import re
import time
import gc
import sys
import os
import unittest
import pdb
import kazoo.exceptions
from mspexceptions import BsaSftpException
from mspzone import MspZone

class MspFile(object):
    """
    A dedicated class for msp files that will be going through processing stages
    stages:
    1) download msp files
    2) encrypt and run stats
    3) publish to bsa streams
    4) upload to bdmsp and finish
    @attribute(in): msp_zone - MspZone the file comes from
    @attribute(in): seq_num - sequential file number
    # download stage
    @attribute(in): msp_host = msp_zone.host
    @attribute(in): msp_dir = msp_zone.dir
    @attribute(in): msp_subdir
    @attribute(in): fname
    @attribute(in): flock
    # encrypt and stat stage
    @attribute:    fenc
    @attribute:    stat - json dictionary to be stored in zk
    @attribute:    bdmsp_fname
    @attribute:    is_corrupt - broken gzip or asn1 or protobuf
    # upload stage
    @attribute: bdmsp_host
    @attribute: bdmsp_dir
    # bsa stream upload stage
    @attribute(in): bsa_host = msp_zone.bsa_host
    @attribute(in): bsa_dir = msp_zone.bsa_dir
    # bsa stream population stage
    @attribute: bsa_fname = None
    """

    #regex pattern for msp files
    regex = '.*_(\\d*)_(\\d*)\\D*'
    #compiled regex pattern
    regex_comp = re.compile(regex)
    last_clear_time = time.time()
    seconds_per_day = 24*60*60
    cache = {}

    # let's list attributes explicitly to avoid typos
    __slots__ = [
        'msp_zone',
        'seq_num',
        'msp_host',
        'msp_dir',
        'msp_subdir',
        'fname',
        'flock',
        'fenc',
        'stat',
        'bdmsp_fname',
        'is_corrupt',
        'bdmsp_host',
        'bdmsp_dir',
        'bsa_host',
        'bsa_dir',
        'bsa_fname',
        'msp_md5sum'
    ]

    @classmethod
    def factory(cls, msp_zone, seq_num, msp_subdir, fname, flock, bdmsp_host, bdmsp_dir):
        '''factory method for creating MspZones, this maintains a cache of MspZones, and returns the element in the cache if it exists.
            Any new MspZones are added to the cache.
        '''
        key = (msp_zone, fname)
        if key in cls.cache:
            return cls.cache[key]
        if time.time() - cls.last_clear_time > cls.seconds_per_day:
            cls.last_clear_time = time.time()
            cls.cache = {}
            gc.collect()
        val = MspFile(msp_zone, seq_num, msp_subdir, fname, flock, bdmsp_host, bdmsp_dir)
        cls.cache[key] = val
        return val

    def __init__(self, msp_zone, seq_num, msp_subdir, fname, flock, bdmsp_host, bdmsp_dir):
        super(MspFile, self).__init__()
        self.msp_zone = msp_zone
        self.seq_num = seq_num
        self.msp_host = msp_zone.host
        self.msp_dir = msp_zone.dir
        self.msp_subdir = msp_subdir
        self.fname = fname
        self.flock = flock
        self.fenc = None
        self.stat = {}
        self.bdmsp_fname = None
        self.is_corrupt = False
        self.bdmsp_host = bdmsp_host
        self.bdmsp_dir = bdmsp_dir
        self.bsa_host = msp_zone.bsa_host
        self.bsa_dir = msp_zone.bsa_dir
        self.bsa_fname = None
        self.msp_md5sum = None

    def __repr__(self):
        bdmsp_fname = self.bdmsp_fname if self.bdmsp_fname else 'None'
        bsa_fname = self.bsa_fname if self.bsa_fname else 'None'
        return '{' + ','.join([str(self.msp_zone), str(self.seq_num), self.msp_host,
                               self.msp_dir, self.fname, bdmsp_fname, bsa_fname])  + '}'

    @classmethod
    def latest_msp_files(cls, msp_zone, limit, blacklist, available_files, zk_get_children, zk_exists, seq_num, bdmsp_rr_dst, logging):
        """
        Compose the list of MspFiles available at msp adm server
        Consider only those files that we didn't work on yet, those won't be present in Zookeeper storage
        @param msp_zone - MspZone object describing the zone with (host, dir, is_glob, bsa_host, bsa_dir) attributes
        @param limit - this paramater is now ignored
        @param blacklist - list of "bad" sftp locations that we want to avoid using
        """
        if msp_zone.is_glob:
            try:
                dates = [sd for sd in available_files(msp_zone.host, msp_zone.dir, sys.maxsize) if re.match("[0-9]{6}", sd)]
            except BsaSftpException:
                logging.warn("first directory listing attempt failed for: {0}:{1}".format(msp_zone.host, msp_zone.dir))
                dates = [sd for sd in available_files(msp_zone.host, msp_zone.dir, sys.maxsize) if re.match("[0-9]{6}", sd)]
            subdirs = sorted(dates, reverse=True)
        else:
            subdirs = ['']
        latest_files = list()
        mspfiles = list()	# list of (file, subdir) pairs
        # get list of all files that are not transmitted yet
        for sd in subdirs:
            try:
                zkfiles = set(zk_get_children(msp_zone.zkdir+"/"+sd))
            except kazoo.exceptions.NoNodeError:
                zkfiles = set([])
            try:
                avail = available_files(msp_zone.host, os.path.join(msp_zone.dir, sd), sys.maxsize)
            except BsaSftpException:
                logging.warn("first directory listing attempt failed for: {0}:{1}/{2}".format(msp_zone.host, msp_zone.dir, sd))
                avail = available_files(msp_zone.host, os.path.join(msp_zone.dir, sd), sys.maxsize)
            mspfiles += [(f, sd) for f in avail if f not in zkfiles and re.match(msp_zone.filename_match, f)]

        for (fname, sd) in mspfiles:
            if bdmsp_rr_dst is None:
                (bdmsp_host, bdmsp_dir) = (None, None)
            else:
                (bdmsp_host, bdmsp_dir) = bdmsp_rr_dst(seq_num, blacklist)
            flock = msp_zone.zkdir+"/"+sd+"/"+fname
            latest_files.append(MspFile.factory(msp_zone, seq_num, sd, fname, flock, bdmsp_host, bdmsp_dir))
            seq_num += 1
        latest_files.reverse()
        return latest_files, seq_num


class MspZoneStub(object):
    '''mocking class for MspZone'''
    def __init__(self, host, adir, bsa_host, bsa_dir, zkroot, filename_match):
        self.host = host
        self.msp_host = host
        (self.dir, self.is_glob) = MspZone.dir_truncate_wildcard(adir)
        self.msp_dir = adir
        self.bsa_host = bsa_host
        self.bsa_dir = bsa_dir
        self.zkdir = zkroot+"/"+self.host+self.dir
        self.filename_match = filename_match


def wrap_bdmsp_rr_dst(seq_num, blacklist):
    return 'tstdata1-mtlab', '/mspstats/tel/group3_archive/*'


class MockAvailableFiles(object):
    def __init__(self, dict_dir_prev_files, dict_dir_new_files):
        self.dict_dir_prev_files = copy.deepcopy(dict_dir_prev_files)
        self.dict_dir_new_files = copy.deepcopy(dict_dir_new_files)

    def __call__(self, hist, adir, nlimit):
        avail_files = None
        if adir in self.dict_dir_prev_files:
            avail_files = self.dict_dir_prev_files[adir]
        if adir in self.dict_dir_new_files:
            avail_files += self.dict_dir_new_files[adir]
        if avail_files is None:
            print('adir={} not recognized'.format(adir))
            sys.exit(1)
        return avail_files

class MockExists(object):
    def __init__(self, all_files):
        self.all_files = set(all_files)

    def __call__(self, filename):
        return filename in self.all_files

class MockGetChildren(object):
    def __init__(self, prefix, dict_dir_prev_files):
        if prefix[-1] == '/':
            prefix = prefix[:-1]
        self.dict_dir_prev_files = {}
        for key, val in dict_dir_prev_files.items():
            akey = prefix + key
            self.dict_dir_prev_files[akey] = val

    def __call__(self, adir):
        avail_files = None
        if adir in self.dict_dir_prev_files:
            return self.dict_dir_prev_files[adir]
        print('adir={} not recognized'.format(adir))
        sys.exit(1)

    def all_files(self):
        files = []
        for _, values in self.dict_dir_prev_files.items():
            files += values
        return values


class TestCache(unittest.TestCase):
    '''Test cases for MspFile'''
    def test_factory_cache(self):
        '''test that multiple calls to create the same MspFile object, in fact return the same object from the cache'''
        msp_zone = MspZoneStub('foo', 'foo', 'foo', 'foo', '/mspfwd', '.')
        self.assertFalse(msp_zone.is_glob)
        seq_num = 1
        msp_subdir = '202111'
        flock = 'flock'
        bdmsp_host = 'bar'
        bdmsp_dir = 'foobar'
        fnames = ['zzzz_0_0', 'yyyy_0_1', 'tttt_1_0', 'ssss_1_1', 'aaaa', 'bbbb', 'ccccc']
        mspfiles = [MspFile.factory(msp_zone, seq_num, msp_subdir, fname, flock, bdmsp_host, bdmsp_dir) for fname in fnames]
        for idx, fname in enumerate(fnames):
            self.assertEqual(fname, mspfiles[idx].fname)

        #do it again, to test the cache, the same objects should be returned
        mspfiles_dupe = [MspFile.factory(msp_zone, seq_num, msp_subdir, fname, flock, bdmsp_host, bdmsp_dir) for fname in fnames]
        for idx, fname in enumerate(fnames):
            self.assertEqual(fnames[idx], mspfiles_dupe[idx].fname)
            self.assertTrue(mspfiles_dupe[idx].fname is mspfiles[idx].fname)

class MockLogger(object):
    def __init__(self):
        pass

    def info(self, msg):
        print('info:' + msg)

    def debug(self, msg):
        print('debug:' + msg)

    def error(self, msg):
        print('error:' + msg)


class TestLastestMspFiles(unittest.TestCase):
    '''Test cases for latest_msp_files'''
    def test_no_new_files(self):
        host = 'tstdata1-mtlab'
        adir = '/mspstats/tel/group3_archive/*'
        bsa_host = 'tstdata1-mtlab'
        bsa_dir = '/lz/01/in/msp'
        zkroot = "/mspfwd"
        azone = MspZoneStub(host, adir, bsa_host, bsa_dir, zkroot, '.')
        self.assertTrue(azone.is_glob)
        blacklist = []
        seq_num = 0
        limit = -1
        mock_logger = MockLogger()

        adir_211120 = [u'TrafficEventLog_tstnz01msp4ts21_40_20211120030305980_92373.chr.gz',
                       u'TrafficEventLog_tstnz01msp0ts01_06_20211120028411508_12830.chr.gz']

        new_211120 = [u'TrafficEventLog_tstnz01msp0ts01_06_20211120029511508_12831.chr.gz']

        adir_211119 = [u'TrafficEventLog_tstnz01msp1ts07_09_20211119003359313_22904.chr.gz',
                       u'TrafficEventLog_tstnz01msp1ts08_04_20211119050723133_40168.chr.gz']

        new_211119 = [u'TrafficEventLog_tstnz01msp1ts08_04_20211119060723133_40169.chr.gz']

        adir_211118 = [u'TrafficEventLog_tstnz01msp1ts07_24_20211118031293164_17431.chr.gz',
                       u'TrafficEventLog_tstnz01msp1ts08_25_20211118022916680_90764.chr.gz']

        new_211118 = [u'TrafficEventLog_tstnz01msp1ts08_25_20211118033016680_90765.chr.gz']


        test1_dict_dir_prev_files = {
            '/mspstats/tel/group3_archive/211118':adir_211118,
            '/mspstats/tel/group3_archive/211119':adir_211119,
            '/mspstats/tel/group3_archive/211120':adir_211120,
            '/mspstats/tel/group3_archive': ['211118', '211119', '211120']
        }

        test1_dict_dir_new_files = {}

        mock_available_files = MockAvailableFiles(test1_dict_dir_prev_files, test1_dict_dir_new_files)

        mock_get_children = MockGetChildren('/mspfwd/tstdata1-mtlab/', test1_dict_dir_prev_files)

        mock_exists = MockExists(mock_get_children.all_files())

        latest_files, seq_num = MspFile.latest_msp_files(azone, limit, blacklist, mock_available_files, mock_get_children,
                                                         mock_exists, seq_num, wrap_bdmsp_rr_dst, mock_logger)

        expected_latest_files = []
        self.assertEqual(0, len(latest_files))
        self.assertEqual(0, seq_num)


    def test_one_new_file(self):
        host = 'tstdata1-mtlab'
        adir = '/mspstats/tel/group3_archive/*'
        bsa_host = 'tstdata1-mtlab'
        bsa_dir = '/lz/01/in/msp'
        zkroot = "/mspfwd"
        azone = MspZoneStub(host, adir, bsa_host, bsa_dir, zkroot, '.')
        self.assertTrue(azone.is_glob)
        blacklist = []
        limit = -1
        mock_logger = MockLogger()

        adir_211120 = [u'TrafficEventLog_tstnz01msp4ts21_40_20211120030305980_92373.chr.gz',
                       u'TrafficEventLog_tstnz01msp0ts01_06_20211120028411508_12830.chr.gz']

        new_211120 = [u'TrafficEventLog_tstnz01msp0ts01_06_20211120029511508_12831.chr.gz']

        adir_211119 = [u'TrafficEventLog_tstnz01msp1ts07_09_20211119003359313_22904.chr.gz',
                       u'TrafficEventLog_tstnz01msp1ts08_04_20211119050723133_40168.chr.gz']

        new_211119 = [u'TrafficEventLog_tstnz01msp1ts08_04_20211119060723133_40169.chr.gz']

        adir_211118 = [u'TrafficEventLog_tstnz01msp1ts07_24_20211118031293164_17431.chr.gz',
                       u'TrafficEventLog_tstnz01msp1ts08_25_20211118022916680_90764.chr.gz']

        new_211118 = [u'TrafficEventLog_tstnz01msp1ts08_25_20211118033016680_90765.chr.gz']


        test2_dict_dir_prev_files = {
            '/mspstats/tel/group3_archive/211118':adir_211118,
            '/mspstats/tel/group3_archive/211119':adir_211119,
            '/mspstats/tel/group3_archive/211120':adir_211120,
            '/mspstats/tel/group3_archive': ['211118', '211119', '211120']
        }

        mock_get_children = MockGetChildren('/mspfwd/tstdata1-mtlab/', test2_dict_dir_prev_files)

        mock_exists = MockExists(mock_get_children.all_files())

        #new files in 211118
        test2_dict_dir_new_files = {'/mspstats/tel/group3_archive/211118':new_211118}

        mock_available_files = MockAvailableFiles(test2_dict_dir_prev_files, test2_dict_dir_new_files)

        seq_num = 0
        latest_files, seq_num = MspFile.latest_msp_files(azone, limit, blacklist, mock_available_files, mock_get_children,
                                                         mock_exists, seq_num, wrap_bdmsp_rr_dst, mock_logger)

        expected_latest_files = new_211118
        self.assertEqual(1, len(latest_files))
        self.assertEqual(1, seq_num)

        latest_file_names = [afile.fname for afile in latest_files]
        self.assertEqual(expected_latest_files, latest_file_names)

        #new files in 211119
        test2_dict_dir_new_files = {'/mspstats/tel/group3_archive/211119':new_211119}

        mock_available_files = MockAvailableFiles(test2_dict_dir_prev_files, test2_dict_dir_new_files)

        seq_num = 0
        latest_files, seq_num = MspFile.latest_msp_files(azone, limit, blacklist, mock_available_files, mock_get_children,
                                                         mock_exists, seq_num, wrap_bdmsp_rr_dst, mock_logger)

        expected_latest_files = new_211119
        self.assertEqual(1, len(latest_files))
        self.assertEqual(1, seq_num)

        latest_file_names = [afile.fname for afile in latest_files]
        self.assertEqual(expected_latest_files, latest_file_names)

        #new files in 211120
        test2_dict_dir_new_files = {'/mspstats/tel/group3_archive/211120':new_211120}

        mock_available_files = MockAvailableFiles(test2_dict_dir_prev_files, test2_dict_dir_new_files)

        seq_num = 0
        latest_files, seq_num = MspFile.latest_msp_files(azone, limit, blacklist, mock_available_files, mock_get_children,
                                                         mock_exists, seq_num, wrap_bdmsp_rr_dst, mock_logger)

        expected_latest_files = new_211120
        self.assertEqual(1, len(latest_files))
        self.assertEqual(1, seq_num)

        latest_file_names = [afile.fname for afile in latest_files]
        self.assertEqual(expected_latest_files, latest_file_names)


    def test_two_new_files(self):
        host = 'tstdata1-mtlab'
        adir = '/mspstats/tel/group3_archive/*'
        bsa_host = 'tstdata1-mtlab'
        bsa_dir = '/lz/01/in/msp'
        zkroot = "/mspfwd"
        azone = MspZoneStub(host, adir, bsa_host, bsa_dir, zkroot, '.')
        self.assertTrue(azone.is_glob)
        blacklist = []
        limit = -1
        mock_logger = MockLogger()

        adir_211120 = [u'TrafficEventLog_tstnz01msp4ts21_40_20211120030305980_92373.chr.gz',
                       u'TrafficEventLog_tstnz01msp0ts01_06_20211120028411508_12830.chr.gz']

        new_211120 = [u'TrafficEventLog_tstnz01msp0ts01_06_20211120029511508_12831.chr.gz']

        adir_211119 = [u'TrafficEventLog_tstnz01msp1ts07_09_20211119003359313_22904.chr.gz',
                       u'TrafficEventLog_tstnz01msp1ts08_04_20211119050723133_40168.chr.gz']

        new_211119 = [u'TrafficEventLog_tstnz01msp1ts08_04_20211119060723133_40169.chr.gz']

        adir_211118 = [u'TrafficEventLog_tstnz01msp1ts07_24_20211118031293164_17431.chr.gz',
                       u'TrafficEventLog_tstnz01msp1ts08_25_20211118022916680_90764.chr.gz']

        new_211118 = [u'TrafficEventLog_tstnz01msp1ts08_25_20211118033016680_90765.chr.gz']


        test2_dict_dir_prev_files = {
            '/mspstats/tel/group3_archive/211118':adir_211118,
            '/mspstats/tel/group3_archive/211119':adir_211119,
            '/mspstats/tel/group3_archive/211120':adir_211120,
            '/mspstats/tel/group3_archive': ['211118', '211119', '211120']
        }

        mock_get_children = MockGetChildren('/mspfwd/tstdata1-mtlab/', test2_dict_dir_prev_files)

        mock_exists = MockExists(mock_get_children.all_files())

        #new files in 211118 and 211119
        test2_dict_dir_new_files = {
            '/mspstats/tel/group3_archive/211118':new_211118,
            '/mspstats/tel/group3_archive/211119':new_211119
        }

        mock_available_files = MockAvailableFiles(test2_dict_dir_prev_files, test2_dict_dir_new_files)

        seq_num = 0
        latest_files, seq_num = MspFile.latest_msp_files(azone, limit, blacklist, mock_available_files, mock_get_children,
                                                         mock_exists, seq_num, wrap_bdmsp_rr_dst, mock_logger)

        expected_latest_files = new_211118 + new_211119
        self.assertEqual(2, len(latest_files))
        self.assertEqual(2, seq_num)

        latest_file_names = [afile.fname for afile in latest_files]
        self.assertEqual(expected_latest_files, latest_file_names)

        #new files in 211118 and 211120
        test2_dict_dir_new_files = {
            '/mspstats/tel/group3_archive/211118':new_211118,
            '/mspstats/tel/group3_archive/211120':new_211120
        }

        mock_available_files = MockAvailableFiles(test2_dict_dir_prev_files, test2_dict_dir_new_files)

        seq_num = 0
        latest_files, seq_num = MspFile.latest_msp_files(azone, limit, blacklist, mock_available_files, mock_get_children,
                                                         mock_exists, seq_num, wrap_bdmsp_rr_dst, mock_logger)

        expected_latest_files = new_211118 + new_211120
        self.assertEqual(2, len(latest_files))
        self.assertEqual(2, seq_num)

        latest_file_names = [afile.fname for afile in latest_files]
        self.assertEqual(expected_latest_files, latest_file_names)

        #new files in 211119 and 211120
        test2_dict_dir_new_files = {
            '/mspstats/tel/group3_archive/211119':new_211119,
            '/mspstats/tel/group3_archive/211120':new_211120
        }

        mock_available_files = MockAvailableFiles(test2_dict_dir_prev_files, test2_dict_dir_new_files)

        seq_num = 0
        latest_files, seq_num = MspFile.latest_msp_files(azone, limit, blacklist, mock_available_files, mock_get_children,
                                                         mock_exists, seq_num, wrap_bdmsp_rr_dst, mock_logger)

        expected_latest_files = new_211119 + new_211120
        self.assertEqual(2, len(latest_files))
        self.assertEqual(2, seq_num)

        latest_file_names = [afile.fname for afile in latest_files]
        self.assertEqual(expected_latest_files, latest_file_names)



if __name__ == '__main__':
    unittest.main()
