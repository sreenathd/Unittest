#!/usr/bin/env python
import re

class MspZone(object):
    """
    Msp Zone class
    @attribute host - MSP host name
    @attribute dir - MSP directory name without * wildcard
    @attribute is_glob - wheather the directory was given with * wildcard in conf file
    @attribute zkdir - the correspondent location for this host+dir in zookeeper
    @attribute bsa_host - what BSA channel this zone goes to
    @attribute bsa_dir - what BSA channel this zone goes to
    @attribute zone_type - some MSP zones may have customized behavior
    @attribure seqnum - sequence number in conf file
    @attribure filename_append - append the uniq suffix to all file names from this zone
    @attribure filename_match - retrieve data files match with filename
    """
    # let's list attributes explicitly to avoid typos
    __slots__ = [
        'host',
        'dir',
        'is_glob',
        'zkdir',
        'bsa_host',
        'bsa_dir',
        'zone_type',
        'seqnum',
        'filename_append',
        'filename_match'
    ]

    @classmethod
    def dir_truncate_wildcard(cls, wdir):
        """
        Truncate optional /* wildcard suffix from dir name
        @param wdir - directory name optionally ending with /*
        @return (dir, is_glob) pair - where dir is canonicalized name without /*
            is_glob is a boolean indicator telling if /* was used
        """
        mdir = re.match(r"^([^*]+)/\*$", wdir)
        if mdir:
            return (mdir.group(1), True)
        else:
            return (wdir, False)

    @classmethod
    def msp_zones(cls, conf, zkroot):
        """
        MspZones from conf file.
        @param conf - entire configuration file contents as dictionary
        @return MspZone objects
        """
        i = 0
        zones = []
        for mzcf in conf["msp"]:
            zones.append(MspZone(conf, mzcf, zkroot, i))
            i += 1
        return zones

    def __init__(self, conf, msp_cf_entry, zkroot, seqnum=-1):
        """
        @param conf - entire configuration file contents as dictionary
        @param msp_cf_entry - section of conf file describing msp zone
        """
        super(MspZone, self).__init__()
        self.seqnum = seqnum
        self.host = msp_cf_entry["host"]
        # check if the dir name ends with /* - then need to go into subdirectories
        (self.dir, self.is_glob) = self.dir_truncate_wildcard(msp_cf_entry["dir"])
        self.zkdir = zkroot+"/"+self.host+self.dir
        if "output_stream" in msp_cf_entry and "output_streams" in conf:
            msp_output_stream = msp_cf_entry["output_stream"]-1
            self.bsa_host = conf["output_streams"][msp_output_stream]["host"]
            self.bsa_dir = conf["output_streams"][msp_output_stream]["dir"]
        else:
            self.bsa_host = None
            self.bsa_dir = None
        self.zone_type = msp_cf_entry.get("zone_type", None)
        self.filename_append = msp_cf_entry.get("filename_append", None)
        self.filename_match = msp_cf_entry.get("filename_match", ".")

    def __repr__(self):
        zone_type = self.zone_type if self.zone_type else "None"
        filename_append = self.filename_append if self.filename_append else "None"
        if self.bsa_host:
            return '{' + ','.join([self.host, self.dir, self.zkdir, zone_type, filename_append, self.bsa_host, self.bsa_dir]) + '}'
        else:
            return '{' + ','.join([self.host, self.dir, self.zkdir, zone_type, filename_append]) + '}'
