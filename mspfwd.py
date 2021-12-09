#!/usr/bin/env python
# TODO don't use ephemeral node for instance lock, as there is no way to
#   handle the ZK connection loss. The current approach with ZkLostException
#   is good enough, but not ideal
# TODO proper approach is to use permanent node as a lock and update it periodically in daemon run()
#   then recover process has to look at lock time stamp and cleanup accordingly
# Run linter with pylint --good-names=i,j,k,ex,_,zk,sd,cv,go mspfwd.py
# pylint: disable=line-too-long,logging-format-interpolation,raising-bad-type,no-member,
# TODO fix logging-format-interpolation in the future
"""
High-available (HA) active-active MSP producer using zookeeper

MSP --> +--> BDMSP (primary mandatory guaranteed delivery feed)
        |
        +--> BSA (secondary optional no guarantee feed)

The forwarder is not using any data backlog buffer and doesn't download src MSP files many times either.
So, when primary destination is down the secondary cannot be fed either.

The forwarder conveyor is constructed as follows:

conveyor = MspBeginStage -> MspWorkerStage    -> MspWorkerStage          -> MspWorkerStage     -> MspWorkerStage
                            MspDownloadWorker    MspEncryptAndStatWorker    MspBsaStreamWorker    MspUploadWorker

Batches of 4 MspFile objects proceed through all conveyor stages

BeginStage sequentializes given set of MspFiles in batches of 4 (default). All Worker stages works simulteuneously in
separate threads and internally handle 4 files at a time in their inner threads.

MspFwdDaemon.run() becomes a simple process with following logic:

zk = connect_to_zookeeper(conf)
while True:
    for zone in MspZone.msp_zones(conf, ZKROOT):
        if acquare_lock_in_zk(zk, zone):
            files = latest_msp_files(zk, zone)
            conveyor.start(files)
            for done_batch in conveyor:
                ordered_commit(zk, done_batch)
        sleep()

latest_msp_files() subtracts msp zone directory listing and our zookeeper statistics on all transmitted files.
ordered_commit() does final 'mv' for remote files and marks files as transmitted in zk storage

MSP and BDMSP locations have to be listed in mspfwd.yaml file,
in addition to that each sftp host has to be listed in your ssh config.
Use ControlMaster for speedup, ~/.ssh/config example:

ControlMaster auto
ControlPath ~/.ssh/ssh_mux_%h_%p_%r
ControlPersist 30s
Host msp01
 HostName 10.240.0.1
 User bsa
Host bdmsp01
 HostName 10.240.0.1
 User bsa

mspfwd.yaml example

zk:
  - zoo1:2181
  - zoo2:2181
  - zoo3:2181
msp:
  - host: msp01
    dir: /tmp/in
bdmsp:
  - host: bdmsp01
    dir: /tmp/out
"""
from __future__ import print_function
import os
import sys
import traceback
import getopt
import uuid
import re
import time
import threading
import signal	# only for killme
import json
import socket 	# only for hostname
import random
import logging
import logging.handlers
import gc
import tempfile
from subprocess import Popen, PIPE
import yaml
from inspect import currentframe, getframeinfo
import kazoo.client
import kazoo.exceptions
import prometheus_client as prom
import kafka
from mspexceptions import ZkLostException, BsaException, BsaSftpException, BsaExecException
from mspzone import MspZone
from mspfile import MspFile
from string_helpers import insert_string_before_file_type, drop_filename_extension, unix_time_to_yyyymmddhhmmss

ZKROOT = "/mspfwd"			#: Zookeeper directory under which all forwarder registry is kept
ZOO_TIMEOUT = 1200
CONF_FILE = "/home/bsa/mspfwd.yaml"
TMP_DIR = "/tmp"                        #: may be overwritten in conf file
GLOBAL_LOCK_NAME = "global.lock"	#: Global lock name to be used for exclusive operations such as recovery
SFTP_ERRORS = "/sftp/errors"		#: where we log sftp errors
SFTP_BLACKLIST = "/sftp/blacklist"	#: sftp locations that we want to avoid
VALIDATE_DECRYPTION = True		#: we see corrupt openssl output and want to do extra validation
MAX_MESSAGE_SIZE = 300*0x100000  #: this is max allowed on bsa kafka brokers
KNOWN_HOSTS_FILE = os.path.expanduser('~') + '/.ssh/known_hosts'
OPENSSL_TIMEOUT = '30s'

EXTREMELY_VERBOSE = False
''' This enables DEBUG level logging for calls to kazoo, (the python interface to zookeeper). Note that this
    is dangerous, because kazoo will print the full list of file names in a single call to the logger.
    This will crash the logger, and mspfwd.py, if there are too many files.
'''

CLEANUP_AT_SHUTDOWN = True
''' This causes mspfwd.py to delete intermediate files at shutdown. If one encounters a situation where
    mspfwd.py is crashing with certain files, setting this option to False will let one keep the files which
    caused the crash.
'''

def killme():
    """
    DEBUG function to inflict an immediate process hard crash
    """
    os.kill(os.getpid(), signal.SIGKILL)

def print_log_line(aline, log_level):
    if log_level == logging.DEBUG:
        logging.debug(aline)
    elif log_level == logging.INFO:
        logging.info(aline)
    elif log_level == logging.WARNING:
        logging.warning(aline)
    elif log_level == logging.ERROR:
        logging.error(aline)
    elif log_level == logging.CRITICAL:
        logging.critical(aline)
    elif log_level == logging.FATAL:
        logging.fatal(aline)
    else:
        logging.error(aline)
        log_message = 'log level {} not recognized'.format(log_level)
        logging.error(log_message)

def print_traceback(exception_name, filename, lineno, level=logging.INFO):
    print_log_line("{} caught at line {} of {}".format(exception_name, filename, lineno), level)
    exc_type, exc_value, exc_traceback = sys.exc_info()
    somestrs = traceback.format_exception(exc_type, exc_value, exc_traceback)
    for astr in somestrs:
        print_log_line(astr, level)

def log_level_to_int(log_level):
    if log_level == 'DEBUG':
        return logging.DEBUG
    elif log_level == 'INFO':
        return logging.INFO
    elif log_level == 'WARNING':
        return logging.WARNING
    elif log_level == 'ERROR':
        return logging.ERROR
    elif log_level == 'CRITICAL':
        return logging.CRITICAL
    elif log_level == 'FATAL':
        return logging.FATAL
    return -1

def zk_get_children(zk_obj, zk_dir):
    prev_level = logging.root.level
    if prev_level == logging.DEBUG and not EXTREMELY_VERBOSE:
        logging.root.setLevel(logging.INFO)
    files = zk_obj.get_children(zk_dir)
    if prev_level != logging.root.level:
        logging.root.setLevel(prev_level)
    return files

class KafkaProducer(object):
    ''' Encapsulates the Kafka class. '''
    def __init__(self, kafka_conf):
        self.topic = kafka_conf['topic']

        max_request_size = kafka_conf.get('max_request_size', MAX_MESSAGE_SIZE)

        pargs = {
            'bootstrap_servers': kafka_conf['servers'],
            'acks': 'all',
            'client_id': "mspfwd-client-" + socket.gethostname(),
            'max_request_size': max_request_size,
            'batch_size': max_request_size,
            'request_timeout_ms': 100000,
            'buffer_memory': max_request_size * 2,
            'retry_backoff_ms': 5000,
            'reconnect_backoff_ms': 10000,
            'max_in_flight_requests_per_connection': 1,
            'retries': 3,
            'api_version': (2, 2, 0)
        }
        if 'ssl_password' in kafka_conf:
            pargs['security_protocol'] = 'SSL'
            pargs['ssl_cafile'] = kafka_conf['ssl_cafile']
            pargs['ssl_keyfile'] = kafka_conf['ssl_keyfile']
            pargs['ssl_password'] = kafka_conf['ssl_password']
            pargs['ssl_certfile'] = kafka_conf['ssl_certfile']

        self.kafka_conf = kafka_conf
        self.pargs = pargs
        self.prod = None


    def open(self):
        '''
            Boilerplate code to open Kafka Producer, insure
            that it is only opened once
        '''
        if self.prod is None:
            self.prod = kafka.KafkaProducer(**self.pargs)
        return self

    def close(self):
        '''
            Boilerplate code to close Kafka Producer, insure
            that it is only closed while open.
        '''
        if self.prod:
            self.prod.close()
            self.prod = None

    def send(self, key, value):
        '''
            Sends message and returns partition and offset.
        '''
        res = self.prod.send(self.topic, key=key, value=value)
        res_md = res.get() # sync
        return res_md.partition, res_md.offset

    def __enter__(self):
        '''
        Wrapper for open, to be used in with clause.
        '''
        self.open()
        return self

    def __exit__(self, _ex_type, _ex, _traceback):
        '''
        Wrapper for close, to be used in with clause.
        '''
        self.close()

def exec_pipe(fin, fout, commands, timeout=0):
    """
    Create and execute unix processes pipeline, but don't wait for completion
    Roughly equivalent to shell: (echo ok | wc -c) <fin >fout &
    We don't use subprocess.Pipe() as its not thread-safe and it doesn't
    close extra descriptors in the child leaving a possibility of a "deadlock".
    If the downstream child quits immediately the upstream may hang on write()
    instead of receiving proper SIGPIPE.
    So we construct pipe using low-level POSIX mantra: pipe()+fork()+dup2()+close()+exec()
    @param fin - input file handle
    @param fout - output file handle
    @param commands - list of exec args like this: [["echo", "ok"], ["wc", "-c"]]
    @param timeout - to be prepended to each command
    @return list of (Command String, Command PID) tuples
    """
    if timeout > 0:
        timeout_cmd = ['timeout', str(timeout)]
    else:
        timeout_cmd = []
    proc_lst = []
    fin_fh = os.dup(fin.fileno())	# avoid closing original fin
    for i in range(len(commands)-1):
        (pread, pwrite) = os.pipe()
        exec_cmd = timeout_cmd + commands[i]
        pid = os.fork()
        if pid == 0:
            os.dup2(fin_fh, sys.stdin.fileno())
            os.dup2(pwrite, sys.stdout.fileno())
            # must explicitly close extra descriptors
            os.close(fin_fh)
            os.close(pwrite)
            os.close(pread)
            os.execvp(exec_cmd[0], exec_cmd)
        else:
            proc_lst.append((" ".join(exec_cmd), pid))
        os.close(fin_fh)
        os.close(pwrite)
        fin_fh = pread
    exec_cmd = timeout_cmd + commands[-1]
    pid = os.fork()
    if pid == 0:
        os.dup2(fin_fh, sys.stdin.fileno())
        os.dup2(fout.fileno(), sys.stdout.fileno())
        # must explicitly close extra descriptors
        os.close(fin_fh)
        os.close(fout.fileno())
        os.execvp(exec_cmd[0], exec_cmd)
    else:
        proc_lst.append((" ".join(exec_cmd), pid))
    os.close(fin_fh)
    return proc_lst

def wait_pipe(proc_lst):
    """
    wait for unix pipe completion from exec_pipe() and raise exception in case of an error
    @param proc_lst - list of (Command String, Command PID) from prior exec_pipe()
    @raise BsaExecException() if any command in the pipe failed
    """
    err_flag = 0
    err_msg = ""
    proc_err_lst = []
    for (cmd_str, cmd_pid) in proc_lst:
        (_, ecode) = os.waitpid(cmd_pid, os.P_WAIT)
        proc_err_lst += [(cmd_str, cmd_pid, ecode)]
        err_msg += "command {0} exit code: {1}; ".format(cmd_str, ecode)
        err_flag |= ecode
    if err_flag != 0:
        raise BsaExecException(err_msg[0:-2], proc_err_lst)

def subprocess_call(cmd):
    """
    simulating subprocess.call() using our wait and exec_pipe() functions
    @param cmd is a vector of command and its arguments suitable for os.exec() invocation
    @returns cmd exit code
    """
    try:
        with open("/dev/null", "w") as dev_null_w:
            with open("/dev/null", "r") as dev_null_r:
                wait_pipe(exec_pipe(dev_null_r, dev_null_w, [cmd], timeout=-1))
        ecode = 0
    except BsaExecException as ex:
        ecode = ex.cmds[0][2]
    return ecode

def __run_bsactl_heartbeat(interval):
    while True:
        time.sleep(interval)
        subprocess_call(["bsa_metrics", "--delta", "--increasing", "BSA_MSPFWD", "HEARTBEAT", "1"])

def start_bsactl_heartbeat(interval):
    """
    Launch a dedicated daemon thread to ping BSA controller priodically.
    """
    thread = threading.Thread(target=__run_bsactl_heartbeat, args=[interval])
    thread.setDaemon(True)
    thread.start()

class MySysLogHandler(logging.handlers.SysLogHandler, object):
    """
    Must redefine syslog handler to set proper ident attribute (process name)
    """
    def __init__(self, do_bsa_logging):
        super(MySysLogHandler, self).__init__(address='/dev/log', facility="local3")
        self.do_bsa_logging = do_bsa_logging
        formatter = logging.Formatter(fmt="%(ident)s %(levelname)s: %(message)s")
        self.setFormatter(formatter)
        self.ident = "{0}[{1}]:".format(os.path.basename(sys.argv[0]), os.getpid())
    def emit(self, record):
        record.ident = self.ident
        super(MySysLogHandler, self).emit(record)
        if self.do_bsa_logging and record.levelno >= logging.ERROR:
            (_, exc, _) = sys.exc_info()
            if exc:
                subprocess_call(["bsa_log_message", "mspfwd.py", record.levelname, "SEV1", record.message+": "+str(exc)])
            else:
                subprocess_call(["bsa_log_message", "mspfwd.py", record.levelname, "SEV1", record.message])


def init_logger(do_bsa_logging, log_level=logging.INFO):
    """
    start logging into rsyslog local3 facility and to the stderr
    @param: do_bsa_logging - should we also engage BSA Controller logging facility
    """
    logger = logging.getLogger()
    logger.setLevel(log_level_to_int(log_level))

    logger.addHandler(MySysLogHandler(do_bsa_logging))
    lsh = logging.StreamHandler()
    lsh.setLevel(logging.WARN)	# only >= WARN to console
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    lsh.setFormatter(formatter)
    logger.addHandler(lsh)
    return logger

class MemorizeLastSignal(object):
    # pylint: disable=too-few-public-methods
    """
    Callable object to memorize signal and invoke default handler
    Useful if you need to handle ^C in a special way
    Just instantiate with a list of signals to look for
    @attribute signum - memorized signal number or None
    """
    def __init__(self, *signals):
        """
        Give a list of signals to memorize
        """
        super(MemorizeLastSignal, self).__init__()
        self.signum = None
        self.old_handler = dict()
        for sig in signals:
            self.old_handler[sig] = signal.signal(sig, self)
    def __call__(self, signum, frame):
        """
        Our handler memorizes the signal and invokes the old handler
        """
        self.signum = signum # atomic
        self.old_handler[signum](signum, frame)

class Sftp(object): # pylint: disable=too-few-public-methods
    """
    execute a series of sftp commands using 'with' context
    number of commands should be small to prevent pipe deadlock
    @attribute plist - exec_pipe() list of (Command String, Command PID) tuples
    @attribute host - Server we connect to
    @attribute commands - List of commands to be executed
    @attribute stdout - sftp process stdout
    """
    def __init__(self, host, commands):
        self.plist = None
        self.host = host
        self.commands = commands
        self.stdout = None
    def __enter__(self):
        with tempfile.TemporaryFile(mode='w+', dir=TMP_DIR) as fcmds:
            for cmd in self.commands:
                print(cmd, file=fcmds)
            fcmds.seek(0, 0)
            (pread_out, pwrite_out) = os.pipe()
            self.stdout = os.fdopen(pread_out, "r")
            fpwrite_out = os.fdopen(pwrite_out, "w")
            self.plist = exec_pipe(fcmds, fpwrite_out, [["sftp", "-q", "-o", "StrictHostKeyChecking=no", "-b", "-", self.host]], timeout=600)
            fpwrite_out.close()
        return self
    def __exit__(self, _ex_type, _ex, _traceback):
        try:
            wait_pipe(self.plist)
        except BsaExecException as ex:
            ecode = ex.cmds[0][2]
            raise BsaSftpException("sftp {0} exit code {1}".format(self.host, ecode), self.host)
        finally:
            self.stdout.close()

class Md5Sum(object): # pylint: disable=too-few-public-methods
    """
    compute md5sum on the file by invoking the system md5sum, do it in "with" semantics
    @attribute plist - exec_pipe() list of (Command String, Command PID) tuples
    @attribute stdout - stdout for md5sum process
    @attribute fname - file name we need md5sum for
    """
    def __init__(self, fname):
        self.plist = None
        self.stdout = None
        self.fname = fname
    def __enter__(self):
        (pread_out, pwrite_out) = os.pipe()
        self.stdout = os.fdopen(pread_out, "r")
        fpwrite_out = os.fdopen(pwrite_out, "w")
        self.plist = exec_pipe(sys.stdin, fpwrite_out, [["md5sum", self.fname]], timeout=-1)
        fpwrite_out.close()
        return self
    def get(self):
        """
        wait for md5sum executable to finish and return the value
        """
        res_line = self.stdout.readline()
        return res_line.split()[0]
    def __exit__(self, _ex_type, _ex, _traceback):
        try:
            wait_pipe(self.plist)
        except BsaExecException as ex:
            ecode = ex.cmds[0][2]
            raise BsaException("md5sum {0} exit code {1}".format(self.fname, ecode))
        finally:
            self.stdout.close()

class SshMaster(object): # pylint: disable=too-few-public-methods
    """
    ssh master agent wrapper - start all controlmaster tunnels
    and more importantly make sure they go down when daemon stops
    @attribute ssh_hosts - list of hosts to take down the ssh tunnel for
    """
    ssh_hosts = []
    def __init__(self, conf):
        """
        Discern the list of ssh_hosts from bsa config file
        @param conf - bsa configuration file
        """
        bsa_hd = []
        if os.path.exists(KNOWN_HOSTS_FILE):
            os.remove(KNOWN_HOSTS_FILE)
        if "output_streams" in conf:
            bsa_hd = conf["output_streams"]
        for ssh_host_dir in conf["msp"]+conf["bdmsp"]+bsa_hd:
            self.ssh_hosts.append(ssh_host_dir["host"])
    def open(self):
        for host in self.ssh_hosts:
            subprocess_call(["timeout", "10s", "sftp", "-o", "StrictHostKeyChecking=no", host])
        return self
    def __enter__(self):
        return self.open()
    def close(self):
        for host in self.ssh_hosts:
            subprocess_call(["timeout", "10s", "ssh", "-O", "exit", host])
    def __exit__(self, _ex_type, _ex, _traceback):
        self.close()

def copy2devnull(infile):
    """
    copy the file into /dev/null effectively ignoring unused content
    @param infile - file to be copied to /dev/null
    """
    buf_sz = 32*1024
    while len(infile.read(buf_sz)) == buf_sz:
        pass

def sftp_available_files(host, remote_dir, nlimit):
    """
    perform `tail -$n` on the remote directory
    @param host - sftp server name as per ~/.ssh/config
    @param remote_dir - directory on sftp server where we check files
    @param nlimit - get n latest files
    """
    res = []
    with Sftp(host, ["cd "+remote_dir, "ls -t1"])  as sftp:
        _ = sftp.stdout.readline()
        _ = sftp.stdout.readline()
        fname = sftp.stdout.readline()
        while nlimit > 0 and fname:
            # ignore in-progress files if needed
            if ".tmp" not in fname:
                res.append(fname.rstrip('\n'))
            nlimit -= 1
            fname = sftp.stdout.readline()
        copy2devnull(sftp.stdout)
    return res

def sftp_download(host, remote_dir, local_dir, fname):
    """
    download the specified file from remote server
    @param host - sftp server name as per ~/.ssh/config
    @param remote_dir - remote src dir
    @param local_dir - local dst dir
    @param fname - file name
    """
    with Sftp(host, ["cd "+remote_dir, "lcd "+local_dir, "get -p "+fname]) as sftp:
        copy2devnull(sftp.stdout)

def scp_download(host, remote_dir, local_dir, fname):
    """
    download the specified file from remote server
    @param host - sftp server name as per ~/.ssh/config
    @param remote_dir - remote src dir
    @param local_dir - local dst dir
    @param fname - file name
    """
    localfile = '{}/{}'.format(local_dir, fname)
    remotefile = '{}:{}/{}'.format(host, remote_dir, fname)
    cmd = ['scp', "-o", "StrictHostKeyChecking=no", remotefile, localfile]
    fail = False
    result = ''
    error = ''
    try:
        logging.info(' '.join(cmd))
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        result, error = proc.communicate()
        if proc.returncode:
            fail = True
        else:
            logging.debug('scp succeded')
    except Exception as ex:
        logging.error('scp caused exception of type {}'.format(ex.__class__.__name__))
        fail = True

    if fail:
        retstr = str(getattr(proc, "returncode", "None"))
        logging.error('scp failed in scp_download, return code = ' + retstr)
        logging.error('scp failed in scp_download, stdout = ' + result)
        logging.error('scp failed in scp_download, stderr = ' + error)
        logging.error('scp cmd="{}"'.format(' '.join(cmd)))
        raise BsaException('scp failed in scp_download, stderr = ' + error)


def scp_upload(host, remote_dir, local_dir, fname):
    """
    upload the specified file into remote server
    @param host - sftp server name as per ~/.ssh/config
    @param remote_dir - remote dst dir
    @param local_dir - local src dir
    @param fname - file name
    NB: the file will be uploaded with ".tmp" extension
    """
    localfile = '{}/{}'.format(local_dir, fname)
    remotefile = '{}:{}/{}.tmp'.format(host, remote_dir, fname)
    cmd = ['scp', "-o", "StrictHostKeyChecking=no", localfile, remotefile]
    fail = False
    result = ''
    error = ''
    try:
        logging.info(' '.join(cmd))
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        result, error = proc.communicate()
        if proc.returncode:
            fail = True
        else:
            logging.debug('scp succeded')
    except Exception as ex:
        logging.error('scp caused exception of type {}'.format(ex.__class__.__name__))
        fail = True

    if fail:
        retstr = str(getattr(proc, "returncode", "None"))
        logging.error('scp failed in scp_upload, return code = ' + retstr)
        logging.error('scp failed in scp_upload, stdout = ' + result)
        logging.error('scp failed in scp_upload, stderr = ' + error)
        logging.error('scp cmd="{}"'.format(' '.join(cmd)))
        raise BsaException('scp failed in scp_upload, stderr = ' + error)

def sftp_commit_upload(host, remote_dir, fname):
    """
    commit the uploading of the specified file into remote server
    @param host - sftp server name as per ~/.ssh/config
    @param remote_dir - remote dst dir
    @param fname - file name
    effectively just drop the .tmp extension on the remote file
    """
    with Sftp(host, ["cd "+remote_dir, "rename "+fname+".tmp"+" "+fname]) as sftp:
        copy2devnull(sftp.stdout)

def sftp_rollback_upload(host, remote_dir, fname):
    """
    remove (rollback) the file been uploaded on the remote server
    @param host - sftp server name as per ~/.ssh/config
    @param remote_dir - remote dst dir
    @param fname - file name
    """
    with Sftp(host, ["cd "+remote_dir, "rm "+fname+".tmp"]) as sftp:
        copy2devnull(sftp.stdout)

def sftp_is_uploading(host, remote_dir, fname):
    """
    check if the specified file is still been uploaded (is in .tmp state) on remote server
    @param host - sftp server name as per ~/.ssh/config
    @param remote_dir - remote dir
    @param fname - file name
    """
    res = False
    necho = 0 # make sure that we get our cd and ls commands echoed back
    try:
        commands = ["cd "+remote_dir, "ls -1 "+fname+".tmp"]
        with Sftp(host, commands) as sftp:
            for cmd in commands:
                echo = sftp.stdout.readline()
                if cmd in echo:
                    necho += 1
            echo = sftp.stdout.readline()
            res = (echo.rstrip('\n') == fname+".tmp")
            copy2devnull(sftp.stdout)
    except BsaSftpException as ex:
        frameinfo = getframeinfo(currentframe())
        print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
        # it may be ok to get non-zero exit code
        # as long as we've got both commands echoed back
        # otherwise re-raise
        if necho < 2:
            raise ex
    return res

def sftp_blacklist(zk):
    """
    Return list of blacklisted sftp locations that we want to avoid using
    @param zk - connection
    """
    bl_hosts = []
    try:
        bl_hosts = zk.get_children(ZKROOT+SFTP_BLACKLIST)
    except kazoo.exceptions.NoNodeError as ex: 
        logging.debug('NoNodeError in sftp_blacklist, returning empty set')
        pass
    return set(bl_hosts)

def sftp_mark_bad_host(zk, s_ex):
    """
    Mark sftp host as bad. With enough error statistic per remote host we'll demote host into blacklist
    @param zk - connection
    @param s_ex - instance of BsaSftpException with error message and remote host name
    """
    host = s_ex.host
    err = str(s_ex)
    err_stat = {"host": host, "error": err, "node": socket.gethostname()}
    zk.create(ZKROOT+SFTP_ERRORS+"/"+host+".", value=json.dumps(err_stat), sequence=True, makepath=True)

def validate_and_enrich_conf(conf, conf_fname):
    # pylint: disable=too-many-return-statements,too-many-branches
    """
    Validate and enrich the conf file
    return None when success or str with error
    @param - conf file as dictionary
    @param - conf_fname config file name (used as a current dir reference)
    """
    allowed_keys = ['loc', 'bsa_logging', 'zk', 'msp', 'no_bdmsp', 'bdmsp', 'audit_dst', 'msaudit_dst', 'encrypt_cert_files', 'encrypt_cert_dir', 'output_streams', 'tmp_dir', 'batch_size', 'convert', 'kafka', 'name', 'log_level', 'zoo_timeout']
    for k in conf.keys():
        if k not in allowed_keys:
            print("WARNING: unknown section {0} in conf file".format(k), file=sys.stderr) # NB! doing logging.warn() before init_logging() messes log facility
    conf_dir = os.path.dirname(conf_fname)
    if 'log_level' in conf and log_level_to_int(conf['log_level']) == -1:
        return "log_level not recognized in conf file"
    if "name" not in conf:
        return "don't see name in conf file"
    if "zk" not in conf:
        return "Don't see list of ZooKeeper servers 'zk' in conf file"
    if not isinstance(conf["zk"], list):
        return "zk section must be a list"
    if "output_streams" in conf:
        bsa_chan_sz = len(conf["output_streams"])
        if not isinstance(conf["output_streams"], list):
            return "output_streams section must be a list"
        for host_dir in conf["output_streams"]:
            if "host" not in host_dir or "dir" not in host_dir:
                return "output_stream entry must have 'host' and 'dir' attributes"
    else:
        bsa_chan_sz = 0
    if "msp" not in conf:
        return "Don't see 'msp' list in conf file"
    if not isinstance(conf["msp"], list):
        return "msp section must be a list"
    msp_n = int(0)
    for msp_el in conf["msp"]:
        m_allowed_keys = ["host", "dir", "output_stream", "zone_type", "filename_append", "filename_match"]
        for k in msp_el.keys():
            if k not in m_allowed_keys:
                print("WARNING: unknown key {0} in msp element in conf file".format(k), file=sys.stderr) # NB! doing logging.warn() before init_logging() messes log facility
        if "host" not in msp_el or "dir" not in msp_el:
            return "each msp entry must have 'host' and 'dir' attributes"
        if "output_stream" in msp_el:
            if not str(msp_el["output_stream"]).isdigit():
                return "bsa_chan attribute must be a number"
            if msp_el["output_stream"] < 1 or msp_el["output_stream"] > len(conf["output_streams"]):
                return "bsa_chan must be a number of output_channel between [1..{0}]".format(len(conf["output_streams"]))
            msp_el["output_stream"] = int(msp_el["output_stream"])
        elif "output_streams" in conf:
            # force-assign the bsa_chan number if not given
            msp_el["output_stream"] = int((msp_n-1) % bsa_chan_sz + 1)
        if "zone_type" in msp_el and msp_el["zone_type"] == "msf" and "loc" not in conf:
            # TODO: retire this
            return "You must specify 'loc' name in the conf file for 'msf' zone types"
        msp_n += 1
    if "no_bdmsp" not in conf:
        conf["no_bdmsp"] = False
    if "bdmsp" not in conf and not conf["no_bdmsp"]:
        return "Don't see 'bdmsp' list in conf file - if you don't want bdmsp delivey specify no_bdmsp: true"
    if "bdmsp" in conf:
        if not isinstance(conf["bdmsp"], list):
            return "bdmsp section must be a list"
        for bdmsp_el in conf["bdmsp"]:
            if "host" not in bdmsp_el or "dir" not in bdmsp_el:
                return "each bdmsp entry must have 'host' and 'dir' attributes"
    else:
        conf["bdmsp"] = []
    ## audit_dst is optional
    if "audit_dst" in conf:
        if not isinstance(conf["audit_dst"], list):
            return "audit_dst section must be a list"
        for ad_el in conf["audit_dst"]:
            if "host" not in ad_el or "dir" not in ad_el:
                return "each audit_dst entry must have 'host' and 'dir' attributes"
    ## msaudit_dst is optional
    if "msaudit_dst" in conf:
        if not isinstance(conf["msaudit_dst"], list):
            return "msaudit_dst section must be a list"
        for ad_el in conf["msaudit_dst"]:
            if "host" not in ad_el or "dir" not in ad_el:
                return "each msaudit_dst entry must have 'host' and 'dir' attributes"
    ####
    ecf = None
    if "encrypt_cert_files" in conf:
        ecf = conf["encrypt_cert_files"]
        if not isinstance(ecf, list):
            return "encrypt_cert_files section must be a list of keys"
        for i in xrange(len(ecf)):
            if not ecf[i].startswith('/'):
                ecf[i] = os.path.join(conf_dir, ecf[i])
                if not os.path.exists(ecf[i]):
                    return "encryption key {0} is not present".format(ecf[i])
    if "encrypt_cert_dir" in conf:
        kdir = conf["encrypt_cert_dir"]
        if not kdir.startswith('/'):
            kdir = os.path.join(conf_dir, kdir)
        if not os.path.isdir(kdir):
            return "encrypt_cert_dir {0} does not exist or it is not a dir".format(kdir)
        if not ecf and not os.listdir(kdir):
            return "encrypt_cert_dir {0} is empty and no encrypt_cert_files provided either".format(kdir)
        conf["encrypt_cert_dir"] = kdir
    if "tmp_dir" in conf and not os.path.exists(conf["tmp_dir"]):
        return "tmp_dir '{0}' does not exist".format(conf["tmp_dir"])
    if "batch_size" in conf and (not str(conf["batch_size"]).isdigit() or conf["batch_size"] < 1):
        return "invalid batch_size '{0}'".format(conf["batch_size"])
    elif not "batch_size" in conf:
        conf["batch_size"] = 4
    else:
        conf["batch_size"] = int(conf["batch_size"])
    if "convert" in conf:
        conf_convert = conf["convert"]
        c_allowed_keys = ["command", "format", "fields"]
        for k in conf_convert:
            if k not in c_allowed_keys:
                print("WARNING: unknown key {0} in convert element in conf file".format(k), file=sys.stderr) # NB! doing logging.warn() before init_logging() messes log facility
        if "command" not in conf_convert:
            return "convert section must specify executable command"
        if "fields" not in conf_convert or not isinstance(conf_convert["fields"], list):
            return "convert section must have list of fields"
    ##added for kafka server
    if "kafka" in conf:
        kafka_conf = conf["kafka"]
        if "servers" not in kafka_conf:
            return "Don't see servers in kafka section"
        if "topic" not in kafka_conf:
            return "Don't see topic in kafka section"
        allowed_keys = ["servers", "topic", "ssl_cafile", "ssl_keyfile", "ssl_certfile", "ssl_password",
                        "max_request_size"]
        for akey in kafka_conf:
            if allowed_keys.count(akey) == 0:
                return akey + " is not allowed in the kafka section"
        if "max_request_size" in kafka_conf and kafka_conf["max_request_size"] > MAX_MESSAGE_SIZE:
            return "max_request_size is too large, must be less than %d" % MAX_MESSAGE_SIZE


# help screen
def usage(cmd, conf_file):
    """
    command usage
    """
    print(
        "Forward MSP file into BDMSP\n"
        "Usage:\n"
        "\t{0} [-c <conf.yaml>] [recover|isync|cleanup|blacklist|blclear] \n"
        "Default conf.yaml location is {1}\n"
        "conf.yaml has the following structure:\n"
        "\t(exact description TBD)\n"
        " recover - look for dangling lock files and fix them\n"
        " isync - perform initial synchronization - mark all MSP files done without actually transmitting them\n"
        "   isync <msp_host> may be used to sync files only from single MSP zone server\n"
        " cleanup - remove obsolete files from zk database\n"
        " blacklist - perform routine management of bad sftp locations. Such locations will be ignored by the forwarder\n"
        "   1) Check if any sftp host gathered enough errors to be demoted into blacklist\n"
        "   2) Check if a problematic hosts were fixed and may be now removed from blacklist\n"
        " blclear - unconditionally clean blacklist of bad sftp locations\n"
        "Default behavior is start daemon\n"
        "rsyslog local3 topic has to be initalized separately\n".format(cmd, conf_file)
        )

class ParallelExec(object):
    """
    Parallel execution primitive base abstract class.
    Derive your own class with data attributes you need to process and override exec_slice(n)
    All exec_slicees would be invoked on every exec_batch()
    Don't touch anything else
    @attribute nthreads - no of parallel threads
    @attribute tick - current batch no
    @attribute active_cnt - no of threads that didn't finish the batch yet
    @attribute cv_begin - start the batch cond var
    @attribute cv_end - end the batch cond var
    @attribute threads - thread objects for each thread
    @attribute exceptions - per-thread exceptions caught (if any)
    """
    def __init__(self, nthreads):
        self.nthreads = nthreads
        self.tick = 0
        self.active_cnt = 0
        self.cv_begin = threading.Condition()
        self.cv_end = threading.Condition()
        self.exceptions = [None]*nthreads
        self.threads = [None]*nthreads
        for i in range(nthreads):
            self.threads[i] = threading.Thread(target=self.run, args=[i])
            self.threads[i].start()
    def close(self):
        """
        Signal all threads to shutdown and wait for them
        """
        self.__threads_shutdown()
        for thread in self.threads:
            thread.join()
    def __enter__(self):
        return self
    def __exit__(self, _ex_type, _ex, _traceback):
        self.close()
    def __threads_go(self):
        try:
            self.cv_begin.acquire()
            self.tick += 1
            self.active_cnt += self.nthreads
            self.cv_begin.notify_all()
        finally:
            self.cv_begin.release()
    def __threads_wait(self):
        try:
            self.cv_end.acquire()
            while self.active_cnt > 0:
                self.cv_end.wait()
        finally:
            self.cv_end.release()
    def __threads_shutdown(self):
        try:
            self.cv_begin.acquire()
            self.tick += 1
            self.active_cnt -= self.nthreads+1 # go below 0 to shutdown threads
            self.cv_begin.notify_all()
        finally:
            self.cv_begin.release()
    def run(self, threadno):
        local_tick = 0
        while True:
            local_active_cnt = 0
            try:
                self.cv_begin.acquire()
                while self.tick == local_tick:
                    self.cv_begin.wait()
                local_tick = self.tick
                local_active_cnt = self.active_cnt
            finally:
                self.cv_begin.release()
            if local_active_cnt > 0:
                try:
                    self.exec_slice(threadno)
                except Exception as ex:
                    frameinfo = getframeinfo(currentframe())
                    print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno)
                    # NB! this is a very important DEBUG logging - uncomment when making changes
                    logging.warn("worker thread {0} caught exception".format(threadno), exc_info=True)
                    self.exceptions[threadno] = ex
                finally:
                    try:
                        self.cv_end.acquire()
                        self.active_cnt -= 1
                        self.cv_end.notify()
                    finally:
                        self.cv_end.release()
            else:
                break   # quit on shutdown signal
    def exec_batch(self):
        """
        Run one batch across all parallel threads
        It unlock all threads and make them run their exec_slice() once
        Then it waits for completion of all exec_slice() invocations and puts
        them back to sleep.
        """
        self.__threads_go()
        self.__threads_wait()
        for ex in self.exceptions:
            if ex:
                raise ex
    def exec_slice(self, sliceno):
        """
        Subcalss and override me
        """
        raise NotImplementedError

class StageExec(object):
    """
    Iteratable parallel conveyor stage primitive.
    easy to use - only derive exec_stage() in first stage and exec_stage(arg) in subsequent stages.
    Last stage is iterable - each time it gets last stage result. One iteration occurs across all
    stages in parallel. The very first iteration will take longer, as the conveyor must fill first.
    Stage objects should be used under 'with' execution semantics for auto-close and threads cleanup.
    Don't touch anything else.
    @attribute batch - batch no
    @attribute done - is there more data
    @attribute depth - stage no counting from head
    @attribute last_result - previous batch result (to be used by the next stage on this batch)
    @attribute cv - conditional var protecting go and abort flags
    @attribute go - stage must go or wait
    @attribute abort - stage must abort
    @attribute thread - thread associated with this stage
    @attribute exception - exception occured on this stage (if any)
    @attribute parent - upstream StageExec
    """
    def __init__(self, parent=None):
        self.batch = 0
        self.done = False
        self.last_result = None
        self.cv = threading.Condition()
        self.go = False
        self.abort = False
        self.exception = None
        self.parent = parent
        if parent:
            self.depth = parent.depth+1
        else:
            self.depth = 0
        self.thread = threading.Thread(target=self.run)
        self.thread.start()
    def __enter__(self):
        return self
    def __exit__(self, _ex_type, _ex, _traceback):
        self.close()
    def __thread_wait_go(self):
        try:
            self.cv.acquire()
            while not self.go:
                self.cv.wait()
        finally:
            self.cv.release()
    def __thread_notify_go(self):
        try:
            self.cv.acquire()
            self.go = True
            self.cv.notify()
        finally:
            self.cv.release()
    def __thread_notify_abort(self):
        try:
            self.cv.acquire()
            self.go = True
            self.abort = True
            self.cv.notify()
        finally:
            self.cv.release()
    def __thread_wait_done(self):
        try:
            self.cv.acquire()
            while self.go:
                self.cv.wait()
        finally:
            self.cv.release()
    def __thread_notify_done(self):
        try:
            self.cv.acquire()
            self.go = False
            self.cv.notify()
        finally:
            self.cv.release()
    def close(self):
        if self.parent and self.parent.thread:
            self.parent.close()
        self.__thread_notify_abort()
        self.thread.join()
        self.thread = None
        self.last_result = None
        self.__thread_notify_done()
    # to iterate again
    def __rewind(self):
        if self.parent:
            self.parent.__rewind()	 # pylint: disable=protected-access
        self.done = self.go = False
        self.exception = self.last_result = None
        self.batch = 0
    # loop until there is no more input
    def run(self): # pylint: disable=protected-access
        parent_last_result = None
        try:
            while not self.abort:
                while not self.done:
                    if self.abort:
                        break
                    self.__thread_wait_go()
                    if self.abort:
                        break
                    if self.parent:
                        if not self.parent.done:
                            self.parent.__thread_notify_go() # pylint: disable=protected-access
                            # idle until conveyor is filled
                            if self.batch >= self.depth:
                                self.last_result = self.exec_stage(parent_last_result)
                            self.parent.__thread_wait_done() # pylint: disable=protected-access
                            parent_last_result = self.parent.last_result
                            if self.parent.exception:
                                raise self.parent.exception
                        else:
                            self.last_result = None
                            self.done = True
                    else:
                        self.last_result = self.exec_stage()
                        self.done = (self.last_result is None)
                    self.batch += 1
                    self.__thread_notify_done()
                else:
                    self.__thread_wait_go()       # wait for rewind or explicit close
        except Exception as ex:
            frameinfo = getframeinfo(currentframe())
            print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno)
            self.exception = ex
            self.abort = True
            self.done = True
            self.__thread_notify_done()
    # iterate the conveyor - advance all stages per one step
    def __iter__(self):
        self.__rewind()
        batch = self.batch
        depth = self.depth
        done = self.done
        last_result = self.last_result
        while not done:
            self.__thread_notify_go()
            if batch > depth:
                yield last_result
            self.__thread_wait_done()
            if self.exception:
                raise self.exception
            batch = self.batch
            depth = self.depth
            done = self.done
            last_result = self.last_result
    # must override
    def exec_stage(self, parent_result=None):
        raise NotImplementedError


class MspFileWorker(ParallelExec, object):
    """
    Base class to process a fixed batch of MspFiles in parallel
    Just override a single file processing method
    @attribute conf - conf file dictionary
    @attribute msp_files - fixed-size vector of MspFiles to process in one step
    """
    def __init__(self, conf, batch_size):
        super(MspFileWorker, self).__init__(batch_size)
        self.conf = conf
        self.msp_files = None
    def exec_slice(self, sliceno):
        if len(self.msp_files) > sliceno:
            self.process_msp_file(self.msp_files[sliceno])
    def parallel_process_msp_files(self, msp_files):
        """
        Shard the execution into multiple exec_slices()
        @param msp_files - vector of MspFiles you need to process in parallel
        """
        self.msp_files = msp_files
        self.exec_batch()
        return self.msp_files
    def process_msp_file(self, mfile):
        """
        Override me: what do you want to do with one MspFile
        """
        raise NotImplementedError

class MspDownloadWorker(MspFileWorker, object):
    """
    Download MspFile
    """
    def __init__(self, conf, batch_size):
        super(MspDownloadWorker, self).__init__(conf, batch_size)
    def process_msp_file(self, mfile):
        logging.info("downloading: {0}:{1}/{2}/{3} as {4}".format(mfile.msp_host, mfile.msp_dir, mfile.msp_subdir, mfile.fname, os.path.join(TMP_DIR, mfile.fname)))
        msp_transmit_start_time = time.time()
        try:
            sftp_download(mfile.msp_host, os.path.join(mfile.msp_dir, mfile.msp_subdir), TMP_DIR, mfile.fname)
        except BsaSftpException as ex:
            frameinfo = getframeinfo(currentframe())
            print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
            logging.warn("first downloading attempt failed for: {0}:{1}/{2}/{3} with error {4}".format(mfile.msp_host, mfile.msp_dir, mfile.msp_subdir, mfile.fname, str(ex)))
            sftp_download(mfile.msp_host, os.path.join(mfile.msp_dir, mfile.msp_subdir), TMP_DIR, mfile.fname)
        msp_transmit_end_time = time.time()
        mfile.stat.update({
            "msp_host": mfile.msp_host,
            "msp_dir": os.path.join(mfile.msp_dir, mfile.msp_subdir),
            "msp_file": mfile.fname,
            "msp_transmit_start": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(msp_transmit_start_time)),
            "msp_transmit_end": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(msp_transmit_end_time)),
        })

def rename_bdmsp_file(mfile, rsuffix):
    """
    Perform the destination file renaming step. We'll do it by augmenting mspfile.bdmsp_fname
    field by attaching a desired suffix and performing filesystem link() command. The said link
    will be removed later in clear_msp_files() and ordered_commit().
    The file name will be changed to include a specified suffix before .gz.smime.der
    @param mfile - MspFile instance
    @param rsuffix - the desired suffix to be included
    """
    if mfile.bdmsp_fname:
        new_bdmsp_fname = insert_string_before_file_type(mfile.bdmsp_fname, rsuffix)
        os.link(os.path.join(TMP_DIR, mfile.bdmsp_fname), os.path.join(TMP_DIR, new_bdmsp_fname))
        mfile.bdmsp_fname = new_bdmsp_fname

def listdir_fullpaths(kdir):
    """
    list files with their fullpaths in the given directory
    @param kdir - given directory
    @returns - kdir contents as fullpaths
    """
    return [os.path.join(kdir, x) for x in os.listdir(kdir)]


def list_cert_files(conf):
    """
    Get encryption certs list from conf file. If conf file has encryption_keys_dir it will be listed
    @param conf - config file as dictionary
    @returns - list of cert files to use for encryption
    """
    res = []
    if "encrypt_cert_dir" in conf:
        res += listdir_fullpaths(conf["encrypt_cert_dir"])
    if "encrypt_cert_files" in conf:
        res += conf["encrypt_cert_files"]
    if "sscert" in conf:
        # for validating decryption step
        res += [conf["sscert"]]
    logging.info('list_cert_files returning {}'.format(res))
    return res

# encrypt and collect stats, such as md5sum and record counts
class MspEncryptAndStatWorker(MspFileWorker, object):
    """
    Encrypt MspFile
    """
    def __init__(self, conf, batch_size):
        super(MspEncryptAndStatWorker, self).__init__(conf, batch_size)

    def process_msp_file(self, mfile):
        # original data md5sum
        msp_stat = os.stat(os.path.join(TMP_DIR, mfile.fname))
        with Md5Sum(os.path.join(TMP_DIR, mfile.fname)) as md5sum_exec:
            msp_md5sum = md5sum_exec.get()
            mfile.msp_md5sum = msp_md5sum
        cmds = []
        # optional convertion
        conv_sfx = ""
        fname = mfile.fname
        convert_exec_match = "no_such_command_to_match"
        if "convert" in self.conf:
            conf_conv = self.conf["convert"]
            conv_cmd = [conf_conv["command"]]
            convert_exec_match = conf_conv["command"]
            if "format" in conf_conv:
                conv_cmd += [conf_conv["format"]]
            conv_cmd += conf_conv["fields"]
            fname = drop_filename_extension(fname, [".proto.gz", ".csv.gz", ".gz"])[0]
            cmds += [["gzip", "-cd"], conv_cmd, ["gzip", "-c"]]
            conv_sfx = ".csv.gz"
        if "encrypt_cert_files" in self.conf or "encrypt_cert_dir" in self.conf:
            cmds += [["openssl", "smime", "-inform", "PEM", "-binary", "-aes-256-cbc", "-outform", "DER", "-encrypt"] + list_cert_files(self.conf)]
            conv_sfx += ".smime.der"
        if cmds:
            mfile.fenc = fname + conv_sfx
            input_file = os.path.join(TMP_DIR, mfile.fname)
            output_file = os.path.join(TMP_DIR, mfile.fenc)
            try:
                with open(input_file, "r") as fin:
                    with open(output_file, "w") as fout:
                        wait_pipe(exec_pipe(fin, fout, cmds, timeout=OPENSSL_TIMEOUT))
            except BsaExecException as ex:
                frameinfo = getframeinfo(currentframe())
                print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                # we specificlly screen errors in gzip -cd | msp2txt section of the execution pipe
                # those will be treated as warnings and files discarded
                # errors in any other place of the pipe is still considered as critical failure
                logging.warn("got pipe execution error while converting {0}: {1}".format(input_file, str(ex)))
                for (cmd, _, ecode) in ex.cmds:
                    if ecode and not (re.match("gzip -cd", cmd) or re.match(convert_exec_match, cmd)):
                        raise # if not gunzip or converter error re-raise
                logging.warn("failed to unzip or convert {0}".format(input_file))
                mfile.is_corrupt = True
            if not mfile.is_corrupt:
                logging.info("converted/encrypted {0} into {1}".format(input_file, output_file))
                if "sskey" in self.conf:
                    # validate that we can decrypt the file using our tmp key
                    with open(output_file, "r") as fenc:
                        with open("/dev/null", "w") as dev_null_w:
                            wait_pipe(exec_pipe(fenc, dev_null_w, [["openssl", "smime", "-inform", "DER", "-binary", "-decrypt", "-inkey", self.conf['sskey'], "-passin", "env:PASSENV"]], timeout=OPENSSL_TIMEOUT))
                    logging.info("validated decryption for {0}".format(output_file))
                mfile.bdmsp_fname = mfile.fenc
                bdmsp_stat = os.stat(os.path.join(TMP_DIR, mfile.fenc))
                with Md5Sum(os.path.join(TMP_DIR, mfile.fenc)) as md5sum_exec:
                    bdmsp_md5sum = md5sum_exec.get()
        else:
            mfile.bdmsp_fname = mfile.fname
            bdmsp_stat = msp_stat
            bdmsp_md5sum = msp_md5sum

        if mfile.msp_zone.filename_append:
            # we must add specified suffix
            rename_bdmsp_file(mfile, "_" + mfile.msp_zone.filename_append)
        mfile.stat.update({
            "msp_mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(msp_stat.st_mtime)),
            "msp_sz": msp_stat.st_size,
            "msp_md5sum": msp_md5sum
        })
        if not mfile.is_corrupt:
            mfile.stat.update({
                "bdmsp_file": mfile.bdmsp_fname,
                "bdmsp_mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(bdmsp_stat.st_mtime)),
                "bdmsp_sz": bdmsp_stat.st_size,
                "bdmsp_md5sum": bdmsp_md5sum,
            })
        else:
            mfile.stat.update({"is_corrupt": True})

class MspBsaStreamWorker(MspFileWorker, object):
    """
    Upload MSP files to BSA channels
    """
    def __init__(self, conf, batch_size, maxage=15*60):
        """
        @param maxage - file age in seconds, so older files don't go into BSA Streams
        """
        super(MspBsaStreamWorker, self).__init__(conf, batch_size)
        self.maxage = maxage
    def process_msp_file(self, mfile):
        if mfile.bsa_host and mfile.bsa_dir and not mfile.is_corrupt:
            try:
                # bsa streaming interface - write out to the MSP_RAW stream
                # create metadata for bsa stream interface
                msp_stat = os.stat(os.path.join(TMP_DIR, mfile.fname))
                msp_mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(msp_stat.st_mtime))
                no_older = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() - self.maxage))
                if msp_mtime < no_older:
                    logging.warn("MSP file {0} is too old to go into BSA channel. Skipping.".format(mfile.fname))
                    return
                metadata = {
                    'filename': mfile.fname,
                    'size': mfile.stat["msp_sz"],
                    'zone': mfile.msp_host,
                    'md5_hexdigest': mfile.stat["msp_md5sum"],
                    'mtime': int(msp_stat.st_mtime)
                }
                entries = mfile.fname.split('_')
                if len(entries) > 3:
                    traffic_server = entries[2]
                    timestamp = entries[3]
                    metadata.update({
                        'timestamp': timestamp,
                        'ts': traffic_server,
                    })
                metastr = json.dumps(metadata)
                fname = mfile.fname
                scp_upload(mfile.bsa_host, mfile.bsa_dir, TMP_DIR, fname)
                mtdt_fname = fname+".meta"
                with open(os.path.join(TMP_DIR, mtdt_fname), "w") as mtdt_file:
                    print(metastr, file=mtdt_file)
                scp_upload(mfile.bsa_host, mfile.bsa_dir, TMP_DIR, mtdt_fname)
                os.unlink(os.path.join(TMP_DIR, mtdt_fname))
                mfile.bsa_fname = fname
                mfile.stat.update({
                    "bsa_host": str(mfile.bsa_host),
                    "bsa_dir": str(mfile.bsa_dir),
                    "bsa_file": str(mfile.bsa_fname)
                })
            except BsaSftpException as ex:
                frameinfo = getframeinfo(currentframe())
                print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                logging.warn("BSA streaming upload error: {0}".format(str(ex)))


class MspKafkaUploadWorker(MspFileWorker, object):
    """
    Publish file to kafka
    """
    def __init__(self, conf, batch_size, kprod):
        super(MspKafkaUploadWorker, self).__init__(conf, batch_size)
        self.kprod = kprod

    def process_msp_file(self, mfile):
        '''
        Sends mfile to kafka broker. This is the standardized function
        stub required by the conveyor.
        '''
        if mfile.is_corrupt:
            return
        kafka_transmit_start_time = time.time()

        if  mfile.msp_zone.filename_append:
            filename = insert_string_before_file_type(mfile.fname, '_' + mfile.msp_zone.filename_append)
        else:
            filename = mfile.fname

        key_dict = {'file' : filename, 'host' : mfile.msp_host, 'dir' : os.path.join(mfile.msp_dir, mfile.msp_subdir)}
        infile_name = os.path.join(TMP_DIR, mfile.fname)

        with open(infile_name, 'r') as infile:
            try:
                value = infile.read()
                key_dict['size'] = mfile.stat["msp_sz"]
                key_dict['md5sum'] = mfile.msp_md5sum
                key_dict['modtime'] = unix_time_to_yyyymmddhhmmss(os.stat(infile_name).st_mtime)
                key = json.dumps(key_dict)
                logging.info("sending: {} to kafka broker which is {} bytes".format(
                    key, len(value)))
                try:
                    partition, offset = self.kprod.send(key, value)
                except Exception as exep:
                    frameinfo = getframeinfo(currentframe())
                    print_traceback(exep.__class__.__name__, frameinfo.filename, frameinfo.lineno)
                    msg = "send to kafka failed with exception, infile_name={} size={} exception={}".format(
                          infile_name, len(value), str(exep))
                    logging.error(msg)
                    raise
                kafka_transmit_end_time = time.time()
                kafka_transmit_time = kafka_transmit_end_time - kafka_transmit_start_time
                logging.info('kafka required {} seconds to send {} partition = {} offset = {}'.\
                     format(kafka_transmit_time, key, partition, offset))

                mfile.stat.update({
                    "kafka_transmit_start": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(kafka_transmit_start_time)),
                    "kafka_transmit_end": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(kafka_transmit_end_time)),
                    "kafka_part": partition,
                    "kafka_offset": offset
                })
            except Exception as exception:
                frameinfo = getframeinfo(currentframe())
                print_traceback(exception.__class__.__name__, frameinfo.filename, frameinfo.lineno)
                raise

class MspUploadWorker(MspFileWorker, object):
    """
    Upload to BDMSP
    """
    def __init__(self, conf, batch_size):
        super(MspUploadWorker, self).__init__(conf, batch_size)
    def process_msp_file(self, mfile):
        if self.conf["no_bdmsp"] or mfile.is_corrupt:
            return
        bdmsp_transmit_start_time = time.time()
        # updating dst file timestamp to match src file timestamp
        msp_stat = os.stat(os.path.join(TMP_DIR, mfile.fname))
        os.utime(os.path.join(TMP_DIR, mfile.bdmsp_fname), (msp_stat.st_atime, msp_stat.st_mtime))
        logging.info("uploading: {0} into temporary file {1}:{2}/{3}.tmp".format(os.path.join(TMP_DIR, mfile.bdmsp_fname), mfile.bdmsp_host, mfile.bdmsp_dir, mfile.bdmsp_fname))
        try:
            scp_upload(mfile.bdmsp_host, mfile.bdmsp_dir, TMP_DIR, mfile.bdmsp_fname)
        except BsaSftpException as ex:
            frameinfo = getframeinfo(currentframe())
            print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
            logging.warn("first uploading attempt failed: {0} into temporary file {1}:{2}/{3}.tmp with error {4}".format(os.path.join(TMP_DIR, mfile.bdmsp_fname), mfile.bdmsp_host, mfile.bdmsp_dir, mfile.bdmsp_fname, str(ex)))
            scp_upload(mfile.bdmsp_host, mfile.bdmsp_dir, TMP_DIR, mfile.bdmsp_fname)
        bdmsp_transmit_end_time = time.time()
        mfile.stat.update({
            "bdmsp_transmit_start": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(bdmsp_transmit_start_time)),
            "bdmsp_transmit_end": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(bdmsp_transmit_end_time))
        })

class MspBeginStage(StageExec, object):
    """
    Conveyor head producing vector of MspFiles with upto batch_size elements (for later stages parallel processing)
    @attribute msp_files - big list of MspFiles to process
    @attribute batch_size - batch the given msp_files in buckets
    @attribute msp_files_in_conveyor - to maintain the set of the files currently in the conveyor (for cleanup)
    """
    def __init__(self, batch_size, msp_files_in_conveyor):
        super(MspBeginStage, self).__init__()
        self.msp_files = None
        self.batch_size = batch_size
        self.msp_files_in_conveyor = msp_files_in_conveyor
    def exec_stage(self, parent_result=None):
        if len(self.msp_files) > 0:
            nxt_batch = self.msp_files[:self.batch_size]
            self.msp_files = self.msp_files[self.batch_size:]
            for mfile in nxt_batch:
                mfile.stat.update({"start": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))})
                self.msp_files_in_conveyor[mfile.seq_num] = mfile	# atomic
                logging.info("start: {0}:{1}/{2}/{3}".format(mfile.msp_host, mfile.msp_dir, mfile.msp_subdir, mfile.fname))
            return nxt_batch
        else:
            return None
    def begin_working_on_files(self, msp_files):
        self.msp_files = msp_files

class MspWorkerStage(StageExec, object):
    """
    Adapter from conveyor stage to parallel worker to use the worker as a conveyor element
    @attribute worker - instance of MspFileWorker
    """
    def __init__(self, parent, worker):
        """
        @attribute worker - parallel MspFileWorker you want to use at this stage
        """
        super(MspWorkerStage, self).__init__(parent)
        self.worker = worker
    def exec_stage(self, parent_result=None):
        return self.worker.parallel_process_msp_files(parent_result)

class MspFwdDaemon(object):
    """
    Main forwarder daemon as a self-cleaning class
    The MspFiles conveyour pipeline must already be pre-built and given as parameter
    @attribute conf - mspfwd.yaml conf file as dictionary
    @attribute zk - zookeeper connection
    @attribute instance - uniq instance UUID kept in Zookeeper
    @attribute fwd_conveyor_head - MspFiles processing conveyor head
    @attribute fwd_conveyor - MspFiles processing conveyor tail
    @attribute seq_num - next file sequence number
    @attribute msp_files_in_conveyor - to maintain the set of the files currently in the conveyor (for cleanup)
    @attribute hostname - `hostname`
    @attribute prom_exp_path - Prometheus export file name for applcatoin metrics
    @attribute prom_registry - collection of Prometheus metrics
    @attribute prom_counter - Prometheus counter: the total number of files processed
    @attribute prom_duration - Prometheus histogram: the processing duration time of each file forwarded
    @attribute prom_msp_age - Prometheus histogram: MSP file ages
    @attribute prom_msp_size - Prometheus histogram: MSP file sizes
    @attribute zk_shutdown - becomes True when Zk session was lost - must shutdown in this case
    """
    def __init__(self, conf, fwd_conveyor_head, fwd_conveyor, msp_files_in_conveyor, name):
        """
        @param conf - mspfwd.yaml conf file as dictionary
        @param fwd_conveyor_head - MspFiles processing conveyor head
        @param fwd_conveyor - MspFiles processing conveyor tail
        @param msp_files_in_conveyor - to maintain the set of the files currently in the conveyor (for cleanup)
        @param name - uniquely specifies this instance of mspfwd for reporting prometheus metrics
        """
        self.conf = conf
        self.zk = None
        self.instance = None
        self.fwd_conveyor_head = fwd_conveyor_head
        self.fwd_conveyor = fwd_conveyor
        self.seq_num = 0
        self.msp_files_in_conveyor = msp_files_in_conveyor
        self.name = name
        self.hostname = socket.gethostname()
        self.prom_exp_path = TMP_DIR + "/prom_metrics/mspfwd_metrics.prom"
        # Linter doesn't understand candidly decorated prometheus-client classes
        # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
        self.prom_registry = prom.CollectorRegistry()
        self.prom_counter = \
            prom.Counter("BSA_files_forwarded",
                         "The number of files are forwarded",
                         ["msp_host", "name"],
                         registry=self.prom_registry)
        self.prom_duration = \
            prom.Histogram("BSA_mspfwd_duration",
                           "The time duration of forwawrding process (sec)",
                           ["msp_host", "name"],
                           registry=self.prom_registry,
                           buckets=(1, 2, 3, 5, 15, 30, 60, 120, 180, 300, 600))
        self.prom_msp_age = \
            prom.Histogram("BSA_msp_age",
                           "MSP file ages when they reach BDMSP (sec)",
                           ["msp_host", "name"],
                           registry=self.prom_registry,
                           buckets=(60, 120, 180, 300, 600, 1200, 1800, 3600, 3600*2, 3600*3))
        meg = 1024*1024
        self.prom_msp_size = \
            prom.Histogram("BSA_msp_size",
                           "MSP file sizes (bytes)",
                           ["msp_host", "name"],
                           registry=self.prom_registry,
                           buckets=(1*meg, 2*meg, 3*meg, 5*meg, 10*meg, 20*meg,
                                    30*meg, 40*meg, 50*meg, 60*meg, 70*meg,
                                    100*meg, 200*meg, 300*meg, 500*meg))
        # pylint: enable=unexpected-keyword-arg,no-value-for-parameter
        self.zk_shutdown = False
        self.sskey = None
        self.sscert = None

    def zk_event_listener(self, state):
        """
        Zookeper connection listener - must initiate shutdown on a single connection loss
        """
        if state == kazoo.client.KazooState.LOST or state == kazoo.client.KazooState.SUSPENDED:
            logging.warn("Zookeeper connection lost, must shutdown instance {0}...".format(self.instance))
            self.zk_shutdown = True
    def __enter__(self):
        """
        If needed render disposal encryption key, Connect to Zookeeper and create instance lock
        Also create prometheus metrics dir
        """
        self.instance = str(uuid.uuid4())
        logging.info("Starting instance {0}".format(self.instance))
        if VALIDATE_DECRYPTION and list_cert_files(self.conf):
            # create disposable encryption cert and key with random password
            # password is kept in the env var and is not visible to anybody
            os.environ["PASSENV"] = str(uuid.uuid4())
            self.sscert = os.path.join(TMP_DIR, "ss-{0}.crt".format(self.instance))
            self.sskey = os.path.join(TMP_DIR, "ss-{0}.key".format(self.instance))
            ecode = subprocess_call(["openssl", "req", "-new", "-x509", "-keyout", self.sskey, "-out", self.sscert, "-days", "3650", "-subj", "/CN=mspfwd-tmp-{0}/C=US/ST=NJ/L=Bedminster/O=AT&T/OU=BDCOE".format(self.instance), "-passout", "env:PASSENV"])
            if ecode != 0:
                raise BsaException("openssl exit code = {0} - could not render temporary encryption key {1} and certificate {2}".format(ecode, self.sskey, self.sscert))
            os.chmod(self.sskey, 0600)
            self.conf['sscert'] = self.sscert # this will enable decryption validation
            self.conf['sskey'] = self.sskey
            logging.info("Created temporary {0} {1} encryption key/cert pair".format(self.sskey, self.sscert))
        zklst = ",".join([str(x) for x in self.conf["zk"]])
        # We must extend the timeout. The default 10 seems to be too tight in busy locations.
        # When timeout occurs the Zk tends to reconnect, but at the same time is causes
        # ephemeral locks to release. So you don't want to timeout. Or if you did then exit immediately
        # to avoid duplicate files processing
        self.zk = kazoo.client.KazooClient(hosts=zklst, timeout=ZOO_TIMEOUT)
        self.zk.start()
        self.zk.add_listener(self.zk_event_listener)
        self.zk.create(ZKROOT+"/instances/"+self.instance, ephemeral=True, makepath=True)
        if not os.path.exists(TMP_DIR + "/prom_metrics"):
            os.mkdir(TMP_DIR + "/prom_metrics")
        return self
    def __exit__(self, _ex_type, _ex, _traceback):
        """
        Release instance lock and disconnect from Zookeeper
        """
        if self.sskey and CLEANUP_AT_SHUTDOWN:
            try:
                os.unlink(self.sskey)
            except OSError as ex:
                frameinfo = getframeinfo(currentframe())
                print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                pass	# file may not be there yet or already be deleted
        if self.sscert and CLEANUP_AT_SHUTDOWN:
            try:
                os.unlink(self.sscert)
            except OSError as ex:
                frameinfo = getframeinfo(currentframe())
                print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                pass	# file may not be there yet or already be deleted
        self.zk.remove_listener(self.zk_event_listener)
        logging.warn("Shutting down instance {0}...".format(self.instance))
        self.zk.delete(ZKROOT+"/instances/"+self.instance)
        self.zk.stop()
        self.zk.close()

    def bdmsp_rr_dst(self, seq_num, blacklist):
        """
        Round-robin the BDMSP destination host/dir for the msp file number seq_num
        Avoid using bdmsp destinations from blacklist
        """
        bdmsp_host_dir = [hd for hd in self.conf["bdmsp"] if hd["host"] not in blacklist]
        bdmsp_sz = len(bdmsp_host_dir)
        if bdmsp_sz < 1:
            raise BsaException("All BDMSP destinations are blacklisted. Cannot continue.")
        bdmsp_host = bdmsp_host_dir[seq_num % bdmsp_sz]["host"]
        bdmsp_dir = bdmsp_host_dir[seq_num % bdmsp_sz]["dir"]
        return (bdmsp_host, bdmsp_dir)


    def latest_msp_files(self, msp_zone, limit, blacklist):
        """
        Compose the list of MspFiles available at msp adm server
        Consider only those files that we didn't work on yet, those won't be present in Zookeeper storage
        @param msp_zone - MspZone object describing the zone with (host, dir, is_glob, bsa_host, bsa_dir) attributes
        @param limit - compose a list of no more than so many files (< 0 mean no limit)
        @param blacklist - list of "bad" sftp locations that we want to avoid using
        """
        def wrap_get_children(adir):
            rv = zk_get_children(self.zk, adir)
            print('zk_get_children({}) => {}'.format(adir, rv))
            sys.stdout.flush()
            return rv

        def wrap_exists(astr):
            rv = self.zk.exists(astr)
            print('zk.exists({}) => {}'.format(astr, rv))
            sys.stdout.flush()
            return rv

        def wrap_bdmsp_rr_dst(seq_num, blacklist):
            rv =  self.bdmsp_rr_dst(seq_num, blacklist)
            print('bdmsp_rr_dst({}, {}) => {}'.format(seq_num, blacklist, rv))
            sys.stdout.flush()
            return rv

        latest_files, self.seq_num = MspFile.latest_msp_files( msp_zone, limit, blacklist, sftp_available_files, wrap_get_children,
            wrap_exists, self.seq_num, wrap_bdmsp_rr_dst, logging)
        return latest_files


    def ordered_commit(self, msp_files, dlock_dv, dlock):
        """
        Need to commit MspFiles sequentially at the very end of processing pipeline
        @param msp_files - batch of MspFiles to commit
        @param dlock - zk lock file for msp zone directory
        @param dlock_dv - zk lock file content as a dictionary
        """
        for mfile in msp_files:
            start_t = time.mktime(time.strptime(mfile.stat['start'], "%Y-%m-%d %H:%M:%S"))
            msp_mtime_t = time.mktime(time.strptime(mfile.stat['msp_mtime'], "%Y-%m-%d %H:%M:%S"))
            # for every file we're about to commit check if zk session lost
            # if so shutdown gracefully by throwing ZkLostException
            if self.zk_shutdown:
                raise ZkLostException("Lost Zookeeper connection")
            #killme()	# DEBUG - recovery doesn't find broken files
            if not self.conf["no_bdmsp"] and not mfile.is_corrupt:
                mfile.stat.update({
                    "node": self.hostname,
                    "instance": self.instance,
                    "state": "ready",
                    "path": mfile.bdmsp_host+":"+os.path.join(mfile.bdmsp_dir, mfile.bdmsp_fname),
                    "end": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
                })
            else:
                mfile.stat.update({
                    "node": self.hostname,
                    "instance": self.instance,
                    "state": "corrupt",
                    "end": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
                })
            # memorize the current file we work on at the moment in the dlock file
            # for potential recovery
            dlock_dv.update({"flock": mfile.flock})
            self.zk.set(dlock, value=json.dumps(dlock_dv, indent=2))
            #killme()	# DEBUG - recovery doesn't find broken files
            self.zk.create(mfile.flock, value=json.dumps(mfile.stat, indent=2), makepath=True)
            #killme()	# DEBUG - recovery should clean this file
            if not self.conf["no_bdmsp"] and not mfile.is_corrupt:
                try:
                    sftp_commit_upload(mfile.bdmsp_host, mfile.bdmsp_dir, mfile.bdmsp_fname)
                except BsaSftpException as ex:
                    frameinfo = getframeinfo(currentframe())
                    print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                    logging.warn("first commit attempt failed for: {0}:{1}/{2}/{3} into {4}:{5}/{6}".format(mfile.msp_host, mfile.msp_dir, mfile.msp_subdir, mfile.fname, mfile.bdmsp_host, mfile.bdmsp_dir, mfile.bdmsp_fname))
                    sftp_commit_upload(mfile.bdmsp_host, mfile.bdmsp_dir, mfile.bdmsp_fname)
                #killme()	# DEBUG - recovery should mark this file 'done' successfully
                logging.info("commited: {0}:{1}/{2}/{3} into {4}:{5}/{6}".format(mfile.msp_host, mfile.msp_dir, mfile.msp_subdir, mfile.fname, mfile.bdmsp_host, mfile.bdmsp_dir, mfile.bdmsp_fname))
            else:
                if mfile.is_corrupt:
                    logging.warn("discarded: {0}:{1}/{2}/{3} as corrupt".format(mfile.msp_host, mfile.msp_dir, mfile.msp_subdir, mfile.fname))
            # processing time for each file
            self.prom_duration.labels(mfile.msp_host, self.name).observe(time.time() - start_t)
            # msp file ages
            self.prom_msp_age.labels(mfile.msp_host, self.name).observe(time.time() - msp_mtime_t)
            # msp file sizes
            self.prom_msp_size.labels(mfile.msp_host, self.name).observe(mfile.stat['msp_sz'])
            # incrementing prometheus metrics conunter and exporting it right away
            self.prom_counter.labels(msp_host=mfile.msp_host, name=self.name).inc()
            if not self.conf["no_bdmsp"]:
                # don't update prom stats when no_bdmsp
                prom.write_to_textfile(self.prom_exp_path, self.prom_registry)
            # augmenting the zk entry as final step
            del mfile.stat["instance"]
            mfile.stat.update({
                "state": ("done", "corrupt")[mfile.is_corrupt],
                "end": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time()))
            })
            self.zk.set(mfile.flock, value=json.dumps(mfile.stat, indent=2))
            #killme()	# DEBUG - recovery doesn't find broken files
            if mfile.bsa_fname:
                # publishing into bsa channel
                # bsa_fh will be None if there was an error with bsa_fh earlier
                try:
                    sftp_commit_upload(mfile.bsa_host, mfile.bsa_dir, mfile.bsa_fname)
                    sftp_commit_upload(mfile.bsa_host, mfile.bsa_dir, mfile.bsa_fname+".meta")
                    logging.info("bsa_populated: {0}:{1}/{2}/{3} into {4}:{5}/{6}".format(mfile.msp_host, mfile.msp_dir, mfile.msp_subdir, mfile.fname, mfile.bsa_host, mfile.bsa_dir, mfile.bsa_fname))
                except BsaSftpException as ex:
                    frameinfo = getframeinfo(currentframe())
                    print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                    logging.warn("BSA Channel population error {0}: {1}".format(mfile.fname, str(ex)))
            os.unlink(os.path.join(TMP_DIR, mfile.fname))
            if mfile.fenc:
                os.unlink(os.path.join(TMP_DIR, mfile.fenc))
            if mfile.bdmsp_fname and mfile.bdmsp_fname != mfile.fname and mfile.bdmsp_fname != mfile.fenc:
                os.unlink(os.path.join(TMP_DIR, mfile.bdmsp_fname))
            del self.msp_files_in_conveyor[mfile.seq_num]
            logging.info("end: {0}:{1}/{2}/{3}".format(mfile.msp_host, mfile.msp_dir, mfile.msp_subdir, mfile.fname))

    def run(self):
        """
        Run forwarding daemon
        """
        try:
            while True:
                blacklist = sftp_blacklist(self.zk)	# re-read blacklist
                mspzones = MspZone.msp_zones(self.conf, ZKROOT)
                if len(blacklist) == len(mspzones):
                    logging.warn('All host are blacklisted')
                else:
                    for msp_zone in mspzones:
                        if self.zk_shutdown: # for every new batch check if zk session closed and if so shutdown gracefully
                            raise ZkLostException("Lost Zookeeper connection")
                        if msp_zone.host in blacklist: # ignore blacklisted hosts
                            continue
                        # try obtaining dlock - an exclusive lock on msp zone (msp_host, msp_dir)
                        dlock = msp_zone.zkdir+"/lock"
                        dlock_dv = {"instance": self.instance}
                        try:
                            if self.zk.exists(dlock):	# do not abuse NodeExistsException
                                continue
                            self.zk.create(dlock, value=json.dumps(dlock_dv, indent=2), makepath=True)
                        except kazoo.exceptions.NodeExistsError as ex:
                            continue
                        lst = self.latest_msp_files(msp_zone, -1, blacklist)
                        # start parallel conveyor on latest files
                        self.fwd_conveyor_head.begin_working_on_files(lst)
                        for msp_files in self.fwd_conveyor:
                            # MspFiles will be coming out of processing conveyor batch by batch and
                            # we commit them sequentially
                            self.ordered_commit(msp_files, dlock_dv, dlock)
                        self.zk.delete(dlock)
                gc.collect()
                time.sleep(30)
        except BsaSftpException as ex:
            frameinfo = getframeinfo(currentframe())
            print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno)
            logging.error("Sftp error: {0}".format(str(ex)), exc_info=True)
            logging.error("Making a note of sftp error for host {0}".format(ex.host))
            sftp_mark_bad_host(self.zk, ex) # mark bad sftp host and die anyway
            return -1 # make sure we exit with error code
        return 0

def daemon_exec(conf, msp_files_in_conveyor):
    """
    Start forwarding daemon
    @param conf - mspfwd.yaml conf file in the form of dictionary
    @param msp_files_in_conveyor - set to support actual set of files in the middle of processing
    @return exit code
    """
    batch_size = conf.get("batch_size", 4)
    closeable = []
    workers = []
    main_exit_code = -1
    try:
        ssh_master = SshMaster(conf)
        closeable = [ssh_master]
        ssh_master.open() # may partially fail
        # workers will never fail to start
        workers = [MspDownloadWorker(conf, batch_size),
                   MspEncryptAndStatWorker(conf, batch_size),
                   MspUploadWorker(conf, batch_size)]
        if "output_streams" in conf:
            workers.append(MspBsaStreamWorker(conf, batch_size))
        if "kafka" in conf:
            kprod = KafkaProducer(conf['kafka'])
            closeable.append(kprod.open()) # may fail
            workers.append(MspKafkaUploadWorker(conf, batch_size, kprod))
        # creating stages will never fail
        fwd_conveyor_head = MspBeginStage(batch_size, msp_files_in_conveyor)
        fwd_conveyor = fwd_conveyor_head
        for w in workers:
            # pylint: disable=redefined-variable-type
            fwd_conveyor = MspWorkerStage(fwd_conveyor, w)
        closeable.append(fwd_conveyor)
        with MspFwdDaemon(conf, fwd_conveyor_head, fwd_conveyor,
                          msp_files_in_conveyor, conf["name"]) as daemon:
            main_exit_code = daemon.run()
    finally:
        for cleanup in reversed(closeable+workers):
            try:
                cleanup.close()
            except Exception as ex:
                frameinfo = getframeinfo(currentframe())
                print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                logging.error("unhandled exception", exc_info=True)
    return main_exit_code

class ZkSession(object):
    """
    Zk Connection as a 'with' object for automated cleanup.
    Optionally you may try to acquare a global application lock
    @attribute zk - zookeeper connection
    """
    def __init__(self, zklst, conf):
        """
        @param zklst - list of zk servers to connect to as a comma-separate string
        """
        self.zk = None
        self.zklst = zklst
        self.globlock = None
        self.zoo_timeout = conf.get('zoo_timeout', ZOO_TIMEOUT)
    def __enter__(self):
        self.zk = kazoo.client.KazooClient(hosts=self.zklst, timeout=self.zoo_timeout)
        zk_started = False
        for _ in range(3):
            try:
                self.zk.start()
                zk_started = True
                break
            except Exception as xcptn:
                time.sleep(30)
        if not zk_started:
            raise xcptn
        return self
    def __exit__(self, _ex_type, _ex, _traceback):
        if self.globlock:
            self.release_globlock()
        self.zk.stop()
        self.zk.close()
        self.zk = None
    def acquare_globlock(self, flock=GLOBAL_LOCK_NAME):
        """
        Try to acquare global lock in no-wait mode and return either true or false
        """
        try:
            self.zk.create(ZKROOT+"/"+flock, ephemeral=True, makepath=True)
            self.globlock = flock
            result = True
        except kazoo.exceptions.NodeExistsError as ex:
            result = False
        return result
    def release_globlock(self):
        """
        Release global lock.
        """
        self.zk.delete(ZKROOT+"/"+self.globlock)
        self.globlock = None

def recover_one_flock(zk, flock, inst):
    """
    Verify flock if the given file was indeed transmitted. Connect to remote BDMSP server for validation. Clear the lock.
    @param zk - connection
    @param flock - file lock in zk
    @param inst - forwarder instance number for logging purposes
    @returns boolean indicator telling if the file needed fixing
    """
    was_fixed = False
    (flock_data, _) = zk.get(flock)
    try:
        flock_dv = json.loads(flock_data)
        state = flock_dv["state"]
        if state == "ready":
            logging.warn("Detected ready file {0} left by instance {1}. Clearing".format(flock, inst))
            # path example: didm02:/lz/02/msp/ingest/in/TrafficEventLog_akrnz03msp4ts05_190_20180913152023822_15379.chr.gz.smime.der
            re_host_path = re.match("([^:]+):(.*)", flock_dv["path"])
            if not re_host_path:
                logging.error("File {0} is in unknown state: {1}. Cleaning the lock, but you must fix the file manually".format(flock, flock_data))
            bdmsp_host = re_host_path.group(1)
            bdmsp_path = re_host_path.group(2)
            re_dir_file = re.match("^(.*[^/]*)/([^/]*)$", bdmsp_path)
            if not re_dir_file:
                logging.error("File {0} is in unknown ready state: {1}. Cleaning the lock, but you must fix the file manually".format(flock, flock_data))
            bdmsp_dir = re_dir_file.group(1)
            bdmsp_file = re_dir_file.group(2)
            if sftp_is_uploading(bdmsp_host, bdmsp_dir, bdmsp_file):
                logging.warn("File {0}:{1}/{2} was not commited yet. Clearing".format(bdmsp_host, bdmsp_dir, bdmsp_file))
                zk.delete(flock)
            else:
                logging.warn("File {0}:{1}/{2} was already commited. Marking it 'done'".format(bdmsp_host, bdmsp_dir, bdmsp_file))
                flock_dv["state"] = "done"
                zk.set(flock, value=json.dumps(flock_dv, indent=2))
                was_fixed = True
    except (ValueError, KeyError):
        logging.error("File {0} is in unknown state: {1}. Cleaning the lock, but you must fix the file manually".format(flock, flock_data))
    return was_fixed

def recover_exec(conf, zk):
    """
    Look for dangling lock files in our zookeeper repository and fix them.
    Scan existing msp directories (using dlock files in zookeeper)
    and find those that don't have a valid running mspfwd instance running. If so, inspect if the last
    transmitted file in that directory and either commit or revert it.
    @param conf - mspfwd.yaml conf file as dictionary
    @param zk - zookeeper connection
    """
    logging.info("start looking for broken files")
    instances = set()
    try:
        instances = set(zk.get_children(ZKROOT+"/instances"))
    except kazoo.exceptions.NoNodeError as ex:
        pass
    nfix = 0
    for msp_zone in MspZone.msp_zones(conf, ZKROOT):
        dlock = msp_zone.zkdir+"/lock"
        inst = None
        flock = None
        try:
            (dlock_data, _) = zk.get(dlock)
            dlock_dv = json.loads(dlock_data)
            if "flock" in dlock_dv:
                flock = dlock_dv["flock"]
            inst = dlock_dv["instance"]
        except kazoo.exceptions.NoNodeError as ex:
            continue	# nobody works this dir
        except (ValueError, KeyError) as ex:
            raise BsaException("Directory lock file {0} is in unknown format: {1}. Fix manually immediately".format(dlock, dlock_data))
        if inst in instances:
            logging.info("Lock {0} belongs to active instance {1}. Checking next lock...".format(dlock, inst))
            continue
        # fix the directory with stale lock
        logging.warn("Detected dangling directory lock {0} left by instance {1}. Cleaning...".format(dlock, inst))
        if flock and zk.exists(flock) and recover_one_flock(zk, flock, inst):
            nfix += 1
        zk.delete(dlock)
    if nfix > 0:
        logging.warn("{0} file(s) fixed".format(nfix))
    logging.info("end looking for broken files")
    return 0

def isync_exec(conf, zk, msp_host):
    """
    Populate initial list of MSP files into zookeeper db.
    This step is required to avoid downloading entire historical archive from MSP.
    @attribute conf - mspfwd.yaml conf file as dictionary
    @attribute zk - zookeeper connection
    @attribute msp_host - if we need to handle only one msp_zone
    """
    # acquare a global lock to run the sync procedure
    logging.info("start syncing files list")
    for msp_zone in MspZone.msp_zones(conf, ZKROOT):
        if msp_host and msp_zone.host != msp_host:
            continue
        if msp_zone.is_glob:
            dates = [sd for sd in sftp_available_files(msp_zone.host, msp_zone.dir, sys.maxsize) if re.match("[0-9]{6}", sd)]
            subdirs = dates	# sync all dates
        else:
            subdirs = ['']
        for sd in subdirs:
            logging.info("Syncning initial files list from {0}:{1}/{2}".format(msp_zone.host, msp_zone.dir, sd))
            zkfiles = []
            try:
                zkfiles = zk_get_children(zk, msp_zone.zkdir+"/"+sd)
            except kazoo.exceptions.NoNodeError as ex:
                pass # the todays dir may not be in zk yet
            files = [f for f in sftp_available_files(msp_zone.host, msp_zone.dir+"/"+sd, sys.maxsize) if re.match(msp_zone.filename_match, f)]

            for fname in set(files) - set(zkfiles):
                flock = msp_zone.zkdir+"/"+sd+"/"+fname
                try:
                    zk.create(flock, value='{"state": "done"}', makepath=True)
                    logging.info("File: {0}:{1}/{2}/{3} marked done".format(msp_zone.host, msp_zone.dir, sd, fname))
                    continue
                except kazoo.exceptions.NodeExistsError as ex:
                    pass
    logging.info("end syncing files list")
    return 0

def cleanup_exec(conf, zk, ndays):
    """
    Clean old files from ZK storage. Files older than ndays that don't exist on MSP will be deleted.
    @param conf - mspfwd.yaml conf file as dictionary
    @param zk - zookeeper connection
    @param ndays - how many whole days do we keep the files for
    """
    time_now = time.time()
    logging.info("starting cleaning old files")
    for msp_zone in MspZone.msp_zones(conf, ZKROOT):
        if msp_zone.is_glob:
            subdirs = sorted([sd for sd in zk.get_children(msp_zone.zkdir) if re.match("[0-9]{6}", sd)]) # consider only valid dir names, ignore "lock" file
        else:
            subdirs = ['']
        for sd in subdirs:
            removed_files_cnt = 0
            zkfiles = zk_get_children(zk, msp_zone.zkdir+"/"+sd)
            # make sure that msp_host,msp_dir is accessible before deciding to delete all obsolete zk files
            sftp_available_files(msp_zone.host, msp_zone.dir, 0)
            mspfiles = []
            try:
                mspfiles = [f for f in sftp_available_files(msp_zone.host, msp_zone.dir+"/"+sd, sys.maxsize) if re.match(msp_zone.filename_match, f)]
            except BsaSftpException as ex:
                frameinfo = getframeinfo(currentframe())
                print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                logging.info("Sftp directory: {0}:{1}/{2} does not exist".format(msp_zone.host, msp_zone.dir, sd))
            zkfiles_to_remove = set(zkfiles) - set(mspfiles)
            for fname in zkfiles_to_remove:
                flock = msp_zone.zkdir+"/"+sd+"/"+fname
                (_, stat) = zk.get(flock)
                fage = (time_now-stat.last_modified)/3600/24
                if fage > ndays:
                    zk.delete(flock)
                    removed_files_cnt += 1
            if removed_files_cnt > 0:
                logging.info("Removed {0} files from zk directory: {1}:{2}/{3}".format(removed_files_cnt, msp_zone.host, msp_zone.dir, sd))
            if sd:
                (_, stat) = zk.get(msp_zone.zkdir+"/"+sd)
                if stat.children_count > 0:
                    logging.info("Keeping {0} files in zk directory: {1}:{2}/{3}".format(stat.children_count, msp_zone.host, msp_zone.dir, sd))
                else:
                    zk.delete(msp_zone.zkdir+"/"+sd)
                    logging.info("Directory: {0}:{1}/{2} removed from zk".format(msp_zone.host, msp_zone.dir, sd))
        logging.info("end cleaning old files")
    return 0


def blacklist_evaluate_new_errors(zk, nodes_sz, time_now_t, maxage_t):
    """
    Check if any sftp host gathered enough errors to be demoted into blacklist
    @param zk - connection
    @param nodes_sz - number of forwarder nodes participating
    @param time_now_t - time now
    @param maxage_t - trash errors older than that
    """
    bad_hosts = []
    try:
        sftp_error_files = zk.get_children(ZKROOT+SFTP_ERRORS)
        for sef in sftp_error_files:
            (jstr, zstat) = zk.get(ZKROOT+SFTP_ERRORS+"/"+sef)
            if time_now_t - zstat.last_modified > maxage_t:
                zk.delete(ZKROOT+SFTP_ERRORS+"/"+sef)
            else:
                dv = json.loads(jstr)
                host = dv["host"]
                node = dv["node"]
                bad_hosts.append((host, node))
        for host in set([h for (h, _) in bad_hosts]):
            cnt = len(set([n for (h, n) in bad_hosts if h == host]))
            bl_host = ZKROOT+SFTP_BLACKLIST+"/"+host
            if cnt >= (nodes_sz+1)/2 and not zk.exists(bl_host):
                logging.error("Sftp host {0} had errors from more than 50% nodes in last {1} minutes. Moving {0} into black list".format(host, maxage_t/60))
                zk.create(bl_host, makepath=True)
    except kazoo.exceptions.NoNodeError as ex:
        frameinfo = getframeinfo(currentframe())
        print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
        pass

def blacklist_host_was_fixed(host, remote_dir):
    """
    Check if the remote host was already fixed and we can do dir listing now
    @param host - sftp host name
    @param remote_dir - remote dir we need to list
    """
    necho = 0
    try:
        with Sftp(host, ["cd "+remote_dir, "ls -1"]) as sftp:
            echo = sftp.stdout.readline()
            while echo:
                necho += 1
                echo = sftp.stdout.readline()
    except BsaSftpException as ex:
        frameinfo = getframeinfo(currentframe())
        print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
        necho = 0
    return necho > 1

def blacklist_recheck_old(conf, zk, time_now_t, maxage_t):
    """
    Check if a problematic hosts were fixed and may be now removed from blacklist. Keep generating errors for others.
    We validate by trying to do sftp directory listing
    @param conf - mspfwd.yaml conf file as dictionary
    @param zk - connection
    @param time_now_t - current time
    @param maxage_t - don't bother re-checking hosts younger than maxage_t
    """
    rc_hosts = dict()	# blacklisted hosts we want to re-check and clear when True
    try:
        blacklist = zk.get_children(ZKROOT+SFTP_BLACKLIST)
        for host in blacklist:
            (_, zstat) = zk.get(ZKROOT+SFTP_BLACKLIST+"/"+host)
            age_t = time_now_t - zstat.last_modified
            if age_t > maxage_t:
                rc_hosts[host] = True
    except kazoo.exceptions.NoNodeError as ex:
        frameinfo = getframeinfo(currentframe())
        print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
        pass
    for conf_sect in ["msp", "bdmsp"]:
        for host_dir in conf[conf_sect]:
            host = host_dir["host"]
            (remote_dir, _) = MspZone.dir_truncate_wildcard(host_dir["dir"])	# truncate /* wildcard suffix from dir name
            if host in rc_hosts:
                rc_hosts[host] &= blacklist_host_was_fixed(host, remote_dir)
    for (host, was_fixed) in rc_hosts.iteritems():
        if was_fixed:
            logging.warn("Sftp host {0} connectivity was fixed. Removing it from black list".format(host))
            zk.delete(ZKROOT+SFTP_BLACKLIST+"/"+host)
        else:
            logging.error("Sftp host {0} connectivity was not fixed yet!".format(host))

def blacklist_exec(conf, zk, nodes_sz):
    """
    Perform routine blacklist management:
    1) Check if any sftp host gathered enough errors to be demoted into blacklist
    2) Check if a problematic hosts were fixed and may be now removed from blacklist.
       Keep generating errors for others.
       We validate by trying to do sftp directory listing
    @param conf - mspfwd.yaml conf file as dictionary
    @param zk - connection
    @param nodes_sz - number of forwarder nodes participating
    """
    time_now = time.time()
    logging.info("start checking blacklisted sftp hosts")
    blacklist_evaluate_new_errors(zk, nodes_sz, time_now, 60*10) # ignore errors older than 10 min
    blacklist_recheck_old(conf, zk, time_now, 60*60) # don't re-check bl entries younger than 1 hour
    logging.info("end checking blacklisted sftp hosts")
    return 0

def blclear_exec(zk):
    """
    Unconditionally clear blacklist
    @param zk - connection
    """
    logging.info("start cleaning blacklist")
    try:
        blacklist = zk.get_children(ZKROOT+SFTP_BLACKLIST)
        for host in blacklist:
            logging.warn("Removing sftp host {0} from blacklist".format(host))
            zk.delete(ZKROOT+SFTP_BLACKLIST+"/"+host)
        logging.info("end cleaning blacklist")
    except kazoo.exceptions.NoNodeError as ex:
        frameinfo = getframeinfo(currentframe())
        print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
        pass
    return 0

def clear_msp_files(msp_files_in_conveyor):
    """
    Remove all msp files that may be in the middle of processing upon exit or shutdown
    @param msp_files_in_conveyor - dictionary of files to clean
    """
    if CLEANUP_AT_SHUTDOWN:
        for k in msp_files_in_conveyor.keys():
            mfile = msp_files_in_conveyor[k]
            try:
                os.unlink(os.path.join(TMP_DIR, mfile.fname))
                if mfile.fenc:
                    os.unlink(os.path.join(TMP_DIR, mfile.fenc))
                if mfile.bdmsp_fname:
                    os.unlink(os.path.join(TMP_DIR, mfile.bdmsp_fname))
            except OSError as ex:
                frameinfo = getframeinfo(currentframe())
                print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
                pass	# file may not be there yet or already be deleted


def cleanup_shm():
    os.system('rm /dev/shm/*.crt')
    os.system('rm /dev/shm/*.key')
    os.system('rm /dev/shm/*.gz')
    os.system('rm /dev/shm/*.der')
    os.system('rm /dev/shm/*.proto')

#
# main
#
def main(argv):
    global TMP_DIR
    conf_file = CONF_FILE
    main_exit_code = 0
    cmd = argv[0]
    os.environ["PATH"] += ":/opt/bsa/bin:/opt/bsa/setup"
    msp_files_in_conveyor = {}
    last_signal = MemorizeLastSignal(signal.SIGTERM, signal.SIGINT)
    try:
        opts, args = getopt.getopt(argv[1:], "c:h", ["config=", "help"])
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage(cmd, conf_file)
                return 0
            elif opt in ("-c", "--config"):
                conf_file = arg
        conf = None
        with open(conf_file) as cfile:
            conf = yaml.load(cfile)
        err = validate_and_enrich_conf(conf, conf_file)
        if err:
            raise BsaException(conf_file+" configuration file problem "+err)
        do_bsa_logging = conf.get('bsa_logging', False)
        log_level = conf.get('log_level', 'INFO')
        init_logger(do_bsa_logging, log_level)
        if "tmp_dir" in conf:
            TMP_DIR = conf["tmp_dir"]
        zklst = ",".join([str(x) for x in conf["zk"]]) # list of zk servers as comma-separate str
        zklst_sz = len(conf["zk"])
        if len(args) == 1 and args[0] == "recover":
            with ZkSession(zklst, conf) as zk_session:
                if zk_session.acquare_globlock():
                    main_exit_code = recover_exec(conf, zk_session.zk)
                else:
                    logging.info("Cannot proceed with zk recovery. Some other process is holding global lock")
        elif len(args) > 0 and args[0] == "isync":
            with ZkSession(zklst, conf) as zk_session:
                if zk_session.acquare_globlock():
                    if len(args) > 1:
                        main_exit_code = isync_exec(conf, zk_session.zk, args[1])
                    else:
                        main_exit_code = isync_exec(conf, zk_session.zk, None)
                else:
                    logging.info("Cannot proceed with initial synchronization. Some other process is holding global lock")
        elif len(args) == 1 and args[0] == "cleanup":
            with ZkSession(zklst, conf) as zk_session:
                if zk_session.acquare_globlock():
                    main_exit_code = cleanup_exec(conf, zk_session.zk, 14)
                else:
                    logging.info("Cannot proceed with old files cleanup. Some other process is holding global lock")
        elif len(args) == 1 and args[0] == "blacklist":
            with ZkSession(zklst, conf) as zk_session:
                if zk_session.acquare_globlock():
                    main_exit_code = blacklist_exec(conf, zk_session.zk, zklst_sz)
                else:
                    logging.info("Cannot proceed with blacklist re-evaluation. Some other process is holding global lock")
        elif len(args) == 1 and args[0] in ["blclear", "blclean", "blacklist_clear"]:
            with ZkSession(zklst, conf) as zk_session:
                if zk_session.acquare_globlock():
                    main_exit_code = blclear_exec(zk_session.zk)
                else:
                    logging.info("Cannot proceed with clearing blacklist. Some other process is holding global lock")
        elif len(args) == 0:
            # start as fwd daemon
            if do_bsa_logging:
                start_bsactl_heartbeat(30)
            cleanup_shm()
            main_exit_code = daemon_exec(conf, msp_files_in_conveyor)
        else:
            logging.error("invoked with incorrect arguments: {0}".format(" ".join(args)))
            main_exit_code = -3
    except getopt.GetoptError as err:
        frameinfo = getframeinfo(currentframe())
        print_traceback(err.__class__.__name__, frameinfo.filename, frameinfo.lineno)
        logging.error("incorrect option: {0}".format(err)) # will print something like "option -a not recognized"
        usage(cmd, conf_file)
        main_exit_code = -2
    except KeyboardInterrupt:	# normal deamon shutdown
        pass
    except ZkLostException as ex:	# graceful shutdown on zk connection loss
        frameinfo = getframeinfo(currentframe())
        print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
        pass
    except Exception as ex:
        if ex.__class__.__name__ == 'SessionExpiredError':
            logging.warn('SessionExpiredError was caught, if this is a dev machine, zookeeper data files are probably too large')
            logging.warn('Increase zoo_timeout in the config to fix this problem. This works for both prod and mtlab.')
            logging.warn('In mtlab, you can run purge_zookeeper_dev.sh to delete the large zookeeper data files')
        frameinfo = getframeinfo(currentframe())
        print_traceback(ex.__class__.__name__, frameinfo.filename, frameinfo.lineno, logging.DEBUG)
        if not last_signal.signum:
            # throwing error not applicable on ^C
            logging.error("Unhandled exception", exc_info=True)
            main_exit_code = -1
    clear_msp_files(msp_files_in_conveyor)
    return main_exit_code

if __name__ == "__main__":
    sys.exit(main(sys.argv))
