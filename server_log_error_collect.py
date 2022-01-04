#!/usr/bin/env python
#Script neeeds an input file server_log_error.yaml which has server details and string pattern
#It will ssh to various servers and search for string pattern provided in the yaml file
#default path location: /var/log/mspfwd.log
#command to run the script ./server_log_error_collect.py -f server_log_error.yaml
#If user need to change the path location they need to pass "-l path location" 
#change of path command: ./server_log_error_collect.py -f server_log_error.yaml -l "path"
#Script will capture one occurance of search pattern in the log file and it also calculate 
#number of times log pattern is repeated in the log. Once the details are collected it will
#send email to the distibution list.

import subprocess
import csv
import argparse
import logging
import yaml
import sys
logging.basicConfig(filename='server_data_collect.log',
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)




EMAIL_STR = ''
LOG = ''
haszero = False
Email = ''

class Error(Exception):
    """Base class for other exceptions"""
    pass

class SSHCommandExecError(Error):
    """Raised when the Command Execution Fails over SSH"""
    pass

class runServerCmd:
    def __init__(self, host):
        self.host = host
        
    def exec_cmd(self, cmd):
        resp = subprocess.Popen(["ssh", "%s" % self.host, cmd], 
                         shell=False, 
                         stdout=subprocess.PIPE, 
                         stderr=subprocess.PIPE)
        result, err = resp.communicate()

        if err and 'Warning' not in err.decode('utf-8'):
            logging.info(err.decode('utf-8'))
            global LOG
            LOG += "ERROR LOG: " + err.decode('utf-8').strip() + '\n'
            raise SSHCommandExecError
        return result.decode('utf-8')


class serverlogCollect:
    def __init__(self, host, string, email, sdir, outfile):
        self.host = host
        self.outfile = outfile
        self.sdir = sdir
        self.string = string
        self.email = email
        self.runcommand = runServerCmd(self.host)
        self.result = {}
        global Email
        Email = ",".join(self.email)
        
    def run(self):
        try:
            self.runcommand.exec_cmd('ls')
        except SSHCommandExecError:
            return
        # command to grep 
        global EMAIL_STR
        global haszero
        resp = ' ' 
        cmd = " grep -m1 " + '"' + self.string + '"' + " " + self.sdir 
        resp += ' ' + self.runcommand.exec_cmd(cmd) 
        cmd = "  grep " + '"' +  self.string + '"' +  " " + self.sdir + "| wc -l" 
        resp += ' ' + self.runcommand.exec_cmd(cmd) 
        if resp:
            EMAIL_STR += self.host + resp + '\n'
            if ((len(str(resp).strip())) > 1):
                #If there is any email string in the log, haszero will set it to true.
                haszero=True
    
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    #ap.add_argument("-e", "--Email", required=True)
    ap.add_argument("-f", "--yamlfile", required=True, help="yaml file input")
    ap.add_argument("-l", "--logfilepath", required=False, help="log file", default="/var/log/mspfwd.log")
    args = vars(ap.parse_args())
            
    with open(args['yamlfile'], "r") as stream:
        try:
            data = yaml.safe_load(stream)
            print(data)
            for item in data:
                if 'email' in item: break
                for i in data[item]:
                    slc = serverlogCollect(i['host'], i['string'], i['email'], args['logfilepath'], args['yamlfile'])
                    slc.run()
        except yaml.YAMLError as exc:
            print(exc)

    while True:
        if (haszero == False):
            #Email string is zero, if there are any Logs for SSH connection it will send email.
            if ((len(str(LOG).strip())) > 25):
                cmd="""echo """+ '"' + LOG + '"' + """  | mailx -s 'Error Detail' """ + '"' + Email + '"' + """ """
                p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
                output, errors = p.communicate()
                break
            else:
                #If there are no Email string and SSH connection logs. No Email will be sent.
                sys.exit()
        elif (haszero == True):
            #Email string is available check for the ssh connection log available. 
            if ((len(str(LOG).strip())) > 25):
                #Both are available, send both email string ssh logs. 
                cmd="""echo """+ '"' + EMAIL_STR + '"' + '"' + LOG + '"' + """  | mailx -s 'Error Detail' """ + '"' + Email + '"' """ """
                p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
                output, errors = p.communicate()
                break
            else:
                #Only Email string is available, will send only Email Sting no SSH logs
                cmd="""echo """+ '"' + EMAIL_STR + '"' + """  | mailx -s 'Error Detail' """ + '"' + Email + '"' """ """
                p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
                output, errors = p.communicate()
                break
        else:
            sys.exit()
