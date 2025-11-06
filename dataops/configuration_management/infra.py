#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess
from src.utils import bcolors

#conectivity, disk, memory, load
class Infra:
    def checkDiskSpace(self,server):
        ssh = subprocess.Popen(["ssh", "%s" % server, "df -h"],
                       shell=False,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            raise Exception("No result returned in disk space verification")
        else:
            return result
    
    def checkMemory(self,server):
        ssh = subprocess.Popen(["ssh", "%s" % server, "cat /proc/meminfo | grep Mem"],
                       shell=False,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            raise Exception("No result returned in memory verification")
        else:
            return result
    
    def checkLoad(self,server):
        ssh = subprocess.Popen(["ssh", "%s" % server, "cat /proc/loadavg | awk  '{print $1}'"],
                       shell=False,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            raise Exception("No result returned in load verification")
        else:
            return result
    
    def checkSsh(self,server1,server2):
        if server1 == "localhost":
            ssh = subprocess.Popen(["ssh", "-o BatchMode=yes", "%s" % server2, "echo ok 2>&1"],
                           shell=False,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
            result = ssh.stdout.readlines()
        else:
            ssh = subprocess.Popen(["ssh", server1, "ssh", "-o BatchMode=yes", "%s" % server2, "echo ok 2>&1"],
                           shell=False,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
            result = ssh.stdout.readlines()

        if result == []:
            return 0
        else:
            if result[0] == "ok\n":
                return 1
            else:
                return 0
    
    def checkRotate(self,filepath,server):
        ssh = subprocess.Popen(["ssh", "%s" % server, "data=$(ls -lh "+filepath+" | awk '{print $6\" \"$7\" \"$8}'); f=$(date -d \"$data\" +%s);hour=$(date +%s -d\"-1 hour\"); if [ $f -ge $hour ]; then echo \"OK\t$data\";else echo \"NOT OK\t$data\";fi"],
                       shell=False,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        if result == []:
            raise Exception("No result returned in rotate verification")
        else:
            return result
    
    def basicCheck(self, server, prefix):
        print "\n"+prefix+"["+server+"] -- Disk Space"
        for l in self.checkDiskSpace(server):
            print prefix+"\t"+l,
            
        print "\n"+prefix+"["+server+"] -- Memory Usage"
        for l in self.checkMemory(server):
            print prefix+ "\t"+l,
        
        print "\n"+prefix+"["+server+"] -- Load"
        for l in self.checkLoad(server):
            print prefix+ "\t"+l,
        
