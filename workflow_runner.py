#!/usr/bin/env python
"""
@version 1.1
@lastupdate 2013-10
@author Nate Vogel, nvogel@fmpub.net

@license GPLv2, federated media publishing, www.federatedmedia.net

    This file is part of Data-Services-Tools.

    Data-Services-Tools is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Data-Services-Tools is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Data-Services-Tools.  If not, see <http://www.gnu.org/licenses/>.

Purpose:
    This application executes a sequence of commands that make up a workflow.
    Any command failure terminates the workflow.

A workflow command sequence is defined in a config file ini
consisting of numbered directives.  e.g. command one and two:
    1 = ls /
    2 = echo "%%(cmd_1_output)s" | grep "something"
    3 = date +_PERC_Y_PERC_m

Usage Notes:
    * You can use a command output in subsequent commands, e.g. %%(cmd_1_output)s
    * Other convenience variables available, %%(now)s, %%(YrMn)s, %%(YrMnDy)s, %%(curdate)s
    * To use '%' character in commands, substitute '_PERC_' or '%%%%'.
"""
import sys
import datetime
import subprocess
from dslib import DSConfig, ConfigException

zookeeper_bin = '/usr/local/bin/zk'

def print_usage():
    print "Workflow Runner v1.1"
    print "Usage: workflow_runner.py <full path to workflow config ini> <starting command num, default 1>"
    print "e.g. ./workflow_runner.py /opt/datasrv/share/bin/config/WF-reporting-current_balances.ini"

def run_cmd(full_cmd):
    query_proc = subprocess.Popen(full_cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
    return_code = query_proc.wait()
    result_value, err = query_proc.communicate()
    if err or return_code != 0:
        raise Exception("%s\nUnexpected return code or value: %d, %s" % (result_value, return_code, err))
    else:
        return result_value.strip()

def prepare_cmd(raw_cmd, convenience_variables):
    """replaces variables and special characters in a "raw cmd" string"""
    try:
        cmd_substituted = (raw_cmd % convenience_variables)
    except KeyError, emsg:
        raise WorkflowException(command_num, "Required variable not found during execution %s" % emsg, convenience_variables)
    return cmd_substituted.replace('_PERC_', '%')

def get_workflow_cmd_num(starting_cmd_num, workflow_config):
    """builds up then generates the list of commands to execute.
    prepends optional config directive always_run_cmds list of commands to execute 
    regardless of specified starting command num.""" 
    cmd_num_list = []
    if starting_cmd_num > 1: 
        try:
            cmd_num_list = [int(x) for x in workflow_config.get('always_run_cmds', 'list')]
        except ConfigException:
            pass

    cmd_num = starting_cmd_num
    while True:
        if workflow_config.has_option(str(cmd_num)):
            if cmd_num not in cmd_num_list:
                cmd_num_list.append(cmd_num)
            cmd_num+=1
        else:
            # no more commands in config file
            break      

    if len(cmd_num_list) == 0:
        raise WFConfigException("No workflow commands found.")

    cmd_num_list.sort()
    for cmd_num in cmd_num_list:
        yield cmd_num

class WorkflowRuntime(object):
    def __init__(self, report_runtime):
        self._report_runtime = report_runtime
        self._runtimes = {}
    def now(self):
        return datetime.datetime.now()
    def track(self, thing):
        if self._report_runtime:
            self._runtimes[thing] = self.now()
    def report(self, thing, formatted=True):
        if thing in self._runtimes:
            return str(self.now() - self._runtimes[thing])
        else:
            return False

class WorkflowException(Exception):
    """This exception class handles whenever a command exits unexpectedly, ending a workflow prematurely.
    It is responsible for running the clean up commands (when configured)."""
    def __init__(self, command_num, emsg, convenience_vars):
        try:
            # execute optional cleanup command 
            cleanup_cmd = prepare_cmd(workflow_config.get('premature_end_cmd'), convenience_vars)
            print "#Premature workflow end command executed ...\n%s" % run_cmd(cleanup_cmd)
        except ConfigException:
            pass
        sys.exit("Workflow ended prematurely, #%d command error: %s" % (command_num, emsg))

class WFConfigException(ConfigException):
    def __init__(self, emsg):
        sys.exit(emsg)

"""MAIN"""
if __name__ == '__main__':

    """Parameter validation and WF config setup"""
    if len(sys.argv) > 1 and len(sys.argv) < 4:
        # workflow config passed in as first parameter
        starting_command_num = 1
        workflow_config_filename = sys.argv[1]
        try:
            workflow_config = DSConfig('workflow_config', environment=workflow_config_filename)
        except Exception, e:
            # EXIT condition: config init failed
            raise WFConfigException(str(e))
        if len(sys.argv) == 3:
            # starting command num specified, second parameter
            starting_command_num = int(sys.argv[2])
    else:
        # EXIT condition: invalid parameter count 
        raise WFConfigException(print_usage())    

    """optional WF config directive, max_num_chars_cmd_output
       number of characters to output from stdout per command
       default: ALL"""
    try:
        num_chars_to_log = int(workflow_config.get('max_num_chars_cmd_output'))
    except (ValueError, ConfigException):
        num_chars_to_log = None
        pass

    """Run time reporter"""
    report_runtime = True
    try:
        report_runtime = workflow_config.get('report_runtimes')
        if report_runtime.lower() == 'false':
            report_runtime = False
    except ConfigException:
        pass
    workflow_runtime = WorkflowRuntime(report_runtime)

    """General convenience variables to be used in workflows"""
    now = workflow_runtime.now()
    convenience_variables = {'now': now, 
                             'YrMn': now.strftime("%Y%m"),
                             'YrMnDy': now.strftime("%Y%m%d"),
                             'curdate': now.strftime("%Y-%m-%d")}

    """Zookeeper stuff"""
    try:
        workflow_zk_queue_path = workflow_config.get('zk_queue_path')
    except ConfigException:
        # EXIT condition
        raise WFConfigException("ZK queue path directive not found in config file (can be empty but must be defined)")

    # no workflow zk (queue) path means we just run as is
    # i.e. workflow defines its own parameters, e.g. date agnostic
    if len(workflow_zk_queue_path):
        try:
            # print "Reading from %s" % workflow_zk_queue_path
            zk_queue_value = run_cmd("%s qpoll %s" % (zookeeper_bin, workflow_zk_queue_path))
            convenience_variables['zk_queue_value'] = zk_queue_value
        except Exception:
            raise WFConfigException("Nothing in the workflow queue '%s'" % workflow_zk_queue_path)

    """time to go to work!"""
    workflow_runtime.track('workflow')
    for command_num in get_workflow_cmd_num(starting_command_num, workflow_config):

        raw_command = workflow_config.get(str(command_num))
        command = prepare_cmd(raw_command, convenience_variables)
        print "#%d >> %s ..." % (command_num, command[0:num_chars_to_log])

        try:
            workflow_runtime.track('cmd_%d' % command_num)
            cmd_output = run_cmd(command)
            runtime = workflow_runtime.report('cmd_%d' % command_num)
            # return cmd output and runtimes
            print cmd_output[0:num_chars_to_log]
            if runtime:
                print "#%d >> runtime: %s" % (command_num, runtime)
        except Exception, emsg:
            raise WorkflowException(command_num, emsg, convenience_variables)

        # save output
        convenience_variables['cmd_%d_output' % command_num] = cmd_output

    """workflow done, no errors returned during cmd sequence"""
    print "Workflow completed at %s" % now
    wf_runtime = workflow_runtime.report('workflow')
    if wf_runtime:
        print ">> runtime: %s" % wf_runtime
