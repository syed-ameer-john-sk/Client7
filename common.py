import sys 
import os 
import shutil
import subprocess
from log import Log 

#######
# LOG #
#######
def set_log(log_path) -> None:
	global logger 
	logger = Log("Workflow", os.path.dirname(log_path))
	sys.stderr = logger.set_log("stderr")

def get_logger() -> Log:
	return logger 

########
# EXIT # 
########
def simple_exit(*msgs) -> None: 
	for msg in msgs:
		logger.log_event("error,terminal", msg)
	exit_workflow() 

def exit_workflow() -> None:
	logger.log_event("error,terminal", "Job not submitted")
	exit(1)

#########
# SHELL # 
#########
def run_command(cmd, exit_after_error=True) -> str:
	logger.log_event("info", "Running command: " + cmd)
	stdout, stderr = subprocess.Popen(cmd, 
									  stdout=subprocess.PIPE,
									  stderr=subprocess.PIPE, 
									  shell=True, 
									  universal_newlines=True).communicate()
	if stderr != "":
		logger.log_event("error,terminal", stderr)
		if exit_after_error: 
			exit_workflow() 
	return stdout 

#########
# FILES #
#########

def copy_file(src, dst, error_message, ignore=False):
	if src == dst:
		logger.log_event("warning,terminal", 
						 "Cannot copy the same path {0}".format(src))  
		return 
	if os.path.exists(dst): 
		if ignore:
			return 
		simple_exit("Cannot copy file - Destination file already exist {0}".format(dst)) 
	try:
		shutil.copy(src, dst)	
	except Exception as e: 
		simple_exit(error_message, e)

def make_dir(path, error_message): 
	try:	
		os.makedirs(path)
	except Exception as e:
		simple_exit(error_message, e)

def rename(src, dst, error_message):
	if src == dst:
		logger.log_event("warning,terminal", 
						 "Cannot rename the same path {0}".format(src)) 
		return 
	if os.path.exists(dst):
		simple_exit("Cannot rename path - Destination path already exist {0}".format(dst)) 
	try:
		os.rename(src, dst)
	except Exception as e:	
		simple_exit(error_message, e) 

def get_basename(path):
	return os.path.basename(path) 
	
def get_previous_step(step):
	if step == "RUN": return "PRE"
	if step == "POST": return "RUN" 
	return None 

def set_mode_bits(path, mode=0o777):
	if os.path.exists(path):
		os.chmod(path, mode)