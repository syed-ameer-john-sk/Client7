from datetime import datetime
import os
from classes import*
from common import*

def set_log(copylogger : Log ):
	global logger 
	logger = copylogger

def set_cron_job(P: Project): 
	script = os.path.join(
		os.path.dirname(__file__), 
        Uniq.job_state
	)
	args = []
	for job in P.jobs: 
		args.append("\"{0},{1}\"".format(os.path.basename(job.path), job.id)) 
	cmd = "bash {0} --add \"{1}\" {2}".format(script, P.run_dir, " ".join(args))
	logger.log_event("info,terminal", "cron cmd: {0}".format(cmd)) 
	run_command(cmd)

def set_workflow_config():
    # Uniq.workflow_config can now be an absolute path (set by tests) or a relative path.
    if os.path.isabs(Uniq.workflow_config):
        config_file = Uniq.workflow_config
    else:
        # Original behavior: relative to the directory of core.py
        config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), Uniq.workflow_config)

    if not os.path.exists(config_file):
        # This check is important if Uniq.workflow_config was relative and couldn't be found.
        # Or if an absolute path was given but is incorrect.
        simple_exit(f"Workflow configuration file not found: {config_file}")

    config = WorkflowConfig()
    parsed_sections = ConfigParser(config_file).get_all_sections()
    if not parsed_sections:
        # Handle case where config file is empty or has no valid sections leading to empty dict
        simple_exit(f"Workflow configuration file {config_file} is empty or contains no valid sections.")
    config.set_members(parsed_sections)
    Uniq.config = config

def set_system_global_env():
	cmd = ". {0} && env".format(Uniq.config.global_env)
	stdout = run_command(cmd)
	env = dict((line.split("=", 1) for line in stdout.splitlines() if "=" in line)) # Ensure split is possible
	try:
		os.environ.update(env)
	except Exception as e: 
		simple_exit("Cannot update environment variables", e)
		
def generate_or_copy_table(table, WA, P):
    # Always update/copy table, regardless of Work_step (ALL, RUN POST, POST)
    # -- Critical Update: No check for work_step; always run table update --
    if table in ["Ext_Aero_Cfx", "Ext_Aero_CFZ", "Ext_Aero_Residuals"]:
        update_table(table, WA, P)
    else:
        # Your previous logic for other tables (if any)
        pass

def update_table(table, WA, P):
    # Actual table update logic as per your workflow
    # Replace with your implementation, for example using pandas/sql/etc
    print(f"Updating table {table} for workstep with args {WA}, project {P}")
    # ... Place table copying/creation logic here ...

def get_hold_job_id(P: Project): 
	if P.jobs[0].software.sim:
		return None 
	cmd = "bash \"{0}\" -w \"{1}\" \"{2}\"".format(
		os.path.join( 
			os.path.dirname(os.path.abspath(__file__)), 
			Uniq.job_state
		), 
		P.run_dir, 
		get_previous_step(Uniq.steps[0]))  
	output = run_command(cmd)
	logger.log_event("info,terminal", "INFO - BASH -W OUTPUT: {0}".format(output)) 
	if "STOP" in output:
		simple_exit("Previous job error")
	lines = output.split("\n") 
	if len(lines) > 1 and lines[1] == "":
		return None 
	if not Regex.match(lines[1], Regex.numbers):
		logger.log_event("info,terminal", "INFO - Cannot retrieve previous job id")
		return None 
	return lines[1] 


# ignore None job path 
# count nb folders in run_dir, if match len(workflow_steps) rename possible 
# 
# visitor design patterns 
# rename save infos about what folders can be renamed 
# no need to check rerun conditions or others 
# check if visitor is better or just addAtExit infos directly without visitor design patterns 
# 
"""
def rename_to_delete(project: Project): 




	# run_dir handler 

	# dir handler 
		rename(src, dst, "Cannot rename folder {0}".format(job_folder_name))


	job_folder_name = os.path.basename(path).replace(".", "") 
	job_dir = os.path.dirname(path) 
	while True:
		job_folder_name = "TODELETE_{0}".format(job_folder_name) 
"""

class JobArchive:

	def __init__(self, ) -> None:
		self.files = None 
		self.iterator = None 
		self.folder_archive = None 

	def _get_iterator_from(self, path):
		return ConfigParser(path).get_first_section("WORKFLOW").get("ITERATOR")

	def get_iterator(self, src, sim_file):
		run_zero = os.path.join(
			os.path.dirname(src),
			"RUN-0"
		)
		if not os.path.exists(run_zero): 
			return "0"
		sim_name = os.path.splitext(sim_file)[0]
		split = sim_name.split("@")
		if len(split) == 1:
			return "0" 
		return split[1]

	def process_folders(self, src, sim_file):  
		if not self.iterator:
			self.iterator = self.get_iterator(src, sim_file)
		dst = "{0}-{1}".format(src, self.iterator) 
		make_dir(dst, "Cannot create folder {0}".format(dst))  
		self.folder_archive = dst 

	def copy_file(self, src_file): 
		dst = os.path.join(
			self.folder_archive, 
			get_basename(src_file) 
		)
		logger.log_event("info,terminal", "copy dst: {0}".format(dst))
		copy_file(src_file, dst, "Cannot copy file {0}".format(src_file)) 
		#self.rename_sim(src_file, job) 

	"""
	def rename_sim(self, src_file, job:Job):
		ext = os.path.splitext(src_file)[1]
		if ext != ".sim":
			return 
		basename = os.path.basename(src_file)
		split = basename.split("@")
		if len(split) == 1:
			return 
		dirname = os.path.dirname(src_file) 
		dst = os.path.join( 
			dirname, 
			"{0}.sim".format(split[0])
		)
		rename(src_file, dst ,"Cannot rename sim file {0}".format(src_file)) 
		job.software.sim = dst 
	"""

	def rename_path(self, src):
		dst = os.path.join( 
			self.folder_archive, 
			get_basename(src) 
		)
		logger.log_event("info,terminal", "rename dst: {0}".format(dst))
		rename(src, dst, "Cannot move path {0}".format(src))

	def process_archive(self, job: Job, name):
		with os.scandir(job.path) as paths: 
			entry = next(paths, None) 
			if not entry: 
				copy_template(job, name)  
				return 
			self.process_folders(job.path, job.software.sim)  
			while (entry):
				path = os.path.abspath(entry.path)
				logger.log_event("info,terminal", "file: {0}".format(path))
				if entry.is_dir():
					self.rename_path(path)
				if (entry.is_file() or entry.is_symlink()) and not entry.path.endswith(".sim") and not entry.path.endswith(".simh"): #20250326SB don't copy simh #1don't copy the sim file in the archive
					if get_basename(path) in self.files: 
						self.copy_file(path) 
					else:
						self.rename_path(path) 
				entry = next(paths, None)

				

	def archive(self, P: Project):
		for job in P.jobs: 
			self.files = job.software.get_files() 
			self.process_archive(job, P.name) 

			self.folder_archive = None 
			self.files = None 


#
# TODO 
# DONE - ERROR CREATE SYMLINK DURING RERUN 
# DONE - ITERATOR NO 0 BECAUSE STARTS FROM SAME FOLDER SO ALWAYS ITERATOR 
# DONE - MIGHT NEED TO DETECT IF 0 FOLDER EXIST AND CREATE IT IF NEEDED 
# 
# RERUN AND REPOST, POST COPY SIM FILE FROM POST ARCHIVE (SHOULD BE DUMMY FILE INSTEAD) 


def copy_template(job: Job, name):
	for file in job.software.files:
			new_files = [] 
			dst_file = os.path.join(
				job.path, 
				os.path.basename(file)
			)
			copy_file(file, dst_file, "Cannot copy file {0}".format(file), True)
			new_files.append(dst_file)  
	for file in new_files: 
		job.software.files = file  

	if isinstance(job.software, StarCCM):
		dst_file = os.path.join( 
			job.path, 
			os.path.basename(job.software.macro)
		) 
		copy_file(job.software.macro, dst_file, "Cannot copy file {0}".format(file), True)
		job.software.macro = dst_file 
		if job.software.sim:
			dst_file = os.path.join( 
				job.path, 
				name + ".sim" 
			) 
			copy_file(job.software.sim, dst_file, "Cannot copy file {0}".format(file), True) 
			job.software.sim = dst_file 

def copy_templates(P : Project): 
	for job in P.jobs:
		copy_template(job, P.name) 


def create_dummy_sim_file(job: Job, P: Project):
	dummy_sim_file = os.path.join(job.path, P.name+".sim")
	open(dummy_sim_file, "w").close() 
	return dummy_sim_file

def sim_file_handler(P: Project): 
	# 1 - job steps do not exist -> check if previous step path exist -> create symlink 
	# 2 - job steps exist -> previous step inside -> create dummy file (risk of collision but ignore for now)
	# 3 - job steps exist -> previous step not inside -> check if previous step path exist -> create symlink 

	for i in range(0, len(P.jobs)): 
		job = P.jobs[i]
		step = os.path.basename(job.path)
		if step == "PRE" or job.software.sim: 
			continue 

		if i > 0: 
			job.software.sim = create_dummy_sim_file(job, P) 
			continue 

		sim_file = os.path.join( 
			job.path, 
			P.name + ".sim" 
		)	 
		job_steps = os.path.join(P.run_dir, ".JOB_STEPS")
		if not os.path.exists(job_steps):
			create_symlink(job.path, step)  
			job.software.sim = sim_file
			continue 

		previous_step = get_previous_step(step)
		with open(job_steps, "r") as file: 
			content = file.readline() 
		
		if previous_step in content: 
			job.software.sim = create_dummy_sim_file(job, P) 
		else:
			create_symlink(job.path, step)  
			logger.log_event("info,terminal", "handler sim file: {0}".format(sim_file))
			job.software.sim = sim_file



def create_symlink(job_path, step):
	previous_step_path = os.path.join(
			os.path.dirname(job_path), 
			get_previous_step(step)
	)
	if not os.path.exists(previous_step_path): 
		simple_exit("Missing SIM_FILE parameter or missing previous job folder")

	cmd = "bash \"{0}\" -s \"{1}\"".format(
		os.path.join( 
			os.path.dirname(os.path.abspath(__file__)),
			Uniq.job_state
		),
		previous_step_path
	)
	output = run_command(cmd) 
	if "ERROR" in output:
		simple_exit("Cannot create symlink for sim file", output) 
	logger.log_event("info,terminal", "DEBUG - symlink output: {0}".format(output)) 


def copy_post_resource(WA:WorkflowArgs, P:Project):
	"""
	Copy the xlsm into POST folder and rename it -PROJECT_CODE-JOB_CODE.xlsm
	"""
	for step in ["PRE", "POST"]:
		if step in Uniq.steps and not Uniq.cleanup:
			output_dir = os.path.join(WA.template, "output")
			if not os.path.exists(output_dir):
				logger.log_event("info,terminal", f"{output_dir} does not exists")
			elif not os.path.isdir(output_dir):
				logger.log_event("info,terminal,", f"{output_dir} is not a folder")
			else:
				with os.scandir(output_dir) as folder:
					for file in folder:
						if os.path.isfile(file) and file.name.endswith(".xlsm"):
							dst_file = os.path.join(P.run_dir, step, file.name.replace(".xlsm", f"-{WA.project_code}-{WA.run_number}.xlsm"))
							copy_file(file, dst_file, f"Cannot copy file {file.path}", True)
							logger.log_event("info,terminal", f"{file.name} copied into {dst_file}")
							break # Protection to multiple copy, normaly the folder contains only one .xlsm file
