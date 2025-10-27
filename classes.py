from functools import wraps
import re
import traceback
import configparser
from typing import List
from common import*

def set_log(copylogger : Log ):
	global logger
	logger = copylogger

##############
# DECORATORS #
##############
def step(*allowed, unique=False, group=False, others=True):
	def decorator(method):
		@wraps(method)
		def inner(self, *args, **kwargs):
			if is_allowed_to_run(*allowed, unique=unique,
								group=group, others=others):
				return method(self, *args, **kwargs)
			return
		return inner
	return decorator

def is_allowed_to_run(*allowed, unique=False, group=False, others=True) -> bool:
	intersect = len(set(allowed).intersection(Uniq.steps))
	if group and len(Uniq.steps) == len(allowed) and intersect == len(allowed):
		return True
	elif unique and len(Uniq.steps) == 1 and intersect == 1:
		return True
	elif unique and len(Uniq.steps) == 2 and intersect == 2 and "RUN" in Uniq.steps:
		return True
	elif others and intersect != 0:
		return True
	return False

def not_none():
	def decorator(method):
		@wraps(method)
		def inner(self, *args, **kwargs):
			if any(arg is None for arg in args):
				simple_exit("Missing parameter: {0}, qualname: {1}".format(method.__name__, method.__qualname__))
			return method(self, *args, **kwargs)
		return inner
	return decorator

class Regex:

	walltime = r'^((([0-1]?[0-9]|[2]?[0-3]):([0-5]?[0-9]):([0-5]?[0-9]))|(([2][4]:[0]?[0]:[0]?[0])))'
	numbers = r'^[0-9]+$'
	spec = r'[\.\/\\\ &^*%#@!?~`{}\[\]+=<>|]'

	@staticmethod
	def match(value, regex) -> bool:
		if not bool(re.match(regex, value)):
			return False
		return True

	@staticmethod
	def sub(value, repl, regex):
		return re.sub(regex, repl, value)

class ConfigParser:

	def __init__(self, file_path : str) -> None:
		if not os.path.exists(file_path):
			simple_exit("File {0} does not exist".format(file_path))
		self.file_path = file_path
		self.config = self.set_config()

	def set_config(self):
		config = configparser.ConfigParser(allow_no_value=True,
									   inline_comment_prefixes=("#","*"),
									   strict=False)
		config.optionxform = str
		return config

	def read_lines_from(self, file_path):
		try:
			with open(file_path, "r") as file:
				lines = file.readlines()
			return lines
		except Exception as e:
			simple_exit("Error while opening {0}".format(file_path), e)

	def to_dict(self):
		return {s:dict(self.config.items(s)) for s in self.config.sections()}

	def get_first_section(self, section): # for workflow parameter
		content = "[{0}]\n{1}".format(section,
						"".join(self.read_lines_from(self.file_path)))
		try:
			self.config.read_string(content)
		except Exception as e:
			simple_exit("Cannot read configuration file", e)
		return self.to_dict().pop(section)

	def get_all_sections(self):
		try:
			self.config.read(self.file_path)
		except Exception as e:
			simple_exit("Cannot read configuration file", e)
		return self.to_dict()



class WorkflowConfig:

	def __init__(self) -> None:
		self._root_dir 			= None
		self._global_env 		= None
		self._list_queues 		= None
		self._solver_versions 	= None
		self._output_dir	 	= None
		self._job_launcher		= None
		self._optionsds		 	= None
		self._project_root_dir 	= None
		self._template_root_dir = None

	def set_members(self, dict : dict):
		scripts   = dict.get("ATOS_SCRIPTS")
		root_dirs = dict.get("ROOT_DIRECTORIES")

		if not scripts or not root_dirs:
			simple_exit("Missing section in workflow config")

		self.root_dir          = scripts.get("root_dir")
		self.optionsds 	   	   = scripts.get("optionsds")
		self.global_env        = scripts.get("global_env")
		self.list_queues       = scripts.get("list_queues")
		self.solver_versions   = scripts.get("solver_versions")
		self.job_launcher 	   = scripts.get("job_launcher")
		self.project_root_dir  = root_dirs.get("project_root_dir")
		self.template_root_dir = root_dirs.get("template_root_dir")

	@property
	def root_dir(self):
		return self._root_dir

	@root_dir.setter
	@not_none()
	def root_dir(self, value):
		if not os.path.exists(value):
			simple_exit("Workflow config - Invalid path {0}: {1}".format("root_dir", value))
		self._root_dir = value

	@property
	def global_env(self):
		return self._global_env

	@global_env.setter
	@not_none()
	def global_env(self, value):
		path = os.path.join(self._root_dir, value)
		if not os.path.exists(path):
			simple_exit("Workflow config - Invalid path {0}: {1}".format("global_env", value))
		self._global_env = path

	@property
	def list_queues(self):
		return self._list_queues

	@list_queues.setter
	@not_none()
	def list_queues(self, value):
		path = os.path.join(self._optionsds, value)
		if not os.path.exists(path):
			simple_exit("Workflow config - Invalid path {0}: {1}".format("list_queues", value))
		self._list_queues = path

	@property
	def solver_versions(self):
		return self._solver_versions

	@solver_versions.setter
	@not_none()
	def solver_versions(self, value):
		path = os.path.join(self._optionsds, value)
		if not os.path.exists(path):
			simple_exit("Workflow config - Invalid parameter {0}: {1}".format("solver_versions", value))
		self._solver_versions = path

	@property
	def job_launcher(self):
		return self._job_launcher

	@job_launcher.setter
	@not_none()
	def job_launcher(self, value):
		path = os.path.join(self._root_dir, value)
		if not os.path.exists(path):
			simple_exit("Workflow config - Invalid parameter {0}: {1}".format("job_launcher", value))
		self._job_launcher = path

	@property
	def optionsds(self):
		return self._optionsds

	@optionsds.setter
	@not_none()
	def optionsds(self, value):
		if not os.path.exists(value):
			simple_exit("Workflow config - Invalid parameter {0}: {1}".format("optionsds", value))
		self._optionsds = value

	@property
	def project_root_dir(self):
		return self._project_root_dir

	@project_root_dir.setter
	@not_none()
	def project_root_dir(self, value):
		if not os.path.exists(value):
			simple_exit("Workflow config - Invalid parameter {0}: {1}".format("project_root_dir", value))
		self._project_root_dir = value

	@property
	def template_root_dir(self):
		return self._template_root_dir

	@template_root_dir.setter
	@not_none()
	def template_root_dir(self, value):
		if not os.path.exists(value):
			simple_exit("Workflow config - Invalid parameter {0}: {1}".format("template_root_dir", value))
		self._template_root_dir = value

class WorkflowArgs:

	def __init__(self) -> None:
		# mandatory
		self._project_code 	 = None
		self._run_number 	 = None
		self._description  	 = None
		self._solver_version = None
		self._walltime 		 = None
		self._queue 	  	 = None # queue used for RUN step
		self._workflow_steps = None
		self._task_code      = None

		# optional
		self._sim_file = None
		self._iterator = None
		self._template = None

	def set_members(self, dict : dict):
		self.project_code 	= dict.get("PROJECT_CODE")
		self.task_code      = dict.get("TASK_CODE")
		self.description 	= dict.get("DESCRIPTION")
		self.solver_version = dict.get("SOLVER_VERSION")
		self.walltime 		= dict.get("WALLTIME")
		self.queue 			= dict.get("QUEUE")
		self.workflow_steps = dict.get("WORKFLOW_STEPS")
		self.run_number     = dict.get("RUN_NUMBER")

		self.iterator = dict.get("ITERATOR")
		self.template = dict.get("TEMPLATE")
		self.sim_file = dict.get("SIM_FILE")

	@property
	def project_code(self):
		return self._project_code

	@project_code.setter
	@not_none()
	def project_code(self, value):
		self._project_code = Regex.sub(value, "_", Regex.spec)

	@property
	def task_code(self):
		return self._task_code

	@task_code.setter
	@not_none()
	def task_code(self, value):
		self._task_code = value

	@property
	def run_number(self):
		return self._run_number

	@run_number.setter
	@not_none()
	def run_number(self, value):
		# This setter now directly accepts the run number generated from the database
		self._run_number = value

	@property
	def description(self):
		return self._description

	@description.setter
	@not_none()
	def description(self, value):
		self._description = value

	@property
	def solver_version(self):
		return self._solver_version

	@solver_version.setter
	@not_none()
	def solver_version(self, value):
		"""   """
		script = os.path.join(
			Uniq.config.optionsds,
			Uniq.config.solver_versions
		)
		cmd = "{0} {1}".format(script, Uniq.default_solver)
		output = run_command(cmd)
		versions = []
		for e in output.split("\n"):
			versions.extend(e.split(";"))
		if not value in versions:
			simple_exit("Invalid solver version")
		self._solver_version = value

	@property
	def walltime(self):
		return self._walltime

	@walltime.setter
	@not_none()
	def walltime(self, value):
		if not Regex.match(value, Regex.walltime):
			simple_exit("Invalid walltime")
		self._walltime = value

	@property
	def queue(self):
		return self._queue

	@queue.setter
	@not_none()
	def queue(self, value):
		# TODO: create a script (or a config file) to provide queues
		# instead of using ac_StarCcmListQueues.sh
		cmd = os.path.join(Uniq.config.optionsds, Uniq.config.list_queues)
		output = run_command(cmd).split("\n")

		available_queues = {}
		for keyValue in output:
			keyValue = keyValue.split(";", 1)
			if len(keyValue) == 2 and keyValue[0] != "" and keyValue[1] != "":
				available_queues[keyValue[1].lower()] = keyValue[0]

		# Temporary add aerox-india queue 27/02/2023
		available_queues["aerox-india"] = "aeroxindia.q"
		value = value.lower()
		if value in available_queues:
			value = available_queues[value]
		elif not value in available_queues.values():
			simple_exit("Invalid queue")
		self._queue = value

	@property
	def workflow_steps(self):
		return self._workflow_steps

	@workflow_steps.setter
	@not_none()
	def workflow_steps(self, value):
		allowed_targets = ["PRE", "RUN", "POST", "ALL"]
		value = [step.strip().upper()
					for step in value.split(" ")]
		if "ALL" in value:
			value = ["PRE", "RUN", "POST"]
		is_different = set(value).difference(allowed_targets)
		if len(is_different) != 0:
			simple_exit("Invalid workflow steps")

		value = self._sort_workflow_steps(value)
		self._workflow_steps = value
		Uniq.steps = value

	def _sort_workflow_steps(self, steps):
		sorted_steps = [None, None, None]
		for step in steps:
			if step == "PRE":
				sorted_steps[0] = step
			elif step == "RUN":
				sorted_steps[1] = step
			else:
				sorted_steps[2] = step
		return [step for step in sorted_steps if step != None]

	@property
	def sim_file(self):
		return self._sim_file

	@sim_file.setter
	@step("PRE", "RUN", "POST", unique=True, group=True, others=False)
	def sim_file(self, value):
		# Rerun condition
		if value and self.iterator and\
		  Uniq.steps[0] == "RUN" and\
		  get_basename(Uniq.user_dir) == "RUN":
			Uniq.rerun = True
		elif not value:
			return

		path = os.path.join(
			Uniq.user_dir,
			value
		)
		if not os.path.exists(path):
			simple_exit("Invalid sim file")
		self._sim_file = path

	@property
	def iterator(self):
		return self._iterator

	@iterator.setter
	def iterator(self, value):
		if not value or Uniq.steps[0] != "RUN":
			return
		if not Regex.match(value, Regex.numbers):
			simple_exit("Invalid iterator")
		self._iterator = value

	@property
	def template(self):
		return self._template

	@template.setter
	def template(self, value):
		# It means that if RUN, POST, template needed for POST
		# but if RUN do not want to use the run template,
		# not possible. It will automatically use the run template
		# because of POST step
		if "PRE" in Uniq.steps or "POST" in Uniq.steps:
			if not value:
				simple_exit("Missing template parameter")
		if value == "run_with_macro":
			self._template = "run_with_macro"
			return
		path = os.path.join(Uniq.config.template_root_dir, value)
		if not os.path.exists(path):
			simple_exit("Invalid template")
		self._template = path




# RERUN CONDITIONS:
# ITERATOR
# FIRST STEP IS RUN
# SIM FILE
# PARAMETERS.TXT IN RUN FOLDER









class Files:

	# append only, no modification allowed

	def __init__(self) -> None:
		self._files = list()

	def __str__(self) -> str:
		return "Files: " + "\n".join([file for file in self._files])

	@property
	def files(self):
		return self._files

	@files.setter
	def files(self, value):
		if not os.path.exists(value):
			simple_exit("File {0} does not exist".format(value))
		self._files.append(value)

class StarCCM(Files):

	def __init__(self) -> None:
		Files.__init__(self)
		# should not be modified outside the class
		self._software = Uniq.starccm

		# need to retrieve these members for xf_Run command
		self._sim = None
		self._macro = None

	def __str__(self) -> str:
		return "\n".join(
				[super().__str__(),
			 	"software: " + self._software,
			 	"sim: {0}".format(self._sim),
				"macro: {0}".format(self._macro)])

	def set_members(self):
		self.macro = os.path.join(
			os.path.dirname(os.path.abspath(__file__)),
			Uniq.star_macro
		)

	def get_files(self):
		files = [get_basename(path) for path in self.files]
		files.append(get_basename(self._macro))
		if self._sim:
			files.append(get_basename(self._sim))
		return files

	@property
	def sim(self):
		return self._sim

	@sim.setter
	@not_none()
	def sim(self, value):
		if not os.path.exists(value):
			simple_exit("File {0} does not exist".format(value))
		self._sim = value

	@property
	def macro(self):
		return self._macro

	@macro.setter
	@not_none()
	def macro(self, value):
		if not os.path.exists(value):
			simple_exit("File {0} does not exist".format(value))
		self._macro = value

	def retrieve_files(self, WA : WorkflowArgs, step):
		#if not WA.template and step != "RUN":
		#	simple_exit("Step is not RUN, you must provide a template name")
		if WA.template == "run_with_macro":
			# if the template in parameters.txt is "run_with_macro", the macro used
			# should be named "Run_simulation.java" in the working folder
			template_file = os.path.join(Uniq.user_dir, "Run_simulation.java")
			if not os.path.exists(template_file):
				simple_exit("Template: run_with_macro found, but {0} does not exist".format(template_file))
			self.files = template_file
		else:
			self._retrieve_template(WA.template, step)
		if step == "PRE" and WA.template not in ["/home/USER/share/_TEMPLATES/default_run", "/home/USER/share/_TEMPLATES/default_run-3"]: #20250418SB do not check for input_data file when doing PRE with default_run. hard-coded. find cleaner solution.
			self._retrieve_user_files()
		self._retrieve_sim(WA.template, step, WA.sim_file)
		self.files = Uniq.parameter

	def _retrieve_sim(self, template_path, step, sim_file):
		# 20-04-2023: For a PRE step, if a sim file is given it should be used instead of sim file from template
		if step == "PRE" and (Uniq.cleanup or sim_file is not None):
			self.sim = sim_file
		elif step == "PRE": # default sim file from template
			self.sim = os.path.join(
				template_path,
				"sim_file",
				"simulation.sim"
			)
		# elif step == "RUN" and Uniq.rerun:
			# TODO: create symlink
			#os.link(sim_file, )
			# pass
		elif Uniq.steps[0] == step and sim_file: # sim file from user (only for first step)
			# always copy SIM_FILE (no symlink)
			self.sim = sim_file

	def _retrieve_user_files(self):
		input_data_file = os.path.join(
			Uniq.user_dir,
			Uniq.template_input_file
		)
		user_files = ConfigParser(input_data_file).get_all_sections()
		def add_files(files):
			for file in files:
				path = os.path.join(
					Uniq.user_dir,
					file
				)
				self.files = path
		template = user_files.get("TEMPLATE")
		if template:
			add_files(template.keys())
		add_files(user_files.pop("PARTS").values())

		self.files = input_data_file

	@not_none()
	def _retrieve_template(self, template_path, step):
		def add_template(path):
			i = 0
			with os.scandir(path) as p:
				for entry in p:
					self.files = entry.path
					i+= 1
			if i == 0:
				simple_exit("No template files available for the job {0}".format(step))

		if template_path:
			path = os.path.join(
				template_path,
				"macros",
				step
			)
			add_template(path)
			return
		path = os.path.join(
			os.path.dirname(os.path.abspath(__file__)),
			#"default_template",
			"default_run",
			step
		)
		if not os.path.exists(path):
			simple_exit("No template files available for the job {0}".format(step))
		add_template(path)

class Job:

	def __init__(self) -> None:
		# path of the job folder (currently PRE/RUN/POST)
		self._path: str = None
		# job id
		self._id: str = None
		# StarCCM, ANSA, ...
		self._software = None

		self._queue = None

	def __str__(self) -> str:
		return "\n".join(
			["path: " + self._path,
			self._software.__str__()])

	def set_members(self, path, software, queue):
		self.path = path
		self.software = software
		self.queue = queue

	@property
	def path(self):
		return self._path

	@path.setter
	@not_none()
	def path(self, value):
		if not os.path.exists(value):
			if Uniq.rerun and os.path.basename(value) == Uniq.steps[0]:
				simple_exit("Cannot rerun with missing job folder {0}".format(value))
			make_dir(value, "Cannot create job folder {0}".format(value))
			set_mode_bits(value, 0o755)
			Uniq.atexit_jobs_path.append(value)
		elif not Uniq.rerun and not Uniq.cleanup:
			simple_exit("Job folder already exist: {0}".format(value))
		self._path = value

	@property
	def id(self):
		return self._id

	@id.setter
	@not_none()
	def id(self, value):
		self._id = value

	@property
	def software(self):
		return self._software

	@software.setter
	@not_none()
	def software(self, value):
		self._software = value

	@property
	def queue(self):
		return self._queue

	@queue.setter
	@not_none()
	def queue(self, value):
		self._queue = value


class Project:

	def __init__(self) -> None:
		self._name: str = None
		self._dir: str = None
		self._run_dir: str = None
		self._jobs: List[Job] = None

	def __str__(self) -> str:
		return "\n".join(
			["name: " + self._name,
			"dir: " + self._dir,
			"run_dir: " + self._run_dir,
			"\n".join([job.__str__() for job in self._jobs])])

	def set_members(self, WA : WorkflowArgs):
		# --- START OF CHANGE ---
		if WA.run_number == "DUMMY_XXX_000":
			self.name = "DUMMY_XXX_000"
		else:
			self.name = "{0}-{1}-{2}".format(WA.project_code, WA.task_code, WA.run_number)
		# --- END OF CHANGE ---

		self.dir = os.path.join(
			Uniq.config.project_root_dir,
			WA.project_code
		)
		self.run_dir = os.path.join(
			self.dir,
			self.name
		)
		if Uniq.rerun and Uniq.user_dir != os.path.join(self.run_dir, "RUN"):
			Uniq.rerun = False
		self._set_jobs(WA)

	def _set_jobs(self, WA : WorkflowArgs):
		jobs = []
		for step in Uniq.steps:
			job_path = os.path.join(self.run_dir, step)
			software = StarCCM()
			software.set_members()
			software.retrieve_files(WA, step)
			if Uniq.steps[0] == step and not software.sim and not self._is_link_possible(step):
				simple_exit(f"Missing previous job folder or SIM_FILE parameter, step='{step}', sim='{software.sim}'")
			job = Job()
			job.set_members(job_path, software, self._set_queue(step, WA.queue))
			jobs.append(job)
		self._jobs = jobs

	def _set_queue(self, step, run_queue): # TODO retrieve automatically default queues (aerox.q)
		default_queues = {"PRE": run_queue, "RUN": run_queue, "POST": run_queue} #SB changed PRE & POST from "aerox.q" to run_queue (change back once access to aerox is general)
		return default_queues[step]

	def _is_link_possible(self, step):
		previous_step = get_previous_step(step)
		if previous_step:
			previous_job_folder = os.path.join(self.run_dir, previous_step)
			if os.path.exists(previous_job_folder):
				logger.log_event("info,terminal", "folder for link exist")
				cmd = "bash \"{0}\" -f \"{1}\"".format(
					os.path.join(
						os.path.dirname(__file__),
						Uniq.job_state
					),
					previous_job_folder
				)
				output = run_command(cmd)
				logger.log_event("info,terminal", "f command output: {0}".format(output))
				if not "None" in output:
					return True
		return False

	@property
	def name(self):
		return self._name

	@name.setter
	@not_none()
	def name(self, value):
		self._name = value

	@property
	def dir(self):
		return self._dir

	@dir.setter
	@not_none()
	def dir(self, value):
		if not os.path.exists(value):
			if Uniq.rerun:
				simple_exit("Cannot rerun non existent project")
			make_dir(value, "Cannot create project folder {0}".format(value))
			set_mode_bits(value, 0o777)
		self._dir = value

	@property
	def run_dir(self):
		return self._run_dir

	@run_dir.setter
	@not_none()
	def run_dir(self, value):
		if not os.path.exists(value):
			if Uniq.rerun:
				simple_exit("Cannot rerun non existent project run")
			make_dir(value, "Cannot create project run folder {0}".format(value))
		self._run_dir = value

	@property
	def jobs(self):
		return self._jobs

	@jobs.setter
	@not_none()
	def jobs(self, value):
		self._jobs = value







class SubmitJob:

	def __init__(self, WA: WorkflowArgs, Project: Project) -> None:
		self.WA = WA
		self.P = Project

	def set_command(self, job: Job, previous_job_id = None):
		root_dir = Uniq.config.root_dir
		xf_run_name = Uniq.config.job_launcher
		xf_Run = os.path.join(root_dir, xf_run_name)
		software_name = "-s {0}".format(job.software._software)
		software_version = "-v " + self.WA.solver_version
		nb_proc = "-n " + "1"
		input_file = "-i " + "\"" + job.software.sim + "\""
		job_name = "-j " + "\"" + self.P.name + "\""
		queue = "-q " + job.queue
		walltime = "-w \"" + self.WA.walltime + "\""
		hold = ("-d " + previous_job_id if previous_job_id else "")
		compress_result = "-e NOZIP"
		output_dir = "-o " + "\"" + job.path + "\""
		output_sub_dir = "-os NONE"
		macros = "-p " + job.software.macro
		send_mail = "-- mail=false" if Uniq.cleanup else "-- mail=true"

		xf_Run_cmd = " ".join([xf_Run, software_name, software_version,
			nb_proc, input_file, job_name, queue, walltime, hold,
			compress_result, output_dir, output_sub_dir,
			macros, send_mail])

		return xf_Run_cmd

	def retrieve_job_id(self, message : str):
		index = message.lower().index("jobid")
		return message[index:].split(":")[1].strip()

	def submit_job(self, previous_job_id=None):
		for job in self.P.jobs:
			cmd = self.set_command(job, previous_job_id)
			logger.log_event("info,terminal", "xf run command: {0}".format(cmd))
			output = run_command(cmd)
			logger.log_event("info,terminal", "submit job output: {0}".format(output))
			#stdout = "jobid:10293"
			if "ERROR" in output:
				exit_workflow()
			previous_job_id = self.retrieve_job_id(output)
			logger.log_event("info,terminal", "Job submitted with ID: {0}".format(previous_job_id))
			job.id = previous_job_id















class AtExit(object):

	def __init__(self, project: Project = None):
		self.project = project

	def set_project(self, project: Project):
		self.project = project

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, tb):
		if exc_value is not None:
			logger.log_event("error,terminal", "__exit__ TODELETE")
			#rename_to_delete(self.project)
			traceback.print_tb(tb, file = sys.stderr)
			traceback.print_tb(tb, file = sys.stdout)
			exit_workflow()









class Uniq:  # this is a different method to create/use global variables
	job_state = "job_state_handler.sh"
	workflow_config = "workflow_config.cfg"
	template_input_file = "input_data_file.txt"
	star_macro = "StarCCM_Main_Macro.java"
	run_macro = "Run_simulation.java"

	user_dir: str = None
	parameter: str = None
	rerun = False
	cleanup = False
	previous_job_id:str = None
	# default queue for PRE / POST
	small_queue = "aerox.q"
	# default solver
	default_solver = "Starccm"
	starccm = "StarccmFlex"

	# workflow config
	config: WorkflowConfig = None
	steps: List[str] = None

	# at exit
	atexit_jobs_path: List[str] = []
	atexit_archives_path: List[str] = []