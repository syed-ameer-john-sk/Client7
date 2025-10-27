from lib2to3.pgen2.token import AT
import sys 
import os 
import core
import common 
import classes 
import argparse
import run_number_manager # Import the new run number manager

def set_log(copylogger : common.Log ):
	global logger 
	logger = copylogger

def setup_parser(parser:argparse.ArgumentParser):
    """Setup the argparse parser"""
    parser.add_argument('parameter_file', type=str, help="parameter file")
    parser.add_argument('-c', '--cleanup', action='store_true', help="Run workflow to clean meshed file")
    parser.add_argument('-d', '--depend', type=int, nargs=1, metavar="<jobId>", help="Dependency: waits for job <jobId> to complete before running, make no sence if -c is not present")

def main(parameter_file):

	#######################
	# INITIALIZE WORKFLOW #
	#######################
	parameter_file = os.path.abspath(parameter_file) 
	common.set_log(parameter_file) 
	set_log(common.get_logger())
	core.set_log(common.get_logger())
	classes.set_log(common.get_logger())
	logger.log_event("info", "Execution")
	core.set_workflow_config()
	core.set_system_global_env() 

	########################### 
	# SET WORKFLOW PARAMETERS # 
	###########################
	workflow_args = classes.ConfigParser(parameter_file).get_first_section("WORKFLOW")
	classes.Uniq.parameter = parameter_file
	classes.Uniq.user_dir = os.path.dirname(parameter_file)  

	# ---- GET AUTOMATED RUN NUMBER FROM DATABASE ----
	run_number_from_db = run_number_manager.get_run_number(classes.Uniq.user_dir)
	if not run_number_from_db:
		common.simple_exit("Failed to generate or retrieve run number from the database.")
	
	# Extract only the numeric part for the workflow
	try:
		run_number_parts = run_number_from_db.split('-')
		if len(run_number_parts) == 3:
			workflow_args['RUN_NUMBER'] = run_number_parts[2]
		else:
			# Fallback for unexpected format
			workflow_args['RUN_NUMBER'] = run_number_from_db
	except Exception as e:
		common.simple_exit(f"Could not parse the run number received from database: {run_number_from_db}. Error: {e}")
	# ------------------------------------------------

	############################# 
	# CHECK WORKFLOW PARAMETERS # 
	############################# 
	WA = classes.WorkflowArgs()
	WA.set_members(workflow_args) 

	################################### 
	# CHECK / RETRIEVE WORKFLOW FILES # 
	###################################
	P = classes.Project()
	P.set_members(WA) 

	hold_job_id = core.get_hold_job_id(P)
		
	###########
	# ARCHIVE # 
	###########
	if classes.Uniq.rerun: 
		logger.move_logs(P.run_dir)
		sys.stderr = logger.set_log("stderr")
		job_archive = core.JobArchive()
		job_archive.archive(P) 
		core.sim_file_handler(P) 
	else: 
		##############
		# COPY FILES # 
		##############
		core.copy_templates(P) 
		core.copy_post_resource(WA, P)
		###########
		# SYMLINK # 
		###########
		core.sim_file_handler(P)   

	###############
	# SUBMIT JOBS #  
	###############
	submit_job = classes.SubmitJob(WA, P)
	if classes.Uniq.previous_job_id is not None:
		submit_job.submit_job(classes.Uniq.previous_job_id)
	else:
		submit_job.submit_job(hold_job_id)

	core.set_cron_job(P)
	
	if not classes.Uniq.rerun:
		logger.move_logs(P.run_dir) 
		sys.stderr = logger.set_log("stderr")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    setup_parser(parser)
    args = parser.parse_args()
    if args.depend and not args.cleanup:
        print("option -d without -c make no sence.\nJob not submitted")
        exit(1)
    if args.cleanup:
        classes.Uniq.cleanup = True
    if args.depend:
        classes.Uniq.previous_job_id = str(args.depend[0])
        print("previous job id:", classes.Uniq.previous_job_id)

    main(args.parameter_file)

