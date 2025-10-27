import sys # Import sys to access command-line arguments
import subprocess # Added for running external commands
import pymysql
import os
import pandas as pd

from datetime import datetime # Added for handling timestamps
from db_connection import get_db_connection
from select_dir import base_directory
import db_table

# --- NEW FUNCTION to read WORKFLOW_STEPS from parameters.txt ---
def get_workflow_step_from_params(base_dir, target_file='parameters.txt'):
    """Reads the WORKFLOW_STEPS value from the parameters.txt file."""
    for root, dirs, files in os.walk(base_dir):
        if target_file in files:
            file_path = os.path.join(root, target_file)
            try:
                with open(file_path, 'r') as file:
                    for line in file:
                        if line.strip().startswith("WORKFLOW_STEPS"):
                            return line.strip().split(":", 1)[-1].strip()
            except Exception as e:
                print(f"Error reading the file {file_path}: {e}")
    return None

# --- NEW Elapse Time Insertion Function ---
def insert_elapse_time(runcode, projectcode, taskcode, runnum, workflowstep,
                       submissiondate, username, prejobid, preelapsetime,
                       runjobid, runelapsetime, postjobid, postelapsetime, connection):
    """
    Inserts a new row in Db_Elapse_Time for the current job run/step.
    Only fills the columns relevant to the step specified (PRE, RUN, POST, ALL).
    """
    # Set all fields to None (NULL)
    values = {
        'RUNCODE': runcode,
        'PROJECTCODE': projectcode,
        'TASKCODE': taskcode,
        'RUNNUMBER': runnum,
        'SUBMISSIONDATE': submissiondate,
        'USERNAME': username,
        'PREJOBID': None,
        'PREELAPSEDTIME': None,
        'RUNJOBID': None,
        'RUNELAPSEDTIME': None,
        'POSTJOBID': None,
        'POSTELAPSEDTIME': None
    }

    workflowstep = workflowstep.upper()
    # Update only the fields corresponding to the workflow step
    if 'PRE' in workflowstep or 'ALL' in workflowstep:
        values['PREJOBID'] = prejobid
        values['PREELAPSEDTIME'] = preelapsetime
    if 'RUN' in workflowstep or 'ALL' in workflowstep:
        values['RUNJOBID'] = runjobid
        values['RUNELAPSEDTIME'] = runelapsetime
    if 'POST' in workflowstep or 'ALL' in workflowstep:
        values['POSTJOBID'] = postjobid
        values['POSTELAPSEDTIME'] = postelapsetime

    # Always insert as a new row, never overwrite
    sql = f"""
        INSERT INTO {db_table.table_elapse}
        (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, SUBMISSION_DATE, USER_NAME, 
         PRE_JOB_ID, PRE_ELAPSED_TIME, RUN_JOB_ID, RUN_ELAPSED_TIME, POST_JOB_ID, POST_ELAPSED_TIME)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (values['RUNCODE'], values['PROJECTCODE'], values['TASKCODE'], values['RUNNUMBER'],
              values['SUBMISSIONDATE'], values['USERNAME'], values['PREJOBID'], values['PREELAPSEDTIME'],
              values['RUNJOBID'], values['RUNELAPSEDTIME'], values['POSTJOBID'], values['POSTELAPSEDTIME'])
    
    # Use a new cursor within the function for encapsulation
    cursor = connection.cursor()
    cursor.execute(sql, params)
    connection.commit()
    cursor.close()

def process_job_data(job_folder_path):
    connection = None # Initialize connection to None
    try:
        connection = get_db_connection()
        print("Connected Successfully to MARIADB")
    except Exception as e:
        print(f"Connection not successful: {e}")
        sys.exit(1) # Exit if DB connection fails

    try:
        base_dir = base_directory(job_folder_path) # Pass the job_folder_path
        print(f"Processing data from base directory: {base_dir}")
    except ValueError as e:
        print(f"Error getting base directory: {e}")
        if connection:
            connection.close()
        sys.exit(1)

    # --- Get the current workflow step ---
    workflow_step = get_workflow_step_from_params(base_dir)
    if not workflow_step:
        print("Could not determine WORKFLOW_STEPS from parameters.txt. Aborting data processing.")
        if connection:
            connection.close()
        sys.exit(1)
    print(f"Current WORKFLOW_STEP: {workflow_step}")


    cursor = connection.cursor() # Initialize cursor after connection is established

    #IF THE SIMULATION IS COMPLETED & IF PRE, RUN, POST FOLDERS ARE CREATED
    pre_meshStatics = 'meshStatistics.csv' # Mention the csv file name as it is in the folder path
    post_simulationMetrics = 'simulationMetrics.csv'
    post_CFx = 'Cx.csv'
    post_CFz = 'CFz.csv' # Corrected from CFz to Cz for consistency with common usage
    post_simulationMetrics_XWD = [f'simulationMetrics{str(i)}.csv' for i in range(0, 200, 5)]
    post_cummulative = ['0-Cumulated_Fx_iter.csv', '1-Cumulated_Fx_iter_bottom.csv', '1-Cumulated_Fx_iter_top.csv', '2-Cumulated_Fx_iter_pressure.csv', '2-Cumulated_Fx_iter_shear.csv']
    post_residuals = 'residuals.csv'
    post_head_Pr_pulse = 'head_pressure_pulse.csv'

    # KNOWN FILE AND FOLDER
    possible_tags = ['ABORT', 'TERMINATED', 'FAILED', 'FINISHED']
    pre_folders = ['PRE', 'PRE-FAILED', 'PRE-ABORTED']
    run_folders = ['RUN', 'RUN-FAILED', 'RUN-ABORTED']
    post_folders = ['POST', 'POST-FAILED', 'POST-ABORTED']

    # Read the .csv files located in the Root directory and say its count
    def find_csv_files(base_dir, file_identifier = '.csv'):
        list_of_found_files = []

        for root, dirs, files in os.walk(base_dir):
            for current_file in files:
                if isinstance(file_identifier, tuple): # Handle tuple of identifiers (e.g., for cumulated files)
                    if current_file in file_identifier:
                        list_of_found_files.append(os.path.join(root, current_file))
                elif isinstance(file_identifier, str) and current_file.endswith(file_identifier): # Handle single extension or full filename
                    list_of_found_files.append(os.path.join(root, current_file))
        return list_of_found_files

    csv_files_with_path = find_csv_files(base_dir) # This function holds the location the csv files from where you can extract the column name.
    if csv_files_with_path:
        print(f'Total Count of CSV Files: {len(csv_files_with_path)}')
    else:
        print('No CSV found. This might be a cleanup job or an error state. Moving on...')

    # Extract the Column names from the folder structure
    def extract_run_code(filepaths):
        # Ensure filepaths is a list, even if a single file path is passed
        if isinstance(filepaths, str):
            filepaths = [filepaths]

        for filepath in filepaths:
            spilt_folder_names = os.path.split(filepath)[0].split(os.sep)
            for folder in spilt_folder_names:
                # Validate that it matches the expected pattern, e.g., PROJECT-TASK-RUN_NUM (e.g., PRD2526MT-RTM-038)
                if '-' in folder and len(folder.split('-')) == 3:
                    return folder
        
        # Fallback to the basename of base_dir if no matching folder name is found in CSV paths
        base_dir_name = os.path.basename(base_dir)
        if '-' in base_dir_name and len(base_dir_name.split('-')) == 3:
            return base_dir_name
        
        return None

    run_code = extract_run_code(csv_files_with_path) # 'run_code' holds the info about the extracted the column name which can be used for the database in the tables.
    
    # Initialize run_code parts, will remain None if run_code not found
    project_code, task_code, run_num = [None] * 3

    if run_code:
        print(f'Extracted RUN CODE From The Folder Structure: {(run_code)}')

        # Split the RUN_CODE further:
        parts = run_code.split('-')
        if len(parts) == 3:
            project_code = parts[0]
            task_code = parts[1]
            run_num = parts[2]

            print(f'Extracted Project_Code: {(project_code)}') # 'project_code' holds the info about the project name
            print(f'Extracted Task_Code: {(task_code)}')       # 'task_code' holds the info about the task like RTM, XWD, EQU
            print(f'Extracted Run_Num: {(run_num)}')           # 'run_num' holds the info about the iteration number
        else:
            print(f"Warning: RUN_CODE '{run_code}' does not split into 3 parts as expected. Data might be incomplete.")
    else:
        print("Could not extract a valid RUN_CODE from folder structure. Skipping detailed data insertion for this job.")
        # We will not exit, but continue to try to insert flags if possible
    
    # Read and insert Mesh Statics CSV File and Insert the Values into DB
    mesh_files = find_csv_files(base_dir, pre_meshStatics)

    for file_path in mesh_files:
        try:
            print(f"Processing mesh file: {file_path}")
            df = pd.read_csv(file_path, delimiter=';', skiprows=1, header=None)
            df.columns = ['MONITORS', 'RESULTS']
            for index, row in df.iterrows():
                cursor.execute(f"""INSERT INTO {db_table.Staging_Mesh}
                              (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, MONITORS, RESULTS)
                             VALUES (%s, %s, %s, %s, %s, %s)""",
                             (run_code, project_code, task_code, run_num, str(row['MONITORS']), str(row['RESULTS'])))
                connection.commit()
        except Exception as e:
            print(f"Error processing mesh file {file_path}: {e}")
            connection.rollback() # Rollback in case of an error during a batch of inserts
    # Search for the file name starting with 'StarccmFlex' in the 'PRE', 'RUN', 'POST' folder:
    def find_WF_in_subfolders(base_dir, prefix = 'StarccmFlex'):
        WF_list = {}
        for subfolder in os.listdir(base_dir):
            subfolder_path = os.path.join(base_dir, subfolder)
            if os.path.isdir(subfolder_path):
                for file in os.listdir(subfolder_path):
                    if file.startswith(prefix) and file.endswith('.log'):
                        WF_list[subfolder] = file
                        break
        return WF_list
        
    WF_list = find_WF_in_subfolders(base_dir)
    print("The log files found are", WF_list)

    # Initialize the values 
    POST_JOB_ID = RUN_JOB_ID = PRE_JOB_ID = None
    pre_owner = run_owner = post_owner = None
    pre_qsub_dt = run_qsub_dt = post_qsub_dt = None
    pre_elapse_time = run_elapse_time = post_elapse_time = None

    # FOLDER TYPE for PRE:
    for key in pre_folders:
        if key in WF_list:
            PRE_JOB_ID = WF_list[key].split('_')[1].split('.')[0]
            break

    # FOLDER TYPE for RUN:
    for key in run_folders:
        if key in WF_list:
            RUN_JOB_ID = WF_list[key].split('_')[1].split('.')[0]
            break

    # FOLDER TYPE for POST:
    for key in post_folders:
        if key in WF_list:
            POST_JOB_ID = WF_list[key].split('_')[1].split('.')[0]
            break

    print('Found job_ID for Pre is:', PRE_JOB_ID)    
    print('Found job_ID for Run is:', RUN_JOB_ID)
    print('Found job_ID for Post is:', POST_JOB_ID)

    # Get the PRE INFO FROM LINUX MACHINE
     # Get the PRE INFO FROM LINUX MACHINE
    try:
        if PRE_JOB_ID is not None:
            pre_linux_info = subprocess.run(['qacct', '-j', str(PRE_JOB_ID)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            usr_name = pre_linux_info.stdout.decode('utf-8')
            lines = usr_name.splitlines()
            if len(lines) >= 15:
                part5 = lines[4].split()
                qt = lines[12].split()
                st = lines[13].split()
                et = lines[14].split()
                pre_owner = part5[1]
                qsub_ent = qt[3], qt[2], qt[5], qt[4]
                pre_qsub_datetime = ' '.join(qsub_ent)
                pre_qsub_dt = datetime.strptime(pre_qsub_datetime, '%d %b %Y %H:%M:%S')

                sim_st = st[3], st[2], st[5], st[4]
                pre_sim_start_time = ' '.join(sim_st)
                pre_start_dt = datetime.strptime(pre_sim_start_time, '%d %b %Y %H:%M:%S')

                sim_end = et[3], et[2], et[5], et[4]
                pre_sim_end_time = ' '.join(sim_end)
                pre_end_dt = datetime.strptime(pre_sim_end_time, '%d %b %Y %H:%M:%S')

                pre_elapse_time = pre_end_dt-pre_start_dt

                print("PRE Owner:", pre_owner)
                print("PRE Qsub Time:", pre_qsub_dt)
                print("PRE Elapse Time:", pre_elapse_time)
        else:
            print("PRE_JOB_ID is None. Skipping PRE job info extraction.")      
    except Exception as e:
        print(f"Error reading the PRE job info: {e}")

    # Get the RUN INFO FROM LINUX MACHINE
    try:
        if RUN_JOB_ID is not None:
            run_linux_info = subprocess.run(['qacct', '-j', str(RUN_JOB_ID)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            usr_name = run_linux_info.stdout.decode('utf-8')
            lines = usr_name.splitlines()
            if len(lines) >= 15:
                part5 = lines[4].split()
                qt = lines[12].split()
                st = lines[13].split()
                et = lines[14].split()
                run_owner = part5[1]
                qsub_ent = qt[3], qt[2], qt[5], qt[4]
                run_qsub_datetime = ' '.join(qsub_ent)
                run_qsub_dt = datetime.strptime(run_qsub_datetime, '%d %b %Y %H:%M:%S')

                sim_st = st[3], st[2], st[5], st[4]
                run_sim_start_time = ' '.join(sim_st)
                run_start_dt = datetime.strptime(run_sim_start_time, '%d %b %Y %H:%M:%S')

                sim_end = et[3], et[2], et[5], et[4]
                run_sim_end_time = ' '.join(sim_end)
                run_end_dt = datetime.strptime(run_sim_end_time, '%d %b %Y %H:%M:%S')

                run_elapse_time = run_end_dt-run_start_dt

                print("RUN Owner:", run_owner)
                print("RUN Qsub Time:", run_qsub_dt)
                print("RUN Elapse Time:", run_elapse_time)
        else:
            print("RUN_JOB_ID is None. Skipping RUN job info extraction.")  
    except Exception as e:
        print(f"Error reading the RUN job info: {e}")

    # Get the POST INFO FROM LINUX MACHINE
    try:
        if POST_JOB_ID is not None:
            post_linux_info = subprocess.run(['qacct', '-j', str(POST_JOB_ID)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            usr_name = post_linux_info.stdout.decode('utf-8')
            lines = usr_name.splitlines()
            if len(lines) >= 15:
                part5 = lines[4].split()
                qt = lines[12].split()
                st = lines[13].split()
                et = lines[14].split()
                post_owner = part5[1]
                qsub_ent = qt[3], qt[2], qt[5], qt[4]
                post_qsub_datetime = ' '.join(qsub_ent)
                post_qsub_dt = datetime.strptime(post_qsub_datetime, '%d %b %Y %H:%M:%S')

                sim_st = st[3], st[2], st[5], st[4]
                post_sim_start_time = ' '.join(sim_st)
                post_start_dt = datetime.strptime(post_sim_start_time, '%d %b %Y %H:%M:%S')

                sim_end = et[3], et[2], et[5], et[4]
                post_sim_end_time = ' '.join(sim_end)
                post_end_dt = datetime.strptime(post_sim_end_time, '%d %b %Y %H:%M:%S')

                post_elapse_time = post_end_dt-post_start_dt

                print("POST Owner:", post_owner)
                print("POST Qsub Time:", post_qsub_dt)
                print("POST Elapse Time:", post_elapse_time)
        else:
            print("POST_JOB_ID is None. Skipping POST job info extraction.")  
    except Exception as e:
        print(f"Error reading the POST job info: {e}")

    # Avoid duplicate username and None:
    user_name_set = {own for own in [pre_owner, run_owner, post_owner] if own is not None}
    user_name = ', '.join(user_name_set) if user_name_set else None
    print("User Name(s):", user_name) 

    # Find the earliest submission date
    first_date = (pre_qsub_dt, run_qsub_dt, post_qsub_dt)
    actual_date = min((dt for dt in first_date if dt), default=None)
    print("Actual Submission Date:", actual_date)

    # --- REPLACED: Saving JOB_ID in the Database ---
    try:
        if run_code: # Only insert if we have a valid run_code
            insert_elapse_time(run_code, project_code, task_code, run_num, workflow_step,
                               actual_date, user_name, PRE_JOB_ID, pre_elapse_time,
                               RUN_JOB_ID, run_elapse_time, POST_JOB_ID, post_elapse_time,
                               connection)
            print("Info from qacct -j JOB_ID has been processed and data inserted.")
    except Exception as e:
        print(f"Error inserting Elapse Time data: {e}")
        connection.rollback()
        
    # Read and insert RTM Simulation Metrics CSV File and Insert the Values into DB
    simulation_files_rtm = find_csv_files(base_dir, post_simulationMetrics)

    for file_path in simulation_files_rtm:
        try:
            print(f"Processing RTM simulation metrics file: {file_path}")
            df = pd.read_csv(file_path, delimiter=';', skiprows=1, header=None)
            df.columns = ['MONITORS', 'RESULTS']
            for index, row in df.iterrows():
                cursor.execute(f"""INSERT INTO {db_table.Staging_Table_Ext_Aero}
                               (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, POST_JOB_ID, MONITORS, RESULTS) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                               (run_code, project_code, task_code, run_num, POST_JOB_ID, str(row['MONITORS']), str(row['RESULTS'])))
                connection.commit()
        except Exception as e:
            print(f"Error processing RTM simulation metrics file {file_path}: {e}")
            connection.rollback()

    # Read and insert POST CFx CSV File and Insert the Values into DB
    simulation_files_cfx = find_csv_files(base_dir, post_CFx)

    for file_path in simulation_files_cfx:
        try:
            print(f"Processing CFx file: {file_path}")
            df = pd.read_csv(file_path, skiprows=1, header=None)
            df.columns = ['ITERATION', 'CFx_Monitor']
            for index, row in df.iterrows():
                cursor.execute(f"""INSERT INTO {db_table.Staging_Aero_CFx}
                               (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, ITERATION, CFx_Monitor) VALUES (%s, %s, %s, %s, %s, %s)""",
                               (run_code, project_code, task_code, run_num, str(row['ITERATION']), str(row['CFx_Monitor'])))
                connection.commit()
        except Exception as e:
            print(f"Error processing CFx file {file_path}: {e}")
            connection.rollback()

    # Read and insert PoOST CFz CSV File and Insert the Values into DB
    simulation_files_cfz = find_csv_files(base_dir, post_CFz)

    for file_path in simulation_files_cfz:
        try:
            print(f"Processing CFz file: {file_path}")
            df = pd.read_csv(file_path, skiprows=1, usecols=[0, 1], header=None)
            df.columns = ['ITERATION', 'CFz_Monitor']
            for index, row in df.iterrows():
                cursor.execute(f"""INSERT INTO {db_table.Staging_Aero_CFz}
                               (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, ITERATION, CFz_Monitor) VALUES (%s, %s, %s, %s, %s, %s)""",
                               (run_code, project_code, task_code, run_num, str(row['ITERATION']), str(row['CFz_Monitor'])))
                connection.commit()
        except Exception as e:
            print(f"Error processing CFz file {file_path}: {e}")
            connection.rollback()

    # Read and insert XWD Simulation Metrics CSV File and Insert the Values into DB
    simulation_files_xwd = [f for f in csv_files_with_path if os.path.basename(f) in post_simulationMetrics_XWD]

    for file_path in simulation_files_xwd:
        try:
            print(f"Processing XWD simulation metrics file: {file_path}")
            df = pd.read_csv(file_path, delimiter=';', skiprows=1, header=None)
            df.columns = ['MONITORS', 'RESULTS']
            for index, row in df.iterrows():
                cursor.execute(f"""INSERT INTO {db_table.Staging_Table_Ext_Aero}
                               (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, MONITORS, RESULTS) VALUES (%s, %s, %s, %s, %s, %s)""",
                               (run_code, project_code, task_code, run_num, str(row['MONITORS']), str(row['RESULTS'])))
                connection.commit()
        except Exception as e:
            print(f"Error processing XWD simulation metrics file {file_path}: {e}")
            connection.rollback()


    # Read and insert all 'cumulated' CSV File and Insert the Values into DB
    cumulated_files = find_csv_files(base_dir, tuple(post_cummulative))

    for file_path in cumulated_files:
        try:
            post_csv_file_name = os.path.basename(file_path)
            print(f"Processing cumulated file: {file_path}")
            df = pd.read_csv(file_path, skiprows=1, header=None)
            df.columns = ['POSITION_m', 'FORCE_N', 'ACCUMULATED_FORCE_N', 'PROFILE_LOWER_m', 'PROFILE_UPPER_m']
            for index, row in df.iterrows():
                cursor.execute(f"""INSERT INTO {db_table.Staging_Table_Cummulative}
                               (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, POST_CSV_FILE_NAME, POSITION_m, FORCE_N, ACCUMULATED_FORCE_N, PROFILE_LOWER_m, PROFILE_UPPER_m)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                               (run_code, project_code, task_code, run_num, post_csv_file_name, row['POSITION_m'], row['FORCE_N'], row['ACCUMULATED_FORCE_N'], row['PROFILE_LOWER_m'], row['PROFILE_UPPER_m']))
                connection.commit()
        except Exception as e:
            print(f"Error processing cumulated file {file_path}: {e}")
            connection.rollback()

    # Read and insert Residuals CSV File and Insert the Values into DB
    residual_files = find_csv_files(base_dir, post_residuals)

    for file_path in residual_files:
        try:
            print(f"Processing residual file: {file_path}")
            df = pd.read_csv(file_path, skiprows=1, header=None)
            df.columns = ['ITERATION', 'Tdr_RESIDUAL', 'Tke_RESIDUAL', 'CONTINUITY', 'X_MOMENTUM', 'Y_MOMENTUM', 'Z_MOMENTUM']
            for index, row in df.iterrows():
                cursor.execute(f"""INSERT INTO {db_table.Staging_Table_Residuals}
                                (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, ITERATION, CONTINUITY, X_MOMENTUM, Y_MOMENTUM, Z_MOMENTUM, Tke_RESIDUAL, Tdr_RESIDUAL)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                (run_code, project_code, task_code, run_num, str(row['ITERATION']), str(row['CONTINUITY']), str(row['X_MOMENTUM']), str(row['Y_MOMENTUM']), str(row['Z_MOMENTUM']), str(row['Tke_RESIDUAL']), str(row['Tdr_RESIDUAL'])))
                connection.commit()
        except Exception as e:
            print(f"Error processing residual file {file_path}: {e}")
            connection.rollback()

    # Read and insert HEAD PRESSURE PULSE CSV File and Insert the Values into DB
    hpp_files = find_csv_files(base_dir, post_head_Pr_pulse)

    for file_path in hpp_files: # Loop through found HPP files, though typically only one is expected
        try:
            print(f"Processing HEAD PRESSURE PULSE file: {file_path}")
            df = pd.read_csv(file_path, skiprows=1, header=None)
            df.columns = [
                          'Line_Probe_1500mm_Direction', 'Line_Probe_1500mm_Pressure',
                          'Line_Probe_1800mm_Direction', 'Line_Probe_1800mm_Pressure',
                          'Line_Probe_2100mm_Direction', 'Line_Probe_2100mm_Pressure',
                          'Line_Probe_2400mm_Direction', 'Line_Probe_2400mm_Pressure',
                          'Line_Probe_2700mm_Direction', 'Line_Probe_2700mm_Pressure',
                          'Line_Probe_3000mm_Direction', 'Line_Probe_3000mm_Pressure',
                          'Line_Probe_3300mm_Direction', 'Line_Probe_3300mm_Pressure'
                          ]
            for index, row in df.iterrows():
                cursor.execute(f"""INSERT INTO {db_table.Staging_Table_head_Pr_pulse}
                               (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER,
                                Line_Probe_1500mm_Direction, Line_Probe_1500mm_Pressure,
                                Line_Probe_1800mm_Direction, Line_Probe_1800mm_Pressure,
                                Line_Probe_2100mm_Direction, Line_Probe_2100mm_Pressure,
                                Line_Probe_2400mm_Direction, Line_Probe_2400mm_Pressure,
                                Line_Probe_2700mm_Direction, Line_Probe_2700mm_Pressure,
                                Line_Probe_3000mm_Direction, Line_Probe_3000mm_Pressure,
                                Line_Probe_3300mm_Direction, Line_Probe_3300mm_Pressure)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                               (run_code, project_code, task_code, run_num,
                                str(row['Line_Probe_1500mm_Direction']), str(row['Line_Probe_1500mm_Pressure']),
                                str(row['Line_Probe_1800mm_Direction']), str(row['Line_Probe_1800mm_Pressure']),
                                str(row['Line_Probe_2100mm_Direction']), str(row['Line_Probe_2100mm_Pressure']),
                                str(row['Line_Probe_2400mm_Direction']), str(row['Line_Probe_2400mm_Pressure']),
                                str(row['Line_Probe_2700mm_Direction']), str(row['Line_Probe_2700mm_Pressure']),
                                str(row['Line_Probe_3000mm_Direction']), str(row['Line_Probe_3000mm_Pressure']),
                                str(row['Line_Probe_3300mm_Direction']), str(row['Line_Probe_3300mm_Pressure'])))
            connection.commit()
        except Exception as e:
            print(f"Error reading or inserting HEAD PRESSURE PULSE CSV file {file_path}: {e}")
            connection.rollback()


    print("All CSV files have been processed and data inserted.")

    # Read the PARAMETERS.txt file from the location:
    def read_param(base_dir, target_file = 'parameters.txt'):
        keywords = {'DESCRIPTION:': None, 'SOLVER_VERSION:': None, 'QUEUE:': None, 'WORKFLOW_STEPS:': None, 'TEMPLATE:': None}
        for root, dirs, files in os.walk(base_dir):
            if target_file in files:
                file_path = os.path.join(root, target_file)
                print(f"Found {target_file} from the path: {file_path}")
                try:
                    with open(file_path, 'r') as file:
                        lines = file.readlines()
                        for line in lines:
                            for key in keywords:
                                if line.strip().startswith(key):
                                    keywords[key] = line.strip().split(key, 1)[-1].strip()
                    # Check all keys *except* WORKFLOW_STEPS, which was already checked
                    non_workflow_keys = {k: v for k, v in keywords.items() if k != 'WORKFLOW_STEPS:'}
                    if all(value is not None for value in non_workflow_keys.values()):
                        return keywords
                    else:
                            print(f"Warning: Not all parameters were found in {target_file} at {file_path}. Missing keys might cause issues.")
                            return keywords # Return what was found
                except Exception as e:
                    print(f"Error reading the file {file_path}: {e}")
                break # Break after finding the first parameters.txt
        return None
    # Read PARAMETERS TXT File and upload the data to the Database
    params = read_param(base_dir)
    if params:
        try:
            # Map found parameters, which might be None if file was incomplete
            mapped_params = {
                'JOB_DESCRIPTION': params.get('DESCRIPTION:'), 
                'SOLVER_VERSION': params.get('SOLVER_VERSION:'), 
                'QUEUE_TYPE': params.get('QUEUE:'), 
                'WORKFLOW_STEPS': params.get('WORKFLOW_STEPS:'), # This was already found
                'TEMPLATE': params.get('TEMPLATE:')
            }
            df = pd.DataFrame([mapped_params], columns=['JOB_DESCRIPTION', 'SOLVER_VERSION', 'QUEUE_TYPE', 'WORKFLOW_STEPS', 'TEMPLATE'])
            for index, row in df.iterrows():
                # Use the main cursor, no need to create a new one here
                cursor.execute(f"""INSERT INTO {db_table.Staging_Parameters}
                              (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, JOB_DESCRIPTION, SOLVER_VERSION, QUEUE_TYPE, WORKFLOW_STEPS, TEMPLATE)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (run_code, project_code, task_code, run_num, row['JOB_DESCRIPTION'], row['SOLVER_VERSION'], row['QUEUE_TYPE'], row['WORKFLOW_STEPS'], row['TEMPLATE']))
                connection.commit()
            print("Parameter file have been processed and data inserted.")
        except Exception as e:
            print(f"Error processing parameters from parameters.txt: {e}")
            connection.rollback()
    else:
        print("Parameters.txt not found or could not be read. Skipping parameter insertion.")

    # --- Fetch Elapse Time Info ---

    
    # --- Find FLAGS in each subfolder and add them in the database ---
    def find_flags_in_subfolders(base_dir, possible_tags):
        folder_to_flag = {}
        if not os.path.isdir(base_dir):
            print(f"Base directory does not exist or is not a directory: {base_dir}")
            return folder_to_flag
        for folder_names in os.listdir(base_dir):
            subfolder_path = os.path.join(base_dir, folder_names)
            if os.path.isdir(subfolder_path):
                found_flag_in_subfolder = False
                for item in os.listdir(subfolder_path):
                    for flag in possible_tags:
                        if item.startswith(flag):
                            folder_to_flag[folder_names] = flag
                            found_flag_in_subfolder = True
                            break
                    if found_flag_in_subfolder:
                        break
        return folder_to_flag

    flag_map = find_flags_in_subfolders(base_dir, possible_tags)
    print("The tags found are", flag_map)

    def get_tag_from_group(flag_map, group):
        for fol in group:
            if fol in flag_map:
                return flag_map[fol]
        return None

    # --- Single-line insertion logic for Table_Flag, reflecting the current job state ---
    if run_code:
        try:
            pre_tag = get_tag_from_group(flag_map, pre_folders)
            run_tag = get_tag_from_group(flag_map, run_folders)
            post_tag = get_tag_from_group(flag_map, post_folders)

            # Start with all NULLs, set only the detected tags for this workflow run
            final_pre_tag, final_run_tag, final_post_tag = None, None, None
            steps_to_run = workflow_step.upper()
            tag_found = False
            
            if 'PRE' in steps_to_run and pre_tag:
                final_pre_tag = pre_tag
                tag_found = True
            if 'RUN' in steps_to_run and run_tag:
                final_run_tag = run_tag
                tag_found = True
            if 'POST' in steps_to_run and post_tag:
                final_post_tag = post_tag
                tag_found = True

            # For 'ALL', only insert if ALL three flags are present
            if 'ALL' in steps_to_run:
                if not (pre_tag and run_tag and post_tag):
                    tag_found = False # Wait until all done
                else:
                    final_pre_tag = pre_tag
                    final_run_tag = run_tag
                    final_post_tag = post_tag
                    tag_found = True

            if tag_found:
                # Use the main cursor for this final operation
                cursor.execute(f"""
                    INSERT INTO {db_table.Table_Flag}
                    (RUN_CODE, PROJECT_CODE, TASK_CODE, RUN_NUMBER, PRE_TAG, RUN_TAG, POST_TAG)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (run_code, project_code, task_code, run_num,
                      final_pre_tag, final_run_tag, final_post_tag))
                connection.commit()
                print(f"New row INSERTED for {run_code} with final status for job: {steps_to_run}.")
            else:
                print("No relevant finished steps found to insert a new tag record for this job submission.")

        except Exception as e:
            print(f"Error inserting TAG data: {e}")
            connection.rollback()
        finally:
            # Close cursor and connection here at the very end
            cursor.close()
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 db_workflow.py <job_folder_path>")
        sys.exit(1)
    job_folder_path = sys.argv[1]
    process_job_data(job_folder_path)