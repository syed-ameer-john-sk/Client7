import pymysql
import os
import pandas as pd
from db_connection import get_db_connection

def read_param(base_dir, target_file='parameters.txt'):
    """Reads parameters from the specified file."""
    file_path = os.path.join(base_dir, target_file)
    keywords = {
        'PROJECT_CODE': None, 'TASK_CODE': None, 'DESCRIPTION': None,
        'SOLVER_VERSION': None, 'QUEUE': None, 'WORKFLOW_STEPS': None,
        'TEMPLATE': None, 'ITERATOR': None
    }

    try:
        with open(file_path, 'r') as file:
            for line in file:
                clean_line = line.strip().replace('\\_', '_').replace('\\-', '-')
                for key in keywords:
                    if clean_line.startswith(f"{key}:"):
                        value = clean_line.split(":", 1)[-1].strip()
                        keywords[key] = value if value else None
        return keywords
    except Exception as e:
        print(f"Error reading the file {file_path}: {e}")
        return None

def get_run_number(base_dir):
    """
    Main function to orchestrate reading params, updating the DB,
    and generating the run number.
    """
    connection = get_db_connection()
    if not connection:
        print("Failed to get database connection.")
        return None

    params = read_param(base_dir)
    if not params:
        print("Could not read parameters.")
        connection.close()
        return None

    Table_Auto_run_number = 'Auto_Run_Num'

    try:
        cursor = connection.cursor()
        
        # Insert parameters into the database
        mapped_params = {
            'PROJECT_CODE': params['PROJECT_CODE'],
            'TASK_CODE': params['TASK_CODE'],
            'JOB_DESCRIPTION': params['DESCRIPTION'],
            'SOLVER_VERSION': params['SOLVER_VERSION'],
            'QUEUE_TYPE': params['QUEUE'],
            'WORKFLOW_STEPS': params['WORKFLOW_STEPS'],
            'TEMPLATE': params['TEMPLATE'],
            'ITERATOR': params['ITERATOR']
        }
        df = pd.DataFrame([mapped_params])

        for index, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {Table_Auto_run_number}
                (PROJECT_CODE, TASK_CODE, JOB_DESCRIPTION, SOLVER_VERSION, QUEUE_TYPE, WORKFLOW_STEPS, TEMPLATE, ITERATOR)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['PROJECT_CODE'], row['TASK_CODE'], row['JOB_DESCRIPTION'],
                  row['SOLVER_VERSION'], row['QUEUE_TYPE'], row['WORKFLOW_STEPS'],
                  row['TEMPLATE'], row['ITERATOR']))
        connection.commit()
        print("Parameter file has been processed and data inserted.")

        # Assign run number
        run_number = assign_run_number(cursor, connection, Table_Auto_run_number)
        return run_number

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        if connection:
            connection.close()

def get_existing_runs(cursor, project_code, task_code):
    """Fetches existing runs for a given project and task code."""
    cursor.execute("""
        SELECT * FROM Auto_Run_Num
        WHERE PROJECT_CODE = %s AND TASK_CODE = %s AND RUN_NUMBER IS NOT NULL
        ORDER BY id DESC
    """, (project_code, task_code))
    return cursor.fetchall()

def generate_run_number(cursor, project_code, task_code):
    """Generates a new run number."""
    cursor.execute("""
        SELECT RUN_NUMBER FROM Auto_Run_Num
        WHERE PROJECT_CODE = %s AND RUN_NUMBER IS NOT NULL
        ORDER BY id DESC
    """, (project_code,))
    all_runs = cursor.fetchall()

    max_num = 0
    for run in all_runs:
        try:
            parts = run[0].split('-')
            num = int(parts[-1])
            if num > max_num:
                max_num = num
        except (ValueError, IndexError):
            continue

    return f"{project_code}-{task_code}-{max_num + 1:03d}"

def assign_run_number(cursor, connection, table_name):
    """Assigns a run number based on the defined logic."""
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 1")
    latest = cursor.fetchone()
    if not latest:
        print("No recent entry found.")
        return None

    id, project_code, task_code, _, job_description, _, _, workflow_steps, _, iterator, *_ = latest
    workflow_steps = workflow_steps.upper() if workflow_steps else ""
    
    previous_entries = get_existing_runs(cursor, project_code, task_code)
    run_number = None

    # Decision Logic
    if workflow_steps in ['PRE', 'PRE RUN', 'ALL']:
        jd_match = [entry for entry in previous_entries if entry[4] == job_description]
        if jd_match:
            run_number = "DUMMY_XXX_000"  # Job description already exists, assign fixed dummy run number
        else:
            run_number = generate_run_number(cursor, project_code, task_code) # Generate New run number
    
    elif workflow_steps in ['RUN', 'RUN POST']:
        jd_match = [entry for entry in previous_entries if entry[4] == job_description]
        if jd_match:
            pre_exists = any('PRE' in (entry[7] or "").upper() for entry in jd_match)
            run_exists = any('RUN' in (entry[7] or "").upper() for entry in jd_match)
            iterator_has_value = any(entry[9] for entry in jd_match)

            if pre_exists or (run_exists and iterator_has_value):
                run_number = jd_match[0][3]
            else:
                run_number = generate_run_number(cursor, project_code, task_code)
        else:
            run_number = generate_run_number(cursor, project_code, task_code)
    elif workflow_steps == 'POST':
        jd_match = [entry for entry in previous_entries if entry[4] == job_description]
        if jd_match:
            run_number = jd_match[0][3]
        else:
            run_number = generate_run_number(cursor, project_code, task_code)

    if run_number:
        cursor.execute(f"UPDATE {table_name} SET RUN_NUMBER = %s WHERE id = %s", (run_number, id))
        connection.commit()
        print(f"RUN_NUMBER assigned: {run_number}")
    else:
        print("No RUN_NUMBER assigned based on logic.")
        
    return run_number