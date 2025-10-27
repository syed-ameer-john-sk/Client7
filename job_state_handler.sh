#!/bin/sh
# Moved SCRIPT_PATH definition to the very top to ensure it's always available
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd -P)/$(basename "$0")"
SCRIPT_DIR_NAME="$(dirname "${SCRIPT_PATH}")" # Directory where the script itself resides

rename_step_folder() 
{
    step_name="$(basename "${STEP_FOLDER}")"
    dst="${JOB_FOLDER}/$step_name-$1" 
    if [[ -d $dst ]]; then 
        echo "nothing yet..."
        # TODO ? 
    fi 
    mv "${STEP_FOLDER}" "$dst" 2> /dev/null 
    STEP_FOLDER=$dst
}

delete_folder() 
{
    # Might be good to make some verification before using rm 
    rm -rf --preserve-root "$1"  
}

move_to_delete()
{
    # Rename folder $1 to $1-TO_DELETE
    mv "$1" "$1-TO_DELETE" 2> /dev/null
}

get_cron_output()
{
    CRON_OUTPUT=$(crontab -l 2> /dev/null) 
}

get_qstat_output()
{
    QSTAT_OUTPUT=$(source $HOME/.bash_profile && /opt/sge/bin/lx-amd64/qstat -u "$(whoami)")
}

add_cron_task()
{ 
    cron_task=$(echo "${CRON_OUTPUT}" | grep "${DEFAULT_CRONTASK}")
    if [[ $(echo "$cron_task" | wc -w) -eq 0 ]]; then 
        echo "1"
        (echo -n "$CRON_OUTPUT" ; echo "${DEFAULT_CRONTASK} \"${JOB_FOLDER}\"") | crontab -
    else 
        cron_job=$(echo "$cron_task" | grep -oP "\"${JOB_FOLDER//"/"/\/}\"" | wc -l)  
        if [[ $cron_job -eq 0 ]]; then 
            echo "2"
            (echo -n "$CRON_OUTPUT" | grep -v "$cron_task" ; echo "$cron_task \"${JOB_FOLDER}\"") | crontab - 
        fi 
    fi 
}

remove_cron_job() 
{
    get_cron_output
    cron_task=$(echo "${CRON_OUTPUT}" | grep "${DEFAULT_CRONTASK}")
    cron_job=$(echo "$cron_task" | grep -oP "\"${JOB_FOLDER//"/"/\/}\"")
#   echo "CRON OUTPUT: $CRON_OUTPUT"
#   echo "DEFAULT CRONTASK: $DEFAULT_CRONTASK"
#   echo "cron task: $cron_task"
#   echo "cron job: $cron_job"
    if [[ "x$cron_job" == "x" ]]; then 
        return 
    fi 
    cron_task=${cron_task/$cron_job/}
    is_not_empty=$(echo "$cron_task" | wc -w)
    if [[ $is_not_empty -eq 7 ]]; then 
#       echo "remove 1"
        echo -n "${CRON_OUTPUT}" | grep -v "${DEFAULT_CRONTASK}" | crontab -
    else 
#       echo "remove 2"
        (echo -n "${CRON_OUTPUT}" | grep -v "${DEFAULT_CRONTASK}" ; echo "$cron_task") | crontab - 
    fi
    get_cron_output
#   echo "CURENT CRONTAB: $CRON_OUTPUT"
} 

create_job_infos()
{
    job_step="${JOB_FOLDER}/.JOB_STEPS"
    if [[ ! -f "$job_step" ]]; then 
        touch "$job_step" 2> /dev/null 
    fi 
}

is_job_info_empty()
{
    job_steps="${JOB_FOLDER}/.JOB_STEPS"
    if [[ -f $job_steps ]]; then 
        nb_c=$(cat "$job_steps" | grep -oP '[a-zA-Z0-9,]*' | wc -c)
        if [[ $nb_c -gt 0 ]]; then 
            return 0
        fi 
    fi 
    return 1 
}

add_job_info() 
    # $1: STEP,JOB_ID 
{
    echo -n "$1 " >> "${JOB_FOLDER}/.JOB_STEPS" 2> /dev/null 
} 

remove_job_steps()
{
    rm -rf "${JOB_FOLDER}/.JOB_STEPS" 
}

remove_job_info() # remove cron task if file not empty 
{ 
    is_empty=$(is_job_info_empty)
    if [[ $is_empty -eq 0 ]]; then 
        job_infos=$(cat "${JOB_FOLDER}/.JOB_STEPS")
        step_info=$(echo "$job_infos" | grep -oP "$1,[0-9]+ ") # the ended space should be taken
        job_infos=${job_infos/$step_info/}
#       echo "REMOVE JOB INFO: $job_infos -- $step_info -- $job_infos"
        echo -n "$job_infos" > "${JOB_FOLDER}/.JOB_STEPS" 2> /dev/null # -n to remove the trailing newline
    fi 
} 

get_job_infos() 
{
    echo -n "$(cat "${JOB_FOLDER}/.JOB_STEPS")" 
}

get_sim_file() 
    # $1 = target path 
{
    step=$(basename "$1") 
    DIR_FILES=$(ls -rt $1/*.sim 2> /dev/null)  
    if [[ "x${DIR_FILES}" == "x" ]]; then 
        echo "None"
        exit 1;
    fi 
    case $step in 
        "PRE") 
            target=$(echo "${DIR_FILES}" | grep "@meshed.sim");;
        "RUN") 
            target=$(echo "${DIR_FILES}" | tail -n1);;
        *)  
            target=""
    esac 
    if [[ "x$target" == "x" ]]; then 
        echo "None"
    else 
        echo "$target"
    fi
}

create_symlink()
    # $1 = target path 
{
    step=$(basename "$1")
    JOB_FOLDER=$(dirname "$1") 
    link_name=$(basename "${JOB_FOLDER}")
    DIR_FILES=$(ls -rt $1/*.sim 2> /dev/null)  
    if [[ "x${DIR_FILES}" == "x" ]]; then 
        echo "ERROR"
        exit 1;
    fi 

    case $step in 
        "PRE") 
            target=$(echo "${DIR_FILES}" | grep "@meshed.sim")
            nextStep="RUN";;
        "RUN") 
            target=$(echo "${DIR_FILES}" | tail -n1) 
            nextStep="POST";; 
        *)  
            return ;;
    esac 

    if [[ "x$target" == "x" ]]; then 
        echo "ERROR"
        #echo "Cannot find sim file in previous step folder"
        exit 1; 
    fi 
    target=$(basename "$target")
    
    #job_steps="${JOB_FOLDER}/.JOB_STEPS"
    #if [[ -e $job_steps ]]; then
    #   is_link_needed=$(cat "$job_steps" | grep "$nextStep" | wc -l)
    #   if [[ $is_link_needed -eq 0 ]]; then
    #       echo "INFO"
    #       echo "Link not created because there is no next step to run"
    #       exit 0;
    #   fi
    #fi
    link_folder="${JOB_FOLDER}/$nextStep"
    if [[ ! -d $link_folder ]]; then 
        echo "INFO"
        echo "Folder $nextStep does not exist."
        echo "Cannot create link."
        exit 1; 
    fi
    echo "dir files: ${DIR_FILES}"
    echo "job folder: ${JOB_FOLDER}"
    echo "target: $1/$target"
    echo "sim file: $link_folder/$link_name.sim" 
    ln -sf "$1/$target" "$link_folder/$link_name.sim"  
} 

delete_job() 
{
    # use ATOS qdel ? 
    qdel "${JOB_ID}" 
}

detect_job_status()
{
    # RETURN CODES

    # 0: RUNNING
    # action => No action

    # 1: FINISHED WITHOUT ERROR - FINISHED file found (without other info file)
    # action => remove_job_info for the current step

    ########################################################################

    # Exception during calculation
    # 2: FINISHED WITH ERROR - FINISHED + FAILED file found
    # action => remove_job_info + clean other step + rename folder -FAILED

    # Active job killed by user
    # 3: ABORTED - FINISHED + ABORT file found
    # action => rename folder -ABORTED

    # Walltime Exceded
    # 4: TERMINATED + ABORT file found
    # action => rename foler -ABORTED

    # Current step pending job killed by user
    # TERMINATED + TO-DELETE file found
    # 5: action => rename foler -ABORTED

    # JOB ERROR - status Error in qstat
    # 6: action => qdel + remove_job_info (+ clean other step)

    # JOB plus dans la file
    # 7: action => create TO-DELETE file in the job folder

    if [[ "x${QSTAT_OUTPUT}" == "x" ]]; then
        get_qstat_output
    fi
#   echo "QSTAT_OUTPUT: ${QSTAT_OUTPUT}"

    is_job_error=$(echo ${QSTAT_OUTPUT} | awk '{ if ($1 == '${JOB_ID}') { print $5 } }' | grep '^E' | wc -l)
    if [[ $is_job_error -gt 0 ]]; then
        return 6 # JOB ERROR (qdel)
    fi

    is_job_running=$(echo "${QSTAT_OUTPUT}" | awk '{print $1}' | grep "${JOB_ID}" | wc -l)
    if [[ $is_job_running -ne 0 ]]; then
        return 0 # RUNNING
    fi

    job_state=$(echo "${DIR_FILES}" | grep -o "FINISHED" | wc -l) #02/2023: FINISHED cree par Atos.
    if [[ $job_state -gt 0 ]]; then
        job_state=$(echo "${DIR_FILES}" | grep -o "FAILED" | wc -l)
        if [[ $job_state -gt 0 ]]; then
            return 2 # FINISHED WITH ERROR - FINISHED + FAILED file found
        fi
        job_state=$(echo "${DIR_FILES}" | grep -o "ABORT" | wc -l)
        if [[ $job_state -gt 0 ]]; then
            return 3 # ABORTED - FINISHED + ABORT file found
        fi
        return 1 # FINISHED WITHOUT ERROR
    fi

    job_state=$(echo "${DIR_FILES}" | grep -o "TERMINATED" | wc -l)
    if [[ $job_state -gt 0 ]]; then
        job_state=$(echo "${DIR_FILES}" | grep -o "ABORT" | wc -l)
        if [[ $job_state -gt 0 ]]; then
            return 4 # TERMINATED + ABORT file found
        fi
        job_state=$(echo "${DIR_FILES}" | grep -o "TO-DELETE" | wc -l)
        if [[ $job_state -gt 0 ]]; then
            return 5 # TERMINATED + TO-DELETE file found
        fi
        echo "[ERROR] Should not happen"
        return 10 # ERROR WORKFLOW
    fi

    #02/2023: supprime. Job dans qstat mais non detecte. 
    is_job_running=$(echo "${QSTAT_OUTPUT}" | awk '{print $1}' | grep "${JOB_ID}" | wc -l)  
    #echo "RES1: ${QSTAT_OUTPUT}"
    #echo "JOB ID1: ${JOB_ID}"
    if [[ $is_job_running -eq 0 ]]; then 
        if [[ "x${STEP_FOLDER}" != "x" ]]; then 
            touch "${STEP_FOLDER}/TERMINATED" 2> /dev/null 
        fi 
        return 7 # NOT FINISHED - ERROR 
    fi 
    return 0 # RUNNING
}

wait_script_end()
{
    username="$(whoami)"
    while [ $(pgrep -u $username -f $(basename ${SCRIPT_PATH}) -c) -gt 1 ]
    do 
#       echo "waiting 5 seconds..."
        sleep 5
    done
}

is_empty_array () {
    # $1: the array to test
    array=$1
    for elem in ${array[@]}; do
        if [ -n "$(echo $elem | tr -d ' \r\t\n')" ]; then
            return 1
        fi
    done
    return 0
}

remove_sim_file_pre_step() {
    # Remove the sim file in PRE step if needed
#   echo "[DEBUG] test remove /PRE/*.sim(~?) file"
    if [[ "$CURRENT_STEP" == "RUN" ]]; then
#       echo "[DEBUG] CURRENT_STEP is effectively RUN"
        for entry in $(ls $STEP_FOLDER/*.sim); do
#           echo "[DEBUG] entry: $entry"
            if [[ ! -L $entry ]]; then
#               echo "[DEBUG] entry is not a symbolic link"
#               rm -f $JOB_FOLDER/PRE/*@meshed.sim #20250417SB
                rm -f $JOB_FOLDER/PRE/*@meshed.sim~
#               rm -f $JOB_FOLDER/PRE/*.sim #20240429SB
                break
            fi
        done
    fi
}

# JOB STATE FILES 
# ABORT & TERMINATED = ERROR -> SHOULD STOP NEXT STEPS 
# FINISHED -> DETECT POSSIBLE FAILURE TO KNOW IF ERROR 

# ABORT STATES:
# 1: TERMINATED BEFORE RUN
# 2: TERMINATED DURING RUN 
# 3: WALLTIME EXCEEDED 

usage()  
{
    script_name=$(basename "$0")
    echo "Usage: bash $script_name [-a <root_job_folder> <step_folder_path,job_id> ...] [-u <root_job_folder> <step_folder_path,job_id> ...]      "
    echo "                                                                                                                                      "
    echo "root_folder is the root folder path of a job                                                                                          "
    echo "args represent the folder path of the job step and the job id                                                                         "
    echo "                                                                                                                                      "
    echo "-h, --help       Display this help                                                                                                    "
    echo "-a, --add      \"root_job_folder\"                                        Add this script as a cron task                                          "
    echo "-r, --remove   \"root_job_folder\" \"step_name\" ...                      Update the current cron task with new arguments                     "
    echo "-m, --macro    \"root_job_folder\" \"step_name\"                          Check the state of the step                                           "
    echo "-w, --workflow \"root_job_folder\" \"step_name\"                          Display a green light for the workflow if the step has no errors "
    echo "-j, --jobid    \"root_job_folder\" \"step_name\"                          Display the job_id attributed for the step job, otherwise ERROR  "
}

# Dynamically determine the path to tzs.py
# Assumes tzs.py is in the same directory as job_state_handler.sh
TZS_SCRIPT_DIR="$(dirname "${SCRIPT_PATH}")"
TZS_SCRIPT_PATH="${TZS_SCRIPT_DIR}/db_workflow.py"


run_tzs_script() {
    
    if [ ! -f "${TZS_SCRIPT_PATH}" ]; then
        
        return 1
    fi
    if [ -z "${JOB_FOLDER}" ] || [ ! -d "${JOB_FOLDER}" ]; then

        return 1
    fi

    
    python3 "${TZS_SCRIPT_PATH}" "${JOB_FOLDER}"
    local tzs_exit_code=$?
    if [ $tzs_exit_code -ne 0 ]; then
        echo "TZS file Failed"
    else
        echo "tzs.py completed successfully"
    fi
    return $tzs_exit_code
}


# MAIN 

if [ $# == 0 ]; then 
    echo "Missing arguments."
    echo "Use -h or --help for more information."
    exit 1
fi 

SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd -P)/$(basename "$0")"
DEFAULT_CRONTASK="* * * * * bash ${SCRIPT_PATH}"
JOB_FOLDER=
STEP_FOLDER=
JOB_ID=
CRON_ARGS=
QSTAT_OUTPUT=
DIR_FILES=

wait_script_end

case "$1" in 
    -h|--help)      
        usage   
        exit 0;;
    -f)
        get_sim_file "$2";; 
    -w|--workflow)
        # call this option only if next step depends on previous step 

        JOB_FOLDER="$2"  
        previous_step="$3" 
        create_job_infos
        if [[ ! -d "${JOB_FOLDER}/$previous_step" ]]; then 
            echo "STOP"
            exit 0 
        fi
        get_cron_output 
        steps=$(get_job_infos)
#       echo "job infos: $steps"
        for step in ${steps[@]}; do
            STEP_FOLDER="${JOB_FOLDER}/${step%,*}"
            JOB_ID=${step##*,}  
#           echo "step folder: ${STEP_FOLDER}"
#           echo "JOB ID: ${JOB_ID}"
            DIR_FILES=$(ls -Ap "${STEP_FOLDER}") 
            detect_job_status
            status=$?
#           echo "status: $status"
            if [[ $status -gt 1 ]]; then 
                echo "STOP" # problem with previous steps, cannot submit next step 
                exit 0
            fi 
        done 
        echo "OK" # because workflow submit next step only when previous steps are done without errors 
        echo "${JOB_ID}" # job id only if previous step is running 
        ;;
    -m|--macro) 
        JOB_FOLDER="$2" 
        create_job_infos
        STEP_FOLDER="${JOB_FOLDER}/$3"  
        steps=$(get_job_infos)
        step=$(echo "$steps" | grep -oP "$3,[0-9]+") 
        is_step_processed=$(echo "$step" | wc -w) 
#       echo "steps: $steps"
#       echo "step: $step"
        if [[ $is_step_processed -eq 0 ]]; then 
            echo "1"
            echo "OK"
            exit 0
        fi
        JOB_ID=${step##*,} 
        DIR_FILES=$(ls -Ap "${STEP_FOLDER}")
        detect_job_status 
        if [[ $? -ne 1 ]]; then 
            echo "STOP"
        else 
            echo "2"
            echo "OK"
        fi 
        ;;
    -j|--jobid) 
        JOB_FOLDER="$2" 
        STEP_FOLDER="${JOB_FOLDER}/$3"  
        steps=$(get_job_infos)
        step=$(echo "$steps" | grep -oP "$3,[0-9]+")
        JOB_ID=${step/$3,/}
        re='^[0-9]+$'
        if ! [[ $JOB_ID =~ $re ]] ; then
            echo "ERROR"
        else
            echo $JOB_ID
        fi
        ;;
    -a|--add)       
        # cron args = "pre,33" "run,34" ...
        JOB_FOLDER="$2";
        create_job_infos
        ARGS="${@:3}"
#       echo "args: $ARGS"
        get_cron_output 
        for arg in ${ARGS[@]}; do
            CRON_ARGS="${arg%,*}"
            add_cron_task 
        done 
        for arg in ${ARGS[@]}; do
            add_job_info "$arg"
        done 
        ;;
    -r|remove)
        # cron args = "pre" "run" ... 
        JOB_FOLDER="$2"
        CRON_ARGS="${@:3}"
        get_cron_output
        for arg in ${CRON_ARGS[@]}; do
            remove_job_info "$arg"
        done
        remove_cron_job
        ;;
    -s|--symlink) 
        create_symlink "$2" 
        exit 0;; 
    -*)               
        echo "Invalid argument: $1"
        exit 1;;  

    # =========================================================================
    # ==                 UPDATED SCRIPT LOGIC STARTS HERE                    ==
    # =========================================================================
    *)
        job_folders="${@:1}"; 
        source $HOME/.bash_profile
        get_cron_output
        for JOB_FOLDER in ${job_folders[@]}; do

            steps=$(get_job_infos)
            if is_empty_array "$steps"; then
                # This block handles the case where the cron job still exists but the .JOB_STEPS file is already empty.
                # This could happen if cleanup was interrupted on a previous run.
                # We assume the TZS script was already run or is no longer needed.
                remove_job_steps
                remove_cron_job
                continue
            fi

            # Loop through all currently active steps for the JOB_FOLDER
            for step in ${steps[@]}; do
                STEP_FOLDER="${JOB_FOLDER}/${step%,*}"
                CURRENT_STEP="${step%,*}"
                JOB_ID="${step##*,}" 
                DIR_FILES=$(ls -Ap "${STEP_FOLDER}")

                detect_job_status
                status=$?
                # echo "job status: $status"

                if [[ $status -eq 0 ]]; then
                    # Status 0: Job is still running. Do nothing and check again on the next cron run.
                    # We 'break' here because there's no need to check other jobs in this JOB_FOLDER;
                    # we must wait for this one to finish first.
                    break
                elif [[ $status -eq 1 ]]; then
                    # Status 1: Job finished successfully.
                    # Remove its entry from .JOB_STEPS.
                    remove_job_info "${step%,*}"
                    remove_sim_file_pre_step
                    # The run_tzs_script call is REMOVED from here.

                elif [[ $status -gt 1 ]]; then
                    # 2: remove_job_info + clean other step + rename folder -FAILED
                    if [[ $status -eq 2 ]]; then
                        # TODO: rename folder -FAILED
                        echo "2 - rename folder -FAILED"
                    fi
                    # 3: ABORTED - FINISHED + ABORT file found
                    if [[ $status -eq 3 ]]; then
                         # TODO: rename folder -ABORTED
                        echo "3 - rename folder -ABORTED"
                    fi
                    # 4: TERMINATED + ABORT file found
                    if [[ $status -eq 4 ]]; then
                        # TODO: rename foler -ABORTED
                        echo "4 - rename foler -ABORTED"
                    fi
                    # 5: TERMINATED + TO-DELETE file found
                    if [[ $status -eq 5 ]]; then
                        # TODO: rename foler -ABORTED
                        echo "5 - rename foler -ABORTED"
                    fi
                    # 6: JOB ERROR - status Error in qstat
                    if [[ $status -eq 6 ]]; then
                        # TODO: qdel + remove_job_info (+ clean other step)
                        echo "6 - qdel + remove_job_info" 
                        delete_job
                    fi
                    # 7: JOB plus dans la file
                    if [[ $status -eq 7 ]]; then
                        # TODO: create TO-DELETE file in the job folder
                        touch "${JOB_FOLDER}/${step%,*}/TO-DELETE"
                        echo "7 - create TO-DELETE file in the job folder"
                        #move_to_delete
                    fi
                    if [[ $status -eq 2 || $status -eq 3 || $status -eq 4 || $status -eq 5 ]]; then
                        rename=
                        case $status in
                            2) rename="FAILED";; 
                            3|4|5) rename="ABORTED";; 
                        esac 
                        rename_step_folder "$rename"
                    fi

                    # Since one step failed, we will now clean up all remaining steps for this JOB_FOLDER.
                    # First, remove the info for the step that just failed.
                    remove_job_info "${step%,*}" 
                    
                    # Get the new list of remaining steps
                    remaining_steps=$(get_job_infos)
                    for remaining_step in ${remaining_steps[@]}; do
                        JOB_ID=${remaining_step##*,} 
                        delete_job # qdel any other pending/running jobs
                        touch "${JOB_FOLDER}/${remaining_step%,*}/TO-DELETE"
                        remove_job_info "${remaining_step%,*}" 
                    done 
                    
                    # Since we've handled the entire JOB_FOLDER due to the error, break the loop.
                    break
                fi
            done
            
            # --- NEW LOGIC BLOCK ---
            # After the loop, check if the .JOB_STEPS file is now empty.
            # This will be true if the last job just finished successfully, or if a job failed and we cleaned everything up.
            current_steps=$(get_job_infos)
            if is_empty_array "$current_steps"; then
                echo "All jobs for ${JOB_FOLDER} are complete. Triggering TZS script."
                
                # This is the single, correct place to run the TZS script.
                run_tzs_script

                # Now, perform the final cleanup.
                remove_job_steps
                remove_cron_job
            fi

        done 
esac