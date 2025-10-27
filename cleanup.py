"""
Cleanup script for Alstom AeroX Software projects.

Written by Sofian El Guetibi (497782)
v1.0
17/01/2023
"""
# from __future__ import annotations

from log import Log
from os import DirEntry
from pathlib import Path
from typing import List
from typing import Union
import argparse
import os
import re
import shutil
import subprocess
import sys
# import doctest

# global variable
LOGGER: Union[Log, None] = None
PROJECTS_PATH: str = "/home/USER/share/_PROJECTS"


#########
# Utils #
#########

def run_command(cmd: str, exit_after_error: bool = True) -> str:
    stdout, stderr = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        universal_newlines=True
    ).communicate()

    if stderr != "":
        if exit_after_error:
            exit_script()
    return stdout


def set_log(log_name: str, log_path: str) -> None:
    global LOGGER
    LOGGER = Log(f"Cleanup-{log_name}", os.path.dirname(log_path))
    sys.stderr = LOGGER.set_log("stderr")


def simple_exit(*msgs: str) -> None:
    for msg in msgs:
        print(msg)
    exit_script()


def exit_script() -> None:
    print("error:", "Folders not cleaned")
    exit(1)


class OSHelper:
    """A class that provides static method for OS operations."""

    @staticmethod
    def path_exists(path: str) -> bool:
        if os.path.exists(path):
            return True
        LOGGER.log_event("warning", f"Path does not exists: {path}")
        return False

    @staticmethod
    def is_owned_by_user(path: str) -> bool:
        file = Path(path)
        if file.stat().st_uid == os.getuid():
            return True
        LOGGER.log_event("warning", f"You are not the owner of this resource: {path}")
        return False

    @staticmethod
    def has_permissions(path: str, mode: int) -> bool:
        if os.access(path, mode, follow_symlinks=False):
            return True
        LOGGER.log_event("warning", f"Permission denied: {path}")
        return False

    @staticmethod
    def is_directory(path: str) -> bool:
        if os.path.isdir(path):
            return True
        LOGGER.log_event("warning", f"Not a directory: {path}")
        return False

    @staticmethod
    def is_file(path: str) -> bool:
        if os.path.isfile(path):
            return True
        LOGGER.log_event("warning", f"Not a file: {path}")
        return False

    @staticmethod
    def is_symlink(path: str) -> bool:
        return os.path.islink(path)

    @staticmethod
    def is_editable_file(path: str) -> bool:
        if not OSHelper.path_exists(path):
            return False
        if not OSHelper.is_owned_by_user(path):
            return False
        if not OSHelper.has_permissions(path, os.W_OK):
            return False
        if not OSHelper.is_file(path):
            return False
        return True

    @staticmethod
    def is_editable_directory(path: str) -> bool:
        if not OSHelper.path_exists(path):
            return False
        if not OSHelper.has_permissions(path, os.X_OK | os.W_OK | os.R_OK):
            return False
        if not OSHelper.is_directory(path):
            return False
        return True

    @staticmethod
    def remove_folders(folders: List[DirEntry]) -> None:
        for folder in folders:
            if not OSHelper.is_owned_by_user(folder.path):
                continue
            if not OSHelper.is_editable_directory(folder.path):
                continue
            LOGGER.log_event("info", f"Folder to delete: {folder}")
            shutil.rmtree(folder)


class CleanupHelper:
    """A class that provides static method for cleanup."""

    @staticmethod
    def build_job_code_list_from_range(job_begin: str, job_end: str) -> List[str]:
        """
        Return a list from range of jobs id between begin and end.
        The job end is included
        >>> CleanupHelper.build_job_code_list_from_range('ALO-001', 'ALO-009')
        ... # doctest: +NORMALIZE_WHITESPACE
        ['ALO-001', 'ALO-002', 'ALO-003', 'ALO-004', 'ALO-005',
        'ALO-006', 'ALO-007', 'ALO-008', 'ALO-009']
        """

        def decode(job_code: str):
            # maybe changed with r'(.+)\-([0-9]+)'
            regex = r'([a-zA-Z0-9_\-]+)\-([0-9]+)'
            result = re.search(regex, job_code)
            if result is None:
                simple_exit("Can't decode <Job code>: " + job_code)
            # group(1) is the project code and group(2) is the number of run
            return result.group(1), result.group(2)

        project_begin, code_begin = decode(job_begin)
        project_end, code_end = decode(job_end)
        if project_begin != project_end:
            simple_exit("Range cleanup but the given job codes does not come from the same project")
        if len(code_begin) != len(code_end):  # if code length are not the same, we can't retrieve the number of trailing 0
            simple_exit("The code length are not the same, we can't retrieve job codes")
        if int(code_begin) > int(code_end):  # Safe cast because the regex drop the non numbers characters
            simple_exit("The begin code should be lesser than the end code")
        return [
            f"{project_begin}-{'{0}'.format(str(i).zfill(len(code_end)))}"
            for i in range(int(code_begin), int(code_end) + 1)  # +1 to include the last job code
        ]

    @staticmethod
    def build_all_available_job_code_from_project_name(project_path: str, project_name: str) -> List[str]:
        path = os.path.join(project_path, project_name)
        if not OSHelper.is_editable_directory(path):
            simple_exit("Should not happen, verify permissions of the project folder")
        with os.scandir(path) as folders:
            regex = r'\w*-'
            return [
                re.sub(regex, '', folder.name, 1)  # remove project name from folder name
                for folder in folders
                if OSHelper.is_owned_by_user(folder.path)
                   and OSHelper.is_editable_directory(folder.path)
                   and folder.name.startswith(project_name)
            ]

    @staticmethod
    def fill_template(file_name: str, project_name: str, run_number: str, sim_file: str, step: str) -> None:
        with open(file_name, 'r') as old:
            file_data = old.read()
            file_data = file_data.replace("{project_to_clean}", project_name) \
                .replace("{run_number_to_clean}", run_number) \
                .replace("{sim_to_clean}", sim_file) \
                .replace("{workflow_step}", step)
            with open(file_name, 'w') as new:
                new.write(file_data)

    @staticmethod
    def retrieve_sim_files(path: str) -> List[str]:
        """
        Build a list of sim file paths.
        param: path to perform the sim files search
        return: list of sim file paths
        """
        with os.scandir(path) as folder:
            return [entity.path for entity in folder if entity.name.endswith(".sim") or entity.name.endswith(".sim~")]

    @staticmethod
    def purge_sim_files(paths: List[str]) -> Union[str, None]:
        """
        Choose the sim_file to be enmeshed and delete all other sim files.
        return: the sim_file chosen to be enmeshed
        """
        paths = list(filter(lambda file: not OSHelper.is_symlink(file), paths))
        if len(paths) == 0:
            return
        sorted_by_size = sorted(paths, key=lambda x: os.stat(x).st_size)
        to_be_enmeshed = sorted_by_size.pop()
        for path in sorted_by_size:
            if OSHelper.is_editable_file(path):
                os.remove(path)
                LOGGER.log_event("info", f"Deleted: {path}")
        # quick fix: to_be_enmeshed is probably a symlink
        if os.stat(to_be_enmeshed).st_size < 10:
            Path(to_be_enmeshed).unlink()
            return
        return to_be_enmeshed

    @staticmethod
    def retrieve_job_id(stdout: str) -> Union[str, None]:
        """
        This function assumes that the workflow output contains the job id in the following form:
        'Job submitted with ID: 43079'
        >>> CleanupHelper.retrieve_job_id('Job submitted with ID: 43079')
        '43079'
        """
        match = re.search(r'(Job submitted with ID: )([0-9]+)', stdout)
        if match is None:
            simple_exit("Some problems occurs when job submitted through the workflow, can't retrieve previous job id")
        return match.group(2)

    @staticmethod
    def contains_sim_file(path: str) -> bool:
        """Test if the given folder contains a sim file"""
        with os.scandir(path) as folder:
            for file in folder:
                if not OSHelper.is_symlink(file.path) and file.name.endswith(".sim"):
                    return True
        return False


class Cleaner:
    """A class that manage cleanup folders."""

    def __init__(self, project: str, run_numbers: List[str]) -> None:
        self._project: str = project
        self._run_numbers: List[str] = run_numbers

    @staticmethod
    def _prepare_folder_clean_workflow(path: str, step: str) -> None:
        """
        Prepare the given folder to run a job through the workflow.
        Delete the previous macros and state files: ABORT, TERMINATED, FAILED, FINISHED
        """
        to_be_removed = ["StarCCM_Main_Macro.java", "ABORT", "ABORT~", "TERMINATED", "FAILED", "FINISHED"]
        macro = {"pre": "Pre_processing.java", "run": "Run_simulation.java", "post": "Post_processing.java"}
        to_be_removed.append(macro[step.lower()])
        with os.scandir(path) as folder:
            for entity in folder:
                if not OSHelper.is_editable_file(entity.path):
                    continue
                if entity.name in to_be_removed:
                    os.remove(entity.path)
                    LOGGER.log_event("info", f"deleted: {entity.path}")

    @staticmethod
    def _clean_folder_using_remove(path: Union[str, None]) -> None:
        """Delete all .sim, .sim~ file found in the folder"""
        if path is None:
            return
        if not OSHelper.path_exists(path):
            return
        if not OSHelper.is_owned_by_user(path):
            return
        if not OSHelper.is_editable_directory(path):
            return
        LOGGER.log_event("info", f"Cleaning {path}...")
        with os.scandir(path) as files:
            for file in files:
                if file.name.endswith(".sim") or file.name.endswith(".sim~"):
                    if OSHelper.is_editable_file(file.path):
                        os.remove(file.path)
                        LOGGER.log_event("info", f"Deleted: {file.path}")
                    elif Path(file.path).is_symlink():
                        Path(file.path).unlink()
                        LOGGER.log_event("info", f"Deleted: {file.path}")

    def _clean_folder_using_workflow(
            self,
            run_number: str,
            step: str,
            previous_job_id: Union[str, None] = None
    ) -> Union[str, None]:
        """
        Clean the given folder using the workflow.
        Run the enmesh command on the biggest .sim file and delete all the other sim files.

        1. Retrieve the good sim_file
        2. Copy the parameters_cleanup template file and fill them
        3. Prepare the folder to run through workflow (delete .JOB_STEPS, state files (ABORTED, FINISHED..etc), previous macros)
        4. Run the workflow command
        5. Parse workflow output to retrieve previous_job_id for cleanup the next folder
        """
        workflow_dir = f"{os.path.dirname(os.path.abspath(__file__))}"
        parameter_file_template = f"{workflow_dir}/parameters_cleanup.txt"
        workflow_path = f"{workflow_dir}/workflow.py"

        step_folder = os.path.join(PROJECTS_PATH, self._project, f"{self._project}-{run_number}", step)
        if not OSHelper.is_owned_by_user(step_folder):
            return
        if not OSHelper.is_editable_directory(step_folder):
            return

        # 1. Retrieve the good sim_file
        sim_files = CleanupHelper.retrieve_sim_files(step_folder)
        if not sim_files:
            return
        sim_file = CleanupHelper.purge_sim_files(sim_files)
        if sim_file is None:
            LOGGER.log_event("info", f"No sim_file found, job not submitted")
            return  # sim file not found, we don't submit the job through the workflow

        LOGGER.log_event("info", f"Cleaning: {step_folder}...")
        sim_file = os.path.basename(sim_file)
        LOGGER.log_event("info,terminal", f"To enmeshed sim_file: {sim_file}")

        # 2. Copy the parameters_cleanup template file and fill them
        parameters_file_path = os.path.join(step_folder, "parameters_cleanup.txt")
        shutil.copyfile(parameter_file_template, parameters_file_path)
        CleanupHelper.fill_template(parameters_file_path, self._project, run_number, sim_file, step)
        LOGGER.log_event("info", f"Filled parameters_cleanup.txt with {self._project}, {run_number}, {sim_file}, {step}")

        # 3. Prepare the folder to run through workflow
        job_steps_path = os.path.join(PROJECTS_PATH, self._project, f"{self._project}-{run_number}", ".JOB_STEPS")
        if not OSHelper.is_editable_file(job_steps_path):
            LOGGER.log_event("info,terminal", "File .JOB_STEPS is not editable")
        else:
            os.remove(job_steps_path)
            LOGGER.log_event("info", f"Deleted: {job_steps_path}")
        Cleaner._prepare_folder_clean_workflow(step_folder, step)

        # 4. Run the workflow command
        cmd = f"python3 {workflow_path} {parameters_file_path} -c " + \
              (f"-d {previous_job_id}" if previous_job_id else "")
        stdout = run_command(cmd, False)
        LOGGER.log_event("info,terminal", f"Running command: {cmd}")
        LOGGER.log_event("info,terminal", f"Workflow stdout: {stdout}")

        # 5. Parse workflow output to retrieve previous_job_id for cleanup the next folder
        return CleanupHelper.retrieve_job_id(stdout)

    @staticmethod
    def _purge_step(path: str, step: str) -> Union[str, None]:
        """
        Delete -FAILED folder.

        Keep the last (sort by date) step folder and delete all the other one according to the step.

        Ex: path/RUN
            path/RUN-FAILED
            path/RUN-1000

        The folder RUN-FAILED will be deleted.

        The last written folder between RUN and RUN-1000 is kept, the other one is deleted.

        If the kept folder is not named exactly as a step (PRE/RUN/POST) it will be renamed.

        Ex: If RUN-1000 is kept, it will be renamed RUN.
        """
        with os.scandir(path) as folders:
            for folder in folders:
                if folder.name.startswith(step) and folder.name.endswith("-FAILED"):
                    if not OSHelper.is_owned_by_user(folder.path):
                        continue
                    if not OSHelper.is_editable_directory(folder.path):
                        continue
                    LOGGER.log_event("info", f"Folder to delete: {folder}")
                    shutil.rmtree(folder)
        with os.scandir(path) as folders:
            # we filter again on "-FAILED" because it may not have been deleted if the user is not the owner of the directory
            step_folders = [folder for folder in folders if folder.name.startswith(step) and not folder.name.endswith("-FAILED")]
        if len(step_folders) == 0:
            return
        sorted_by_timestamp = sorted(step_folders, key=lambda x: os.stat(x).st_mtime)
        folder_to_workflow = sorted_by_timestamp.pop()
        OSHelper.remove_folders(sorted_by_timestamp)
        step_path = os.path.join(os.path.dirname(folder_to_workflow.path), step)
        if folder_to_workflow.path != step_path:
            LOGGER.log_event("info", f"Folder {folder_to_workflow.path} move to {step_path}")
            os.rename(folder_to_workflow, step_path)
        return step_path

    def _clean_folder(self, run_number: str, previous_job_id: Union[str, None] = None) -> Union[str, None]:
        """
        Clean a root job folder.
        1. Purge the workspace to just keep (PRE/RUN/POST) folders
        2. Check in this specific order the PRE, RUN, POST folder
        3. The first one found, we create a new job to clean meshed file.
        4. The other step folders are cleaned using rm on .sim and .sim~ file.
        """
        path = os.path.join(PROJECTS_PATH, self._project, f"{self._project}-{run_number}")
        pre_path = Cleaner._purge_step(path, "PRE")
        run_path = Cleaner._purge_step(path, "RUN")
        post_path = Cleaner._purge_step(path, "POST")
        if pre_path is not None \
                and OSHelper.path_exists(pre_path) \
                and OSHelper.is_directory(pre_path) \
                and CleanupHelper.contains_sim_file(pre_path):
            previous_job_id = self._clean_folder_using_workflow(run_number, "PRE", previous_job_id)
            Cleaner._clean_folder_using_remove(run_path)
            Cleaner._clean_folder_using_remove(post_path)
            LOGGER.log_event("info,terminal", f"Folder {self._project}-{run_number} cleaned")
            return previous_job_id
        elif run_path is not None \
                and OSHelper.path_exists(run_path) \
                and OSHelper.is_directory(run_path) \
                and CleanupHelper.contains_sim_file(run_path):
            Cleaner._clean_folder_using_remove(pre_path)
            previous_job_id = self._clean_folder_using_workflow(run_number, "RUN", previous_job_id)
            Cleaner._clean_folder_using_remove(post_path)
            LOGGER.log_event("info,terminal", f"Folder {self._project}-{run_number} cleaned")
            return previous_job_id
        elif post_path is not None \
                and OSHelper.path_exists(post_path) \
                and OSHelper.is_directory(post_path) \
                and CleanupHelper.contains_sim_file(post_path):
            Cleaner._clean_folder_using_remove(pre_path)
            Cleaner._clean_folder_using_remove(run_path)
            previous_job_id = self._clean_folder_using_workflow(run_number, "POST", previous_job_id)
            LOGGER.log_event("info,terminal", f"Folder {self._project}-{run_number} cleaned")
            return previous_job_id
        else:
            Cleaner._clean_folder_using_remove(pre_path)
            Cleaner._clean_folder_using_remove(run_path)
            Cleaner._clean_folder_using_remove(post_path)
            LOGGER.log_event("info,terminal", f"Folder {self._project}-{run_number} cleaned")
            return

    def clean_folders(self) -> None:
        """Call to perform the cleanup"""
        previous_job_id = None
        for run_number in self._run_numbers:
            run_folder = os.path.join(PROJECTS_PATH, self._project, f"{self._project}-{run_number}")
            set_log(run_number, run_folder)
            if not OSHelper.path_exists(run_folder):
                LOGGER.log_event("info,terminal", f"Folder {self._project}-{run_number} does not exists")
                continue
            if not OSHelper.is_owned_by_user(run_folder):
                LOGGER.log_event("info,terminal", f"Folder {self._project}-{run_number} not cleaned: not the right owner")
                continue
            if not OSHelper.is_editable_directory(run_folder):
                LOGGER.log_event("info,terminal", f"Folder {self._project}-{run_number} not cleaned: not editable folder")
                continue
            tmp_previous_id = self._clean_folder(run_number, previous_job_id)
            if tmp_previous_id is not None:
                previous_job_id = tmp_previous_id
                LOGGER.log_event("info,terminal", f"Folder {self._project}-{run_number} cleanup launched")
                LOGGER.move_logs(run_folder)

    @staticmethod
    def build_cleaner_from_argparse(parser: argparse.ArgumentParser):  # -> Cleaner:
        args = parser.parse_args()
        project = args.project[0]
        run_numbers = []
        if args.simple:
            run_numbers = [args.simple[0]]
        elif args.range:
            run_numbers = CleanupHelper.build_job_code_list_from_range(args.range[0], args.range[1])
        elif args.list:
            run_numbers = args.list
        elif args.all:
            set_log("Cleanup-ALL", os.path.join(PROJECTS_PATH, project))
            run_numbers = CleanupHelper.build_all_available_job_code_from_project_name(PROJECTS_PATH, project)
            LOGGER.log_event("info,terminal", f"Jobs folder found: {run_numbers}")
            LOGGER.move_logs(os.path.join(PROJECTS_PATH, project))
        else:
            simple_exit("ERROR: invalid option (should not happen)")
        return Cleaner(project=project, run_numbers=run_numbers)


########
# MAIN #
########


def setup_parser(parser: argparse.ArgumentParser) -> None:
    """Set up the argparse parser"""
    parser._optionals.title = 'arguments'
    parser.add_argument('-p', '--project', type=str, metavar='<Project code>',
                        nargs=1, help="Project folder to clean", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--simple', type=str, nargs=1,
                       metavar="<Job code>", help="Clean a job folder")
    group.add_argument('-r', '--range', type=str, nargs=2,
                       metavar="<Job code>", help="Clean a range of jobs folder")
    group.add_argument('-l', '--list', type=str, nargs='+',
                       metavar="<Job code>", help="Clean a list of jobs folder")
    group.add_argument('-a', '--all', action='store_true',
                       help="Clean all jobs folder")


def main() -> None:
    parser = argparse.ArgumentParser()
    setup_parser(parser)
    cleaner = Cleaner.build_cleaner_from_argparse(parser)
    cleaner.clean_folders()


if __name__ == "__main__":
    # print(doctest.testmod())
    main()
