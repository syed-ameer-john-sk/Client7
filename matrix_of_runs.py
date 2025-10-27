import re
import os


class MatrixOfRuns():
    """
    This class gives methods to run the mupltiple jobs for 
    different values of parameters given
    """

    def __init__(self, param_file_path):
        """
        """
        self.param_file_path = param_file_path

        # Get path of the input data file
        param_folder = os.path.dirname(os.path.abspath(self.param_file_path))
        self.config_file_path = os.path.join(param_folder,
                                             "input_data_file.txt")

        self.parameter_names = []
        self.parameter_values = []
        self.current_run_number = ''

    def get_parameter_data(self):
        """
            Gets the parameter names and parameter values 
            from the parameter file
        """
        self.parameter_names = []
        self.parameter_values = []
        self.current_run_number = ''
        self.description = ''
        with open(self.param_file_path, 'r') as file:
            for line in file:
                # Getting the current run number
                if line.strip().startswith("RUN_NUMBER"):
                    run_num_str = line.split(":", 1)
                    self.current_run_number = run_num_str[1].strip()
                elif line.strip().startswith("DESCRIPTION"):
                    des_str = line.split(":", 1)
                    self.description = des_str[1].strip()
                # Getting parameter names
                elif line.strip().startswith("PARAMETERS"):
                    # Extract content after ':' and strip
                    # surrounding brackets and whitespace
                    _, value = line.split(":", 1)
                    param_str = value.strip().strip("[]")
                    if len(param_str) == 0:
                        continue
                    # Split by commas and strip whitespace
                    self.parameter_names = [
                        p.strip() for p in param_str.split(",")
                    ]
                # Getting values for parameter for each run
                elif line.strip().startswith("PARAMETER_VALUES"):
                    # Extract everything after the colon
                    _, value = line.split(":", 1)
                    # Find all occurrences of [ ... ]
                    matches = re.findall(r'\[(.*?)\]', value)
                    # Split each match by comma and strip spaces
                    self.parameter_values = [[
                        v.strip() for v in match.split(",")
                    ] for match in matches]

    def check_strings_in_input_file(self):
        """
        Checks if the exact matches of parameter names are found in the 
        input data file. Returns False if all parameter names not found, 
        True otherwise.
        """
        praram_flag = True
        results = {}

        if not os.path.exists(self.config_file_path):
            print(f'Input data file not exists! {self.config_file_path}')
            return False

        with open(self.config_file_path, 'r') as file:
            content_lines = file.readlines()  # Read the file line by line
            # Loop through each search_string
            for search_string in self.parameter_names:
                param_found = False
                # Check for an exact match in each line
                for line in content_lines:
                    # Check if the search_string is present as the left
                    # side of an '=' sign # or followed by a newline/space,
                    # to ensure it's a standalone term.
                    if line.strip().startswith(search_string +
                                               "=") or (line.strip()
                                                        == search_string):
                        param_found = True
                        break

                # Save the result for the search_string
                results[search_string] = param_found

        # Check results and print messages
        for string, param_found in results.items():
            if not param_found:
                print(f"'{string}' not found in the input data file.")
                praram_flag = False

        return praram_flag

    def validate_parameters(self):
        """
        """
        # Check for number of parameters
        if len(self.parameter_names) > 5:
            print('Number of parameters should be lessthan 5')
            return False

        # Check each run has all values equal to number of parameters
        rtn_val = all(
            len(sublist) == len(self.parameter_names)
            for sublist in self.parameter_values)
        if rtn_val is False:
            print('Parameters are invalid!')
            return False

        if re.match(r'^[A-Z]{3}-\d{3}$', self.current_run_number) == None:
            print('RUN_NUMBER format is invalid!')
            return False

        rtn_val = self.check_strings_in_input_file()
        if rtn_val is False:
            return False

        return True

    def update_parameter_file(self, param_vals):
        """
        Update the RUN_NUMBER in the parameter file
        """
        with open(self.param_file_path, 'r') as file:
            lines = file.readlines()

        with open(self.param_file_path, 'w') as file:
            for line in lines:
                if line.strip().startswith("DESCRIPTION"):
                    parts = line.split(":", 1)
                    updated_line = f"{parts[0]}:\t\t{self.description} {self.parameter_names}={param_vals}\n"
                    file.write(updated_line)
                elif line.strip().startswith("RUN_NUMBER"):
                    parts = line.split(":", 1)
                    updated_line = f"{parts[0]}:\t\t{self.current_run_number}\n"
                    file.write(updated_line)
                else:
                    file.write(line)

    def update_input_config_file(self, values):
        """
            Update parameter values in the input data file
        """
        if len(self.parameter_names) != len(values):
            raise ValueError(
                "Parameters Names and values lists must have the same length.")

        with open(self.config_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        name_value_map = dict(zip(self.parameter_names, values))

        for i, line in enumerate(lines):
            for name in self.parameter_names:
                pattern = rf'^(\s*{re.escape(name)}\s*=).*'
                if re.match(pattern, line):
                    new_value = str(name_value_map[name])
                    lines[i] = re.sub(r'^(.*=)\s*.*', rf'\1 {new_value}', line)
                    break

        with open(self.config_file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)

    @staticmethod
    def increment_run_number(run_string):
        # Split the string into prefix and numeric part
        split_string = run_string.split('-')
        if len(split_string) == 2:
            prefix, number = run_string.split('-')
            # Increment the numeric part   # Preserve leading zeros
            new_number = str(int(number) + 1).zfill(len(number))
            # Combine the prefix and new number
            new_run_string = f"{prefix}-{new_number}"
        else:
            try:
                new_run_string = str(int(run_string) + 1).zfill(
                    len(run_string))
            except:
                new_run_string = '001'
        return new_run_string

    @staticmethod
    def is_float(v):
        try:
            float(v)
            return True
        except:
            return False


def submit_multiple_jobs(parameter_file):
    """
    """
    parameter_file = os.path.abspath(parameter_file)
    mat_obj = MatrixOfRuns(parameter_file)
    mat_obj.get_parameter_data()
    if len(mat_obj.parameter_values) == 0:
        ## Call the main method here
        # main(parameter_file)
        print('Submitting - NO PARAMETERS GIVEN')
    else:
        rtn_value = mat_obj.validate_parameters()
        if rtn_value is False:
            return None

        # Submit multiple jobs
        for param_vals in mat_obj.parameter_values:
            mat_obj.update_parameter_file(param_vals)
            mat_obj.update_input_config_file(param_vals)
            new_run_num = mat_obj.increment_run_number(
                mat_obj.current_run_number)
            mat_obj.current_run_number = new_run_num

            ## Call the main method here
            # main(parameter_file)


if __name__ == "__main__":

    # Example usage
    submit_multiple_jobs(r'C:\Users\201083499\Documents\parameters.txt')
