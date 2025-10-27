package macro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Collectors;

//import org.netbeans.core.spi.multiview.CloseOperationHandler;
//import org.netbeans.core.spi.multiview.CloseOperationState;

//import star.base.neo.ServerConnection.CloseOption;
import star.common.*;

public class StarCCM_Main_Macro extends StarMacro {

    private Simulation simulation;
    private String job_folder = "";
    private String previousStep = "";
    private String currentStep = "";

    private static final String job_state_handler = "/home/USER/share/Workflow/latest/src/job_state_handler.sh";
    // private static final String job_state_handler =
    // "/home/USER/share/Workflow/latest/src/job_state_handler.sh";

    public void startPre(File pre_macro) {
        Boolean isError = startScript(pre_macro);
        if (!isError) {
            saveSim();
            simulation.println("in between");
            createSymlink();
        } else {
            createFailed();
        }
    }

    public void startRun(File run_macro) {
        String paramPath = simulation.getSessionDir() + "/" + "parameters.txt";
        HashMap<String, String> parameters = readParameters(paramPath);
        String value = parameters.get("iterator");
        if (value != null) {
            StepStoppingCriterion stoppingCriterion = ((StepStoppingCriterion) simulation
                    .getSolverStoppingCriterionManager()
                    .getSolverStoppingCriterion("Maximum Steps"));
            stoppingCriterion.setMaximumNumberSteps(Integer.parseInt(value));
        }
        Boolean isError = startScript(run_macro);
        if (!isError) {
            saveSim();
            createSymlink();
        } else {
            createFailed();
        }
    }

    public void startPost(File post_macro) {
        String paramPath = simulation.getSessionDir() + "/" + "parameters.txt";
        HashMap<String, String> parameters = readParameters(paramPath);
        String value = parameters.get("iterator");
        if (value != null) {
            StepStoppingCriterion stoppingCriterion = ((StepStoppingCriterion) simulation
                    .getSolverStoppingCriterionManager()
                    .getSolverStoppingCriterion("Maximum Steps"));
            stoppingCriterion.setMaximumNumberSteps(Integer.parseInt(value));
        }
        Boolean isError = startScript(post_macro);
        simulation.println("POST");
        if (!isError) {
            saveSim();
            createSymlink();
        } else {
            createFailed();
        }
    }

    public boolean isErrorSimulation(String filePath) {
        String line = null;
        try (FileReader input = new FileReader(filePath); BufferedReader buff = new BufferedReader(input)) {
            while ((line = buff.readLine()) != null) {
                line = line.strip();
                if (line.matches("^.*(error|Error|ERROR):.*$"))
                    return true;
            }
        } catch (IOException e) {
            System.out.println("Error when parsing file");
        }
        return false;
    }

    public void saveSim() {
        String sim_name = simulation.getSessionDir() + "/" + simulation.getPresentationName() + ".sim";
        simulation.saveState(sim_name);
    }

    public void disableAutoSave() {
        AutoSave autoSave = simulation.getSimulationIterator().getAutoSave();
        autoSave.setAutoSaveBatch(false);
    }

    private void deleteFile(String fileName) {
        try {
            Files.delete(Path.of(simulation.getSessionDir(), fileName));
        } catch (IOException e) {
            simulation.println("[INFO] Fail to delete " + fileName + " file");
        }
    }

    public void createSymlink() {
        // search better solution
        // The submacro should not delete parameters_cleanup.txt anymore.
        File cleanupFile = new File(simulation.getSessionDir() + "/parameters_cleanup.txt");
        if (cleanupFile.exists()) {
            // If the file exists, that means we are in a cleanup job, so no symlink is
            // needed.
            // We can delete the file, because cleanup is done.
            deleteFile("parameters_cleanup.txt");
            return;
        }

        String target_path = job_folder + "/" + currentStep;
        String output = startCommand("bash", "-c", job_state_handler + " -s \"" + target_path + "\"");
        if (output.contains("ERROR")) {
            simulation.println("Error while creating symlink: " + output);
            generateError();
        }
        if (output.contains("INFO")) {
            simulation.println(output);
            return;
        }
        simulation.println("create symlink output: " + output);
    }

    public HashMap<String, String> readParameters(String filePath) {
        HashMap<String, String> parameters = new HashMap<>();
        String delimiters = ",:=";
        String comments = "#*";
        String line = null;
        try {
            FileReader input = new FileReader(filePath);
            BufferedReader buff = new BufferedReader(input);
            while ((line = buff.readLine()) != null) {
                line = line.strip();
                if (line.matches("^\\s*[" + comments + "]+.*|^\\s*$|^\\s*\\[.*\\]"))
                    continue;
                String[] keyValue = line.split("[" + delimiters + "]", 2);
                if (keyValue.length == 2 && !parameters.containsKey(keyValue[0])) {
                    parameters.put(keyValue[0].strip().toLowerCase(), keyValue[1].strip());
                }
            }
            buff.close();
        } catch (Exception e) {
            simulation.println("Error while parsing file");
            simulation.println(e);
            generateError();
        }
        return parameters;
    }

    public void generateError() {
        Integer.parseInt("None");
    }

    public void createFailed() {
        try {
            new File(simulation.getSessionDir() + "/FAILED").createNewFile();
            simulation.println("FAILED file created");
        } catch (IOException e) {
            simulation.println("Cannot create FAILED file");
            simulation.println(e);
            generateError();
        }
    }

    public Boolean startScript(File file) {
        StarScript script = new StarScript(getActiveRootObject(), file);
        simulation.println("start play");
        script.play();
        simulation.println("finish play");
        if (script.getInterruptedByException()) {
            simulation.println("got interrupted by exception");
            createFailed();
            return true;
        }

        // String cmd = "bash -c " + job_state_handler + " -j \"" +
        // simulation.getSessionDir() + "\" " + currentStep;

        String output = startCommand("bash", "-c", job_state_handler + " -j \"" + job_folder + "\" " + currentStep);
        //simulation.println("job_state_handler command: " + cmd);
        //String output = startCommand(cmd);
        simulation.println("output:" + output);
        if (output.contains("ERROR")) {
            simulation.println("Cannot find the job_id for " + simulation.getSessionDir() + " " + currentStep);
            return false;
        }
        int job_id = 0;
        try {
            job_id = Integer.parseInt(output);
        } catch (NumberFormatException e) {
            simulation.println("Cannot parseInt job_id");
            return false;
        }
        String logFilePath = simulation.getSessionDir() + "/StarccmFlex_" + job_id + ".log";
        if (isErrorSimulation(logFilePath)) {
            simulation.println("error: founded in " + logFilePath);
            createFailed();
            return true;
        }
        return false;
    }

    public void startMacros() {
        File current_dir = new File(simulation.getSessionDir());
        File[] files = current_dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.toLowerCase().matches("^(pre_|run_|post_).*"))
                    return true;
                return false;
            }
        });

        if (files.length == 0) {
            simulation.println("Cannot find template main macro");
            return;
        }
        File file = files[0];
        String startName = file.getName()
                .toLowerCase()
                .replaceFirst("_.*", "");
        switch (startName) {
            case "pre":
                startPre(file);
                break;
            case "run":
                startRun(file);
                break;
            case "post":
                startPost(file);
                break;
        }
    }

    public String getPreviousStep(String currentStep) {
        switch (currentStep) {
            case "RUN":
                return "PRE";
            case "POST":
                return "RUN";
            default:
                return null;
        }
    }

    public String startCommand(String... cmd) {
        try {
            ProcessBuilder builder = new ProcessBuilder(cmd);
            Process process = builder.start();
            process.waitFor();
            return new BufferedReader(
                    new InputStreamReader(process.getInputStream())).lines()
                    .collect(Collectors.joining(" "));
        } catch (IOException | InterruptedException e) {
            System.out.println("Error while running command: " + cmd);
            System.out.println(e);
            generateError();
        }
        return "";
    }

    public Boolean isSimRunnable() {
        String sim_file_path = simulation.getSessionDir() + "/" + simulation.getPresentationName();
        Path sim_file = Paths.get(sim_file_path);
        simulation.println("path: " + sim_file.toString());
        Path step_folder = sim_file.getParent();
        currentStep = step_folder.getFileName().toString();
        job_folder = step_folder.getParent().toString();
        if (!Files.isSymbolicLink(sim_file)) // if not link, not bound to previous step, can run
            return true;
        if (currentStep.equals("PRE")) // no previous step, can run
            return true;

        previousStep = getPreviousStep(currentStep);
        String output = startCommand("bash", "-c", job_state_handler + " -m \"" + job_folder + "\" " + previousStep);
        if (output.contains("OK")) // if OK, can run; if STOP, cannot run
            return true;
        return false;
    }

    @Override
    public void execute() {
        try {
            simulation = getActiveSimulation();
            if (!isSimRunnable()) {
                simulation.println("Error - Cannot run step " + currentStep);
                generateError();
            }
            startMacros();
        } catch (Exception e) {
            createFailed();
            generateError();
        }
        simulation.println("end");
    }

}