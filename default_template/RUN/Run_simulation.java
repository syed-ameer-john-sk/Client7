package macro;

import java.util.*;
import star.common.*;
import star.base.neo.*;

public class Run_simulation extends StarMacro {
    
    private Simulation sim = getActiveSimulation();

    @Override 
    public void execute() { 
        sim.getSimulationIterator().run();
    }

}
