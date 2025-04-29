package giu.edu.cspg.loadbalancers;

import java.util.List;
import java.util.Map;

import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import giu.edu.cspg.LoadBalancingBroker;
import giu.edu.cspg.SimulationCore;
import giu.edu.cspg.SimulationSettings;
import giu.edu.cspg.utils.ConfigLoader;
import giu.edu.cspg.utils.SimulationResultUtils;

public class LeastLoadedLoadBalancer {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeastLoadedLoadBalancer.class.getSimpleName());
    private static final String CONFIG_FILE = "config.yml"; // Or load from args/env var
    private static final String EXPERIMENT_ID = "experiment_1"; // Or load from args/env var

    public static void main(String[] args) {
        new LeastLoadedLoadBalancer().run(); // Create instance to manage index state
    }

    public void run() {
        LOGGER.info("Starting LeastLoadedLoadBalancer Experiment...");

        // 1. Load Configuration
        Map<String, Object> params = ConfigLoader.loadConfig(CONFIG_FILE, EXPERIMENT_ID);
        if (params.isEmpty()) {
            LOGGER.error("Failed to load configuration. Exiting.");
            return;
        }
        SimulationSettings settings = new SimulationSettings(params);
        LOGGER.info("Simulation settings dump\n{}", settings.printSettings());

        // 2. Create Simulation Core
        SimulationCore simulationCore = new SimulationCore(settings);
        LoadBalancingBroker broker = simulationCore.getBroker();

        // 3. Main Simulation Loop
        LOGGER.info("Starting simulation loop...");
        int step = 0;
        while (simulationCore.isRunning()) {
            step++;
            LOGGER.debug("--- Step {} (Clock: {}) ---", step, String.format("%.2f", simulationCore.getClock()));

            // Assign waiting cloudlets using Least Loaded logic
            while (broker.hasWaitingCloudlets()) {
                List<Vm> runningVms = broker.getVmExecList();
                if (!runningVms.isEmpty()) {
                    Vm bestVm = null;
                    double minLoad = Double.MAX_VALUE;

                    for (Vm vm : runningVms) {
                        if (vm != null && vm.isCreated() && !vm.isFailed()) {
                            // Option A: CPU Utilization
                            double currentLoad = vm.getCpuPercentUtilization();

                            // Option B: Active Cloudlets Count
                            // double currentLoad = vm.getCloudletScheduler().getCloudletExecList().size();

                            if (currentLoad < minLoad) {
                                minLoad = currentLoad;
                                bestVm = vm;
                            }
                        }
                    }

                    if (bestVm != null) {
                        LOGGER.debug("LeastLoaded: Assigning next cloudlet to VM {} (Load: {})", bestVm.getId(),
                                minLoad);
                        boolean assigned = broker.assignCloudletToVm((int) bestVm.getId());
                        if (!assigned) {
                            LOGGER.warn(
                                    "LeastLoaded assignment failed for VM {}. May be unsuitable. Stopping assignment for this step.",
                                    bestVm.getId());
                            break; // Avoid infinite loop if the best VM is unsuitable
                        }
                    } else {
                        LOGGER.warn("LeastLoaded LB: Could not find a suitable running VM. Queue size: {}",
                                broker.getWaitingCloudletCount());
                        break; // Exit loop if no suitable VM found
                    }
                } else {
                    LOGGER.warn("LeastLoaded LB: No running VMs available to assign cloudlet. Queue size: {}",
                            broker.getWaitingCloudletCount());
                    break; // Exit inner loop if no VMs
                }
            }

            // Advance simulation time by one step
            simulationCore.runOneTimestep();
        }
        LOGGER.info("Simulation loop finished at clock: {}", String.format("%.2f", simulationCore.getClock()));

        // 4. Print and Save Results
        SimulationResultUtils.printAndSaveResults(simulationCore, settings.getSimulationName() + "_LeastLoaded");

        LOGGER.info("LeastLoadedLoadBalancer Experiment Finished.");
    }
}
