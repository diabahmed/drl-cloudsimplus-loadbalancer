package giu.edu.cspg;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import static java.util.Comparator.comparingDouble;
import static java.util.Comparator.comparingLong;
import java.util.List;
import java.util.Map;

import org.cloudsimplus.builders.tables.AbstractTable;
import org.cloudsimplus.builders.tables.CsvTable;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostStateHistoryEntry;
import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import giu.edu.cspg.tables.CloudletsTableBuilderWithDetails;
import giu.edu.cspg.tables.HostHistoryTableBuilderCsv;
import static giu.edu.cspg.tables.TableLogger.logAndSaveTable;
import giu.edu.cspg.tables.VmsTableBuilderWithDetails;
import py4j.GatewayServer;

public class LoadBalancerGateway {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadBalancerGateway.class.getSimpleName());
    private static final DecimalFormat df = new DecimalFormat("#.###");

    private SimulationCore simulationCore;
    private SimulationSettings settings;
    private String simName;
    private GatewayServer gatewayServer;
    private long currentSeed = 0; // Store seed for resets
    private int currentStep = 0;
    private int maxPotentialVms = 0; // Calculated based on max capacity and Size of the observation arrays for VMs

    // To store reward components calculated in step for SimulationStepInfo
    private double rewardWaitTimeComponent = 0;
    private double rewardUnutilizationComponent = 0;
    private double rewardCostComponent = 0;
    private double rewardQueuePenaltyComponent = 0;
    private double rewardInvalidActionComponent = 0;

    public LoadBalancerGateway() {
        // Settings will be properly initialized by configureSimulation
        LOGGER.info("LoadBalancerGateway created. Waiting for configuration...");
    }

    /**
     * Configures the simulation settings. Called once by Python after
     * initialization.
     * 
     * @param params Map containing simulation parameters.
     */
    public void configureSimulation(Map<String, Object> params) {
        this.settings = new SimulationSettings(params);
        this.simName = settings.getSimulationName();
        LOGGER.info("Simulation Name: {}", this.simName);
        LOGGER.info("Simulation settings dump\n{}", settings.printSettings());
        LOGGER.info("Simulation configured. Waiting for reset request...");
    }

    /**
     * Calculates the theoretical maximum number of smallest VMs the infrastructure
     * could hold.
     */
    private int calculateMaxPotentialVms(SimulationSettings settings) {
        if (settings.getHostsCount() <= 0 || settings.getHostPes() <= 0 || settings.getSmallVmPes() <= 0) {
            return 0; // Avoid division by zero
        }
        int totalHostCores = (int) simulationCore.getTotalHostCores();
        int smallestVmCores = settings.getSmallVmPes();
        // Simple calculation: total host cores / smallest VM cores
        // Could be refined based on RAM/BW if those are more limiting
        // Theoretical max + a buffer (e.g., 20%) in case of fragmentation or many small
        // VMs created
        return (int) Math.ceil((double) (totalHostCores / smallestVmCores) * 1.2);
        // Alternative: Use a large fixed number from config if preferred
        // return settings.getIntParam("max_observation_vms", 100); // Example
    }

    /**
     * Resets the simulation environment.
     * 
     * @param seed Random seed for reproducibility.
     * @return A SimulationResetResult containing the initial state and info.
     */
    public SimulationResetResult reset(long seed) {
        if (settings == null) {
            throw new IllegalStateException("Simulation not configured. Call configureSimulation first.");
        }
        LOGGER.info("Received reset request from Python with seed {}.", seed);
        this.currentSeed = seed; // Store seed if needed later
        this.currentStep = 0;
        // resetEpisodeStats(); // Reset any episode-specific stats if added later

        // Create/Reset core components
        if (this.simulationCore != null) {
            this.simulationCore.stopSimulation(); // Ensure previous run is fully stopped
        }
        this.simulationCore = new SimulationCore(this.settings); // Calls internal reset

        // Calculate max potential VMs for padding observation arrays
        this.maxPotentialVms = calculateMaxPotentialVms(settings);
        if (this.maxPotentialVms <= 0) {
            LOGGER.warn(
                    "Calculated maxPotentialVms is <= 0. Observation padding might be incorrect. Check host/vm PE settings.");
            // Set a minimum size to avoid zero-length arrays
            this.maxPotentialVms = Math.max(10,
                    settings.getInitialSVmCount() + settings.getInitialMVmCount() + settings.getInitialLVmCount());
        }

        LOGGER.info("Max potential VMs calculated: {}", this.maxPotentialVms);

        ObservationState initialState = getCurrentState();
        // Info at reset: clock is 0, no actions taken yet
        SimulationStepInfo initialInfo = new SimulationStepInfo(0.0);

        LOGGER.info("Reset complete. Initial State: {}", initialState);
        return new SimulationResetResult(initialState, initialInfo);
    }

    /**
     * Executes one simulation step based on the agent's action list.
     * Action format: [action_type, target_vm_id, target_host_id, vm_type_index]
     * 
     * @param actionList Java List of Integers representing the action.
     * @return A StepResult object containing the new state, reward, done flags, and
     *         info.
     */
    public SimulationStepResult step(List<Integer> actionList) {
        if (simulationCore == null || settings == null) {
            throw new IllegalStateException("Simulation not initialized/configured. Call reset() first.");
        }
        if (actionList == null || actionList.size() < 4) {
            LOGGER.error("Invalid action list received: {}", actionList);
            throw new IllegalArgumentException("Action list must have 4 elements.");
        }

        currentStep++;
        LOGGER.debug("Step {} starting. Action: {}", currentStep, actionList);

        // --- 1. Execute Action ---
        int actionType = actionList.get(0);
        int targetVmId = actionList.get(1);
        int targetHostId = actionList.get(2);
        int vmTypeIndex = actionList.get(3);

        boolean assignSuccess = false;
        boolean createAttempted = false;
        boolean createSuccess = false;
        boolean destroyAttempted = false;
        boolean destroySuccess = false;
        boolean wasInvalidAction = false; // Flag if the action logic itself deems it invalid
        int hostAffectedId = -1; // Default: no host affected
        int coresChanged = 0; // Default: no cores changed

        switch (actionType) {
            case 0 -> {
                // No-op
                LOGGER.trace("Action Type 0: No operation.");
                break;
            }
            case 1 -> {
                // Assign Cloudlet
                if (simulationCore.getBroker().hasWaitingCloudlets()) {
                    assignSuccess = simulationCore.getBroker().assignCloudletToVm(targetVmId);
                    if (!assignSuccess) {
                        LOGGER.warn(
                                "Assign Cloudlet to VM {} failed (VM likely invalid or full). Invalid action taken.",
                                targetVmId);
                        wasInvalidAction = true; // Treat failed assignment as invalid action
                    }
                } else {
                    LOGGER.warn("Assign Cloudlet action taken, but queue is empty. Invalid action.");
                    wasInvalidAction = true; // Invalid action if queue is empty
                }
            }
            case 2 -> {
                // Create VM
                createAttempted = true;
                if (vmTypeIndex >= 0 && vmTypeIndex < SimulationSettings.VM_TYPES.length) {
                    String vmType = SimulationSettings.VM_TYPES[vmTypeIndex];
                    // The method returns a boolean if submission failed internally (rare)
                    if (simulationCore.createVmOnHost(vmType, targetHostId)) {
                        createSuccess = true; // Request submitted
                        hostAffectedId = targetHostId; // Host where creation was requested
                        coresChanged = settings.getSmallVmPes() * settings.getSizeMultiplier(vmType); // Cores added
                    } else {
                        // Should ideally not happen if host ID is valid, allocation policy might fail
                        // later
                        LOGGER.error("Core VM creation/submission failed unexpectedly for type {} on host {}.", vmType,
                                targetHostId);
                        wasInvalidAction = true;
                    }
                } else {
                    LOGGER.warn("Invalid VM type index {} in Create VM action.", vmTypeIndex);
                    wasInvalidAction = true;
                }
            }
            case 3 -> {
                // Destroy VM
                destroyAttempted = true;
                // Need to find the VM first to get its details before destroying
                Vm vmToDestroy = simulationCore.getBroker().getVmExecList().get(targetVmId);
                if (vmToDestroy != Vm.NULL && vmToDestroy.isCreated() && !vmToDestroy.isFailed()) {
                    hostAffectedId = (int) vmToDestroy.getHost().getId(); // Get host before destroy request
                    coresChanged = -(int) vmToDestroy.getPesNumber(); // Cores removed (negative)
                    destroySuccess = simulationCore.destroyVmById(targetVmId); // Initiate destruction
                    if (!destroySuccess) { // Should ideally not happen if checks pass
                        LOGGER.warn("Destroy VM {} initiation failed unexpectedly.", targetVmId);
                        wasInvalidAction = true;
                        hostAffectedId = -1; // Reset if initiation failed
                        coresChanged = 0;
                    }
                } else {
                    LOGGER.warn(
                            "Destroy VM {} failed (VM likely doesn't exist or already stopped). Invalid action taken.",
                            targetVmId);
                    wasInvalidAction = true;
                }
            }
            default -> {
                LOGGER.warn("Invalid action type received: {}", actionType);
                wasInvalidAction = true;
            }
        }

        // --- 2. Advance Simulation Time ---
        simulationCore.runOneTimestep(); // Run one timestep (default: 1 second)
        double currentClock = simulationCore.getClock();

        // --- 3. Calculate Reward ---
        double totalReward = calculateReward(wasInvalidAction); // Pass invalid flag

        // --- 4. Get New State ---
        ObservationState newState = getCurrentState();

        // --- 5. Check Termination/Truncation ---
        boolean terminated = !simulationCore.isRunning();
        boolean truncated = !terminated && (currentStep >= settings.getMaxEpisodeLength());
        if (truncated)
            LOGGER.info("Episode truncated at step {}", currentStep);
        if (terminated)
            LOGGER.info("Episode terminated naturally at step {} (clock {})", currentStep, currentClock);

        // --- 6. Create Info Object ---
        SimulationStepInfo stepInfo = new SimulationStepInfo(
                assignSuccess, createAttempted, createSuccess,
                destroyAttempted, destroySuccess, wasInvalidAction,
                hostAffectedId, coresChanged,
                currentClock,
                this.rewardWaitTimeComponent,
                this.rewardUnutilizationComponent,
                this.rewardCostComponent,
                this.rewardQueuePenaltyComponent, this.rewardInvalidActionComponent);

        LOGGER.debug("Step {} finished. Reward: {}, Term: {}, Trunc: {}, Info: {}", currentStep, totalReward,
                terminated, truncated, stepInfo);

        // --- 7. Return Result ---
        return new SimulationStepResult(newState, totalReward, terminated, truncated, stepInfo);
    }

    /**
     * Calculates the reward for the current state and action outcome.
     * Also stores individual components for the SimulationStepInfo object.
     * 
     * @param wasInvalidAction true if the action taken in this step was logically
     *                         invalid.
     * @return The total calculated reward.
     */
    private double calculateReward(boolean wasInvalidAction) {
        if (simulationCore == null || settings == null)
            return 0.0;

        // --- Calculate Individual Components ---

        // 1. Wait Time Reward (Negative penalty for wait time of *finished* cloudlets
        // this step)
        List<Double> finishedWaitTimes = simulationCore.getBroker()
                .getFinishedWaitTimesLastStep(simulationCore.getClock());
        double avgWaitTime = finishedWaitTimes.isEmpty() ? 0
                : finishedWaitTimes.parallelStream().mapToDouble(d -> d).average().orElse(0.0);
        this.rewardWaitTimeComponent = -settings.getRewardWaitTimeCoef() * avgWaitTime;

        // 2. Unutilization Reward (Negative penalty for average CPU unutil of *running*
        // VMs)
        List<Vm> runningVms = simulationCore.getVmPool();
        double avgUnutilization = 0;
        if (!runningVms.isEmpty()) {
            avgUnutilization = runningVms.parallelStream()
                    .filter(vm -> vm != null && vm.isCreated() && !vm.isFailed())
                    .mapToDouble(vm -> 1 - vm.getCpuPercentUtilization())
                    .average()
                    .orElse(0.0);
        }
        this.rewardUnutilizationComponent = -settings.getRewardUnutilizationCoef() * avgUnutilization;

        // 3. Cost Reward (Negative penalty for running more host cores)
        this.rewardCostComponent = -settings.getRewardCostCoef()
                * getHostCoresAllocatedToVmsRatio();

        // 4. Queue Penalty (Negative penalty for number of waiting cloudlets)
        this.rewardQueuePenaltyComponent = -settings.getRewardQueuePenaltyCoef() * getWaitingCloudletsRatio();

        // 5. Invalid Action Reward (Negative penalty for invalid action taken)
        this.rewardInvalidActionComponent = -settings.getRewardInvalidActionCoef() * (wasInvalidAction ? 1.0 : 0.0);

        // --- Total Reward ---
        double totalReward = this.rewardWaitTimeComponent +
                this.rewardUnutilizationComponent +
                this.rewardCostComponent +
                this.rewardQueuePenaltyComponent +
                this.rewardInvalidActionComponent;

        LOGGER.debug("Reward Calc: Wait={}, Util={}, Cost={}, Queue={}, Invalid={}, Total={}",
                this.rewardWaitTimeComponent,
                this.rewardUnutilizationComponent,
                this.rewardCostComponent,
                this.rewardQueuePenaltyComponent, this.rewardInvalidActionComponent, totalReward);

        return totalReward;
    }

    /**
     * Calculates the ratio of allocated cores to total host cores.
     * Used for cost reward calculation.
     * 
     * @return The ratio of allocated cores to total host cores.
     */
    private double getHostCoresAllocatedToVmsRatio() {
        return ((double) simulationCore.getAllocatedCores()) / simulationCore.getTotalHostCores();
    }

    /**
     * Calculates the ratio of waiting cloudlets to total arrived cloudlets.
     * Used for queue penalty calculation.
     * 
     * @return The ratio of waiting cloudlets to total arrived cloudlets.
     */
    private double getWaitingCloudletsRatio() {
        final long arrivedCloudletsCount = simulationCore.getArrivedCloudletsCount();

        return arrivedCloudletsCount > 0
                ? (double) simulationCore.getNotYetRunningCloudletsCount() / (double) arrivedCloudletsCount
                : 0.0;
    }

    /**
     * Collects the current simulation state and formats it into an ObservationState
     * object,
     * including padding for dynamic VM lists.
     */
    private ObservationState getCurrentState() {
        if (simulationCore == null || settings == null) {
            LOGGER.warn("Attempting to get state before simulation core/settings is initialized.");
            // Return a default empty/zero state matching the expected dimensions
            int maxHosts = settings != null ? settings.getHostsCount() : 10; // Use default if settings is null somehow
            int maxVms = maxPotentialVms > 0 ? maxPotentialVms : 50; // Use calculated max or default
            return new ObservationState(
                    new double[maxHosts], new double[maxHosts],
                    new double[maxVms], new int[maxVms], new int[maxVms], getInfrastructureObservation(),
                    0, 0, 0, 0);
        }

        // Initialize padded arrays
        int numHosts = settings.getHostsCount();
        double[] hostLoads = new double[numHosts];
        double[] hostRamUsageRatio = new double[numHosts];

        double[] vmLoads = new double[maxPotentialVms]; // Padded (-1=off)
        int[] vmTypes = new int[maxPotentialVms]; // Padded (-1=off)
        int[] vmHostMap = new int[maxPotentialVms]; // Padded (-1=off)
        Arrays.fill(vmLoads, -1); // Default to -1
        Arrays.fill(vmTypes, -1); // Default to -1
        Arrays.fill(vmHostMap, -1); // Default to -1

        // Populate Host data (assuming host IDs are 0 to numHosts-1)
        List<Host> currentHosts = simulationCore.getDatacenter().getHostList();
        for (int i = 0; i < numHosts; i++) {
            if (i < currentHosts.size()) { // Check bounds
                Host host = currentHosts.get(i);
                if (host != null && host != Host.NULL && host.isActive()) { // Check if host is valid and active
                    hostLoads[i] = host.getCpuPercentUtilization();
                    hostRamUsageRatio[i] = host.getRam().getPercentUtilization();
                } else { // Handle inactive or null hosts if possible in future scenarios
                    hostLoads[i] = 0.0;
                    hostRamUsageRatio[i] = 0.0;
                }
            } else {
                // Should not happen with fixed hosts, but handle defensively
                hostLoads[i] = 0.0;
                hostRamUsageRatio[i] = 0.0;
            }
        }

        // Populate VM data into padded arrays
        List<Vm> currentVms = simulationCore.getVmPool(); // Gets broker's exec list
        int actualVmCount = 0;
        for (Vm vm : currentVms) {
            if (vm != null && vm.isCreated() && !vm.isFailed()) {
                int vmId = (int) vm.getId();
                // Ensure vmId is within the bounds of our observation arrays
                if (vmId >= 0 && vmId < maxPotentialVms) {
                    vmLoads[vmId] = vm.getCpuPercentUtilization();
                    vmTypes[vmId] = vmTypeStringToIndex(vm.getDescription());
                    vmHostMap[vmId] = (int) vm.getHost().getId(); // Store host ID
                    actualVmCount++;
                } else {
                    LOGGER.warn("VM ID {} is out of bounds for observation array size {}. Ignoring VM.", vmId,
                            maxPotentialVms);
                }
            }
            // Ignore VMs that are not created, failed, or null
        }

        // Get Queue state
        int waitingCloudlets = (simulationCore.getBroker() != null)
                ? simulationCore.getBroker().getWaitingCloudletCount()
                : 0;
        Cloudlet nextCloudlet = (simulationCore.getBroker() != null) ? simulationCore.getBroker().peekWaitingCloudlet()
                : null;
        int nextCloudletPes = (int) ((nextCloudlet != null) ? nextCloudlet.getPesNumber() : 0);

        return new ObservationState(
                hostLoads, hostRamUsageRatio, vmLoads, vmTypes, vmHostMap, getInfrastructureObservation(),
                waitingCloudlets, nextCloudletPes, actualVmCount, numHosts);
    }

    /** Helper to convert VM type string (S, M, L) to integer index (1, 2, 3). */
    private int vmTypeStringToIndex(String type) {
        // Handle potential temporary description like "S-host5"
        String actualType = type.contains("-") ? type.substring(0, type.indexOf('-')) : type;
        return switch (actualType) {
            case SimulationSettings.SMALL -> 1;
            case SimulationSettings.MEDIUM -> 2;
            case SimulationSettings.LARGE -> 3;
            default -> {
                LOGGER.warn("Unrecognized VM type in description: '{}'. Mapping to 0 (Off).", type);
                yield 0; // Map unknown/null to 0
            }
        };
    }

    /**
     * Gets a string representation of the current simulation state for rendering.
     */
    public String getRenderInfo() {
        if (simulationCore == null) {
            return "Simulation not initialized.";
        }
        ObservationState state = getCurrentState();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Time: %s | Step: %d\n",
                df.format(simulationCore.getClock()), currentStep));
        sb.append(String.format("Hosts (%d): ", state.getActualHostCount()));
        for (int i = 0; i < state.getActualHostCount(); i++) {
            sb.append(String.format("H%d[CPU:%.1f%% RAM:%.1f%%] ",
                    i,
                    state.getHostLoads()[i] * 100,
                    state.getHostRamUsageRatio()[i] * 100));
        }
        sb.append("\n");
        sb.append(String.format("VMs (%d / %d potential): ",
                state.getActualVmCount(), maxPotentialVms));
        for (int i = 0; i < maxPotentialVms; i++) {
            if (state.getVmTypes()[i] > 0) {
                String type = switch (state.getVmTypes()[i]) {
                    case 1 -> "S";
                    case 2 -> "M";
                    case 3 -> "L";
                    default -> "?";
                };
                sb.append(String.format("V%d(%s@H%d)[CPU:%.1f%%] ",
                        i,
                        type,
                        state.getVmHostMap()[i],
                        state.getVmLoads()[i] * 100));
            }
        }
        if (state.getActualVmCount() == 0) {
            sb.append("(None)");
        }
        sb.append("\n");
        sb.append(String.format("Queue: %d waiting | Next PEs: %d\n",
                state.getWaitingCloudlets(),
                state.getNextCloudletPes()));
        sb.append("Infrastructure tree:\n");
        appendInfrastructureTree(sb, state.getInfrastructureObservation());
        sb.append("\n");
        sb.append("--------------------\n");

        return sb.toString();
    }

    /**
     * Reads the flat infrastructureObservation array and appends
     * a human-friendly, indented tree to the StringBuilder.
     */
    private void appendInfrastructureTree(StringBuilder sb, int[] obs) {
        int idx = 0;
        int totalCores = obs[idx++];
        int hostsNum = obs[idx++];
        sb.append(String.format("  Total cores: %d\n", totalCores));
        sb.append(String.format("  Hosts: %d\n", hostsNum));

        for (int h = 0; h < hostsNum; h++) {
            int hostPes = obs[idx++];
            int vmCount = obs[idx++];
            sb.append(String.format("    Host[%d]: PEs=%d  VMs=%d\n",
                    h, hostPes, vmCount));

            for (int v = 0; v < vmCount; v++) {
                int vmPes = obs[idx++];
                int jobCount = obs[idx++];
                sb.append(String.format("      VM[%d]: PEs=%d  Cloudlets=%d\n",
                        v, vmPes, jobCount));

                for (int j = 0; j < jobCount; j++) {
                    int jobPes = obs[idx++];
                    idx++; // skip the “0” child-count
                    sb.append(String.format("        Cloudlet[%d]: PEs=%d\n",
                            j, jobPes));
                }
            }
        }
    }

    private int[] getInfrastructureObservation() {
        // 1) Fetch hosts
        List<Host> hosts = simulationCore.getDatacenter().getHostList();
        int hostsNum = hosts.size();

        // 2) Use a dynamic list
        List<Integer> treeList = new ArrayList<>();

        // 3) Header: total cores & number of hosts
        treeList.add((int) simulationCore.getTotalHostCores());
        treeList.add(hostsNum);

        // 4) For each host, record its cores & VM count, then each VM, then each job
        for (Host host : hosts) {
            List<Vm> vmList = host.getVmList();
            treeList.add((int) host.getPesNumber());
            treeList.add(vmList.size());

            for (Vm vm : vmList) {
                if (vm != null && vm.isCreated() && !vm.isFailed()) {
                    List<Cloudlet> jobList = vm.getCloudletScheduler().getCloudletList();
                    treeList.add((int) vm.getPesNumber());
                    treeList.add(jobList.size());

                    for (Cloudlet job : jobList) {
                        treeList.add((int) job.getPesNumber());
                        treeList.add(0); // jobs have no children
                    }
                }
            }
        }

        // 5) Convert to primitive int[]
        return treeList.stream().mapToInt(Integer::intValue).toArray();
    }

    /** Allows Python to cleanly shut down the simulation and gateway. */
    public void close() {
        LOGGER.info("Received close request from Python.");
        if (simulationCore != null) {
            // Print results before stopping
            printSimulationResults();
            simulationCore.stopSimulation();
        }
        // Trigger JVM shutdown
        if (gatewayServer != null) {
            LOGGER.info("Initiating gateway shutdown via Main.initiateShutdown.");
            Main.initiateShutdown(this.gatewayServer);
        }
    }

    /**
     * Prints the simulation results using CloudletsTableBuilder.
     * Called internally by close().
     */
    private void printSimulationResults() {
        if (simulationCore == null || simulationCore.getBroker() == null) {
            LOGGER.error("Cannot print results, simulation core or broker is null.");
            return;
        }

        LoadBalancingBroker broker = simulationCore.getBroker();
        List<Cloudlet> finishedList = broker.getCloudletFinishedList();
        List<Vm> vmList = broker.getVmCreatedList();

        if (vmList.isEmpty()) {
            LOGGER.info("No VMs finished to print in the results table.");
            return;
        }

        if (finishedList.isEmpty()) {
            LOGGER.info("No cloudlets finished to print in the results table.");
            return;
        }

        LOGGER.info("Simulation finished. Printing results tables...");

        // Sort the list before printing
        // Sort by Cloudlet Arrival Time first, then by VM ID, then by Cloudlet ID
        final Map<Long, Double> cloudletsArrivalTimeMap = broker.getCloudletArrivalTimeMap();
        final Comparator<Cloudlet> sortByVmId = comparingLong(c -> c.getVm().getId());
        final Comparator<Cloudlet> sortByArrivalTime = comparingDouble(c -> cloudletsArrivalTimeMap.get(c.getId()));
        List<Cloudlet> sortedList = new ArrayList<>(finishedList); // Create a copy to sort
        sortedList.sort(sortByArrivalTime.thenComparing(sortByVmId).thenComparing(Cloudlet::getId));

        // Create and build the tables
        CloudletsTableBuilderWithDetails cloudletsTableBuilder = new CloudletsTableBuilderWithDetails(sortedList,
                new CsvTable(), simulationCore.getBroker().getCloudletArrivalTimeMap());
        cloudletsTableBuilder.build(); // Prints the table to the console (default output)
        String outputPath = String.format("results/%s/cloudlets.csv", simName);
        logAndSaveTable((AbstractTable) cloudletsTableBuilder.getTable(), outputPath);
        LOGGER.info("Cloudlet results table printed.");

        VmsTableBuilderWithDetails vmTableBuilder = new VmsTableBuilderWithDetails(vmList,
                new CsvTable());
        vmTableBuilder.build(); // Prints the table to the console (default output)
        outputPath = String.format("results/%s/vms.csv", simName);
        logAndSaveTable((AbstractTable) vmTableBuilder.getTable(), outputPath);
        LOGGER.info("VM results table printed.");

        List<Host> hostList = simulationCore.getDatacenter().getHostList();
        hostList.forEach(this::printHostHistory);
        LOGGER.info("Host results table printed.");

        showTotalCost(sortedList);

        LOGGER.info("Total simulation time: {} seconds", simulationCore.getClock());
    }

    /**
     * Calculates and prints the total and mean cost as well as the mean CPU time
     * for the finished cloudlets.
     *
     * @param finishedCloudlets list of finished cloudlets.
     */
    private void showTotalCost(List<Cloudlet> finishedCloudlets) {
        LOGGER.info("Calculating simulation costs and statistics...");
        double totalCost = 0.0;
        double totalVmCost = 0.0;
        double totalCompletionTime = 0.0;
        double totalVmCpuUtilization = 0.0;
        int count;
        int vmCount = 0;

        for (Cloudlet cloudlet : finishedCloudlets) {
            CloudletCost cloudletCost = new CloudletCost(cloudlet,
                    simulationCore.getBroker().getCloudletArrivalTimeMap());

            totalCost += cloudletCost.getTotalCost();
            final var totalWaitTime = Math.ceil(cloudlet.getStartTime()
                    - simulationCore.getBroker().getCloudletArrivalTimeMap().get(cloudlet.getId()));
            totalCompletionTime += (cloudlet.getTotalExecutionTime() + totalWaitTime);
        }

        for (Vm vm : simulationCore.getBroker().getVmCreatedList()) {
            if (vm.hasStarted() || vm.isFinished()) {
                if (vm.getCpuUtilizationStats().getMean() > 0) {
                    // Only consider VMs with CPU utilization > 0
                    totalVmCpuUtilization += vm.getCpuUtilizationStats().getMean() * 100.0;
                    totalVmCost += new org.cloudsimplus.vms.VmCost(vm).getTotalCost();
                    vmCount++;
                }
            }
        }

        count = finishedCloudlets.size();

        LOGGER.info("Total cost of executing " + count + " Cloudlets = $" + String.format("%.2f", totalCost));
        LOGGER.info("Total cost of executing " + vmCount + " VMs = $" + String.format("%.2f", totalVmCost));
        if (count > 0) {
            LOGGER.info("Mean cost of executing " + count + " Cloudlets = $"
                    + String.format("%.2f", totalCost / count));
            LOGGER.info("Mean cost of executing " + vmCount + " VMs = $"
                    + String.format("%.2f", totalVmCost / vmCount));
            LOGGER.info("Mean actual Completion time of " + count + " Cloudlets = "
                    + String.format("%.2f", totalCompletionTime / count));
            LOGGER.info("Mean CPU utilization of " + vmCount + " VMs = "
                    + String.format("%.2f", totalVmCpuUtilization / vmCount) + "%");
        } else {
            LOGGER.info("No finished Cloudlets to calculate mean costs/time.");
        }
    }

    private void printHostHistory(Host host) {
        final boolean cpuUtilizationNotZero = host.getStateHistory()
                .stream()
                .map(HostStateHistoryEntry::percentUsage)
                .anyMatch(cpuUtilization -> cpuUtilization > 0);

        if (cpuUtilizationNotZero) {
            HostHistoryTableBuilderCsv hostTableBuilder = new HostHistoryTableBuilderCsv(host, new CsvTable());
            hostTableBuilder.setTitle(host.toString());
            hostTableBuilder.build(); // Prints the table to the console (default output)
            String outputPath = String.format("results/%s/host%d.csv", simName, host.getId());
            logAndSaveTable((AbstractTable) hostTableBuilder.getTable(), outputPath);
        } else
            LOGGER.info("\t%s CPU was zero all the time%n", host);
    }

    /** Called by Main to set the server instance if shutdown is needed. */
    public void setGatewayServer(GatewayServer server) {
        this.gatewayServer = server;
    }

    public long getCurrentSeed() {
        return currentSeed;
    }

    public void setCurrentSeed(long currentSeed) {
        this.currentSeed = currentSeed;
    }
}
