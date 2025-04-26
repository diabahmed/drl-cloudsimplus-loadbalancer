package giu.edu.cspg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.core.CloudSimTag;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.traces.SwfWorkloadFileReader;
import org.cloudsimplus.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SimulationCore {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimulationCore.class.getSimpleName());

    private final SimulationSettings settings;
    private CloudSimPlus simulation;
    private Datacenter datacenter;
    private LoadBalancingBroker broker;
    private List<Host> hostList;
    private List<Vm> vmPool; // This will hold the initial fleet + dynamically created ones
    private List<Cloudlet> cloudletList; // Full list of cloudlets from the trace
    private boolean firstStep; // Flag to indicate if it's the first step of the simulation

    /**
     * Initializes the simulation core with specific settings.
     * 
     * @param settings The simulation settings to use.
     */
    public SimulationCore(SimulationSettings settings) {
        if (settings == null) {
            throw new IllegalArgumentException("SimulationSettings cannot be null.");
        }
        this.settings = settings;
        this.resetSimulation(); // Internal reset to initialize the simulation environment
    }

    /**
     * Resets and initializes the simulation environment with a fixed initial fleet.
     * Loads SWF trace and queues cloudlets.
     */
    public void resetSimulation() {
        LOGGER.info("Resetting CloudSim simulation environment (Initial Fixed Fleet)...");
        stopSimulation(); // Ensure previous run is stopped cleanly

        // Initialize core CloudSimPlus engine
        simulation = new CloudSimPlus(settings.getMinTimeBetweenEvents());
        firstStep = true;

        // Initialize lists
        hostList = new ArrayList<>();
        vmPool = new ArrayList<>(); // Start with empty pool for initial fleet creation
        cloudletList = new ArrayList<>();

        // --- Step 1: Load Cloudlets from SWF or CSV trace file ---
        if (settings.getWorkloadMode().equalsIgnoreCase("SWF")) {
            loadCloudletsFromSwf();
        } else if (settings.getWorkloadMode().equalsIgnoreCase("CSV")) {
            LOGGER.warn("CSV workload mode is not implemented yet. Please use SWF mode.");
            throw new UnsupportedOperationException("CSV workload mode is not implemented yet.");
        } else {
            LOGGER.error("Invalid workload mode: {}", settings.getWorkloadMode());
            throw new IllegalArgumentException("Invalid workload mode: " + settings.getWorkloadMode());
        }

        // Initialize broker
        broker = new LoadBalancingBroker(simulation, cloudletList);
        broker.setShutdownWhenIdle(false); // important to keep the broker running

        // --- Step 2: Create Hosts and Datacenter ---
        // DatacenterSetup.createDatacenter creates the fixed number of hosts
        // Use VmAllocationPolicyCustom for future dynamic VM creation by agent
        datacenter = DatacenterSetup.createDatacenter(simulation, settings, this.hostList,
                new VmAllocationPolicyCustom());
        LOGGER.info("Datacenter created with {} hosts.", hostList.size());

        // --- Step 3: Create Initial VM Fleet ---
        DatacenterSetup.createInitialVmFleet(settings, this.vmPool); // Populates vmPool
        LOGGER.info("Initial VM fleet created with {} VMs.", vmPool.size());

        // --- Step 4: Submit the Initial VM Fleet to the broker ---
        // Broker needs to know about these VMs for placement and tracking
        broker.submitVmList(new ArrayList<>(vmPool)); // Submit a copy

        // This is important to ensure the broker knows about the cloudlets
        ensureAllCloudletsCompleteBeforeSimulationEnds();

        // --- Step 5: Start the simulation engine ---
        simulation.startSync();
        // initialize the simulation to allow the datacenter to be created
        proceedClockTo(settings.getMinTimeBetweenEvents());

        LOGGER.info("Simulation reset complete. {} Hosts, {} Initial VMs created. {} Cloudlets queued.",
                hostList.size(), vmPool.size(), broker.getWaitingCloudletCount());
    }

    /**
     * Loads cloudlets using SwfWorkloadFileReader and associates them with the
     * broker.
     * (This method populates this.cloudletList)
     */
    private void loadCloudletsFromSwf() {
        String filePath = settings.getCloudletTraceFile();
        int referenceMips = settings.getWorkloadReaderMips();
        int maxCloudletsToCreateFromWorkloadFile = settings.getMaxCloudletsToCreateFromWorkloadFile();

        LOGGER.info("Loading cloudlet trace from SWF file: {} using MIPS reference: {}", filePath, referenceMips);
        try {
            SwfWorkloadFileReader reader = new SwfWorkloadFileReader(filePath, referenceMips);
            reader.setMaxLinesToRead(maxCloudletsToCreateFromWorkloadFile);
            this.cloudletList = reader.generateWorkload();
            LOGGER.info("Loaded and processed {} cloudlets from trace.", cloudletList.size());

        } catch (IllegalArgumentException e) {
            LOGGER.error("Invalid argument for SWF Reader (e.g., MIPS <= 0 or invalid path): {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            LOGGER.error("Error processing SWF trace file: {}", filePath, e);
            throw new RuntimeException("Failed to load or process SWF trace: " + filePath, e);
        }
    }

    /**
     * Ensures that all cloudlets are completed before the simulation ends.
     * This method sets up an event listener that checks if there are unfinished
     * cloudlets when there is only one future event left.
     * If there are unfinished cloudlets, it sends an empty event
     * to keep the simulation running.
     */
    private void ensureAllCloudletsCompleteBeforeSimulationEnds() {
        double interval = settings.getSimulationTimestep();
        simulation.addOnEventProcessingListener(info -> {
            if (getNumberOfFutureEvents() == 1 && broker.hasUnfinishedCloudlets()) {
                LOGGER.trace("Cloudlets not finished. Sending empty event to keep simulation running.");
                simulation.send(datacenter, datacenter, interval, CloudSimTag.NONE, null);
            }
        });
    }

    /**
     * Advances the simulation clock to the specified target time. This method runs
     * the simulation
     * in increments until the target time is reached or the maximum number of
     * iterations is
     * exceeded to prevent an infinite loop.
     *
     * @param targetTime The target time to advance the simulation clock to.
     */
    private void proceedClockTo(final double targetTime) {
        if (simulation == null) {
            throw new IllegalStateException("Simulation not initialized. Call resetSimulation first.");
        }
        if (!simulation.isRunning()) {
            LOGGER.warn("Attempting to run a simulation that is not running.");
        }

        double adjustedInterval = targetTime - getClock();
        int maxIterations = 1000; // Safety check to prevent infinite loop
        int iterations = 0;

        // Run the simulation until the target time is reached
        while (simulation.runFor(adjustedInterval) < targetTime) {
            // Calculate the remaining time to the target
            adjustedInterval = targetTime - getClock();
            // Use the minimum time between events if the remaining time is non-positive
            adjustedInterval = adjustedInterval <= 0 ? settings.getMinTimeBetweenEvents() : adjustedInterval;

            // Increment the iteration counter and break if it exceeds the maximum allowed
            // iterations
            if (++iterations >= maxIterations) {
                LOGGER.warn(
                        "Exceeded maximum iterations in runForInternal. Breaking the loop to prevent infinite loop.");
                break;
            }
        }
    }

    double calculateTargetTime() {
        // if first step we have already done 0.1 and we need to finish the first step
        // at 1
        // else, just add 1 to the current time
        final double targetTime;
        if (firstStep) {
            targetTime = settings.getSimulationTimestep();
        } else {
            targetTime = getClock() + settings.getSimulationTimestep();
        }
        return targetTime;
    }

    public void runOneTimestep() {
        final double oldClock = getClock();
        final double startTime = Math.max(getClock() - settings.getSimulationTimestep(), 0);
        final double targetTime = calculateTargetTime();
        ensureSimulationIsRunning();
        broker.clearFinishedWaitTimes(); // Clear finished wait times for the next timestep
        clearListsIfNeeded();
        List<Cloudlet> cloudletsToQueueList = broker.getCloudletsToQueueAtThisTimestep(targetTime, startTime,
                this.getClock());
        proceedClockTo(targetTime);
        final double startTimeNew = getClock() - settings.getSimulationTimestep();
        broker.queueCloudletsAtThisTimestep(cloudletsToQueueList, startTimeNew, getClock());
        if (shouldPrintStats()) {
            printStats();
        }
        if (firstStep) {
            firstStep = false;
        }
        LOGGER.info("VMs running: {}", broker.getVmExecList().size());
        LOGGER.info("{}: Proceeding clock to {}", oldClock, targetTime);
    }

    /**
     * Ensures that the simulation is currently running. If the simulation is not
     * running, it throws an IllegalStateException.
     *
     * @throws IllegalStateException if the simulation is not running.
     */
    private void ensureSimulationIsRunning() {
        if (!isRunning()) {
            throw new IllegalStateException(
                    "Simulation is not running. Please reset or create a new one!");
        }
    }

    /**
     * Clears the lists of created cloudlets and VMs if needed to prevent
     * OutOfMemoryError (OOM).
     * The lists can grow to large sizes as cloudlets are re-scheduled when VMs are
     * terminated. This
     * method checks a setting to determine if the lists should be cleared. It is
     * safe to clear
     * these lists in the current environment because they are only used in
     * CloudSimPlus when a VM
     * is being upscaled, which is not performed in this environment.
     */
    private void clearListsIfNeeded() {
        if (settings.isClearCreatedLists()) {
            broker.getCloudletCreatedList().clear();
            // broker.getVmCreatedList().clear();
        }
    }

    private boolean shouldPrintStats() {
        return ((int) Math.round(getClock()) % 1 == 0) || !isRunning();
    }

    /**
     * Logs various statistics about the jobs and VMs in the simulation.
     * <ul>
     * <li>Total number of VMs created.</li>
     * <li>Number of VMs currently running.</li>
     * <li>Total number of jobs.</li>
     * <li>Count of jobs by their status.</li>
     * </ul>
     */
    public void printStats() {
        // Contradictory to the previous functions which are called before the clock is
        // procceded,
        // this function is called after the clock is procceded. So, we need to
        // calculate the start
        // time of the timestep (instead of the target time) to get the correct
        // statistics.
        final double startTime = getClock() - settings.getSimulationTimestep();

        LOGGER.info("[{} - {}]: All jobs: {} ", startTime, getClock(), cloudletList.size());
        Map<Cloudlet.Status, Integer> countByStatus = new HashMap<>();
        for (Cloudlet c : cloudletList) {
            final Cloudlet.Status status = c.getStatus();
            int count = countByStatus.getOrDefault(status, 0);
            countByStatus.put(status, count + 1);
        }

        for (Map.Entry<Cloudlet.Status, Integer> e : countByStatus.entrySet()) {
            LOGGER.info("[{} - {}]: {}: {}", startTime,
                    getClock(), e.getKey().toString(),
                    e.getValue());
        }

        LOGGER.info("[{} - {}]: Arrived Cloudlets: {}", startTime, getClock(), getArrivedCloudletsCount());
    }

    /** Stops the simulation. */
    public void stopSimulation() {
        if (simulation != null && this.isRunning()) {
            simulation.terminate();
            LOGGER.info("CloudSim simulation terminated.");
        }
    }

    /** Checks if the simulation is considered active. */
    public boolean isRunning() {
        if (simulation == null || !simulation.isRunning()) {
            return false;
        }
        boolean waiting = broker != null && broker.hasUnfinishedCloudlets();
        return waiting && simulation.isRunning();
    }

    // --- Getters ---
    public CloudSimPlus getSimulation() {
        return simulation;
    }

    public Datacenter getDatacenter() {
        return datacenter;
    }

    public LoadBalancingBroker getBroker() {
        return broker;
    }

    // This now returns the potentially dynamic list of VMs known to the broker
    public List<Vm> getVmPool() {
        return broker != null ? broker.getVmExecList() : new ArrayList<>();
    }

    public double getClock() {
        return simulation != null ? simulation.clock() : 0;
    }

    public int getCloudletTotalCount() {
        return this.cloudletList != null ? cloudletList.size() : 0;
    }

    public long getNumberOfFutureEvents() {
        return simulation.getNumberOfFutureEvents(simEvent -> true);
    }

    public long getAllocatedCores() {
        final long reduce = broker.getVmExecList().parallelStream().map(Vm::getPesNumber)
                .reduce(Long::sum).orElse(0L);
        return reduce;
    }

    public long getTotalHostCores() {
        return this.getDatacenter().getHostList().stream()
                .mapToLong(Host::getPesNumber).sum();
    }

    public long getArrivedCloudletsCount() {
        return broker.getCloudletArrivalTimeMap().entrySet().parallelStream()
                .filter(entry -> entry.getValue() <= getClock()).count();
    }

    public long getNotYetRunningCloudletsCount() {
        return broker.getInputCloudlets().parallelStream()
                .filter(cloudlet -> broker.getCloudletArrivalTimeMap().get(cloudlet.getId()) <= getClock())
                .filter(cloudlet -> !cloudlet.getStatus().equals(Cloudlet.Status.INEXEC))
                .filter(cloudlet -> !cloudlet.getStatus().equals(Cloudlet.Status.SUCCESS)).count();
    }

    // --- Methods for dynamic VM management (to be called by Gateway) ---

    /** Requests the creation of a VM on a specific host. */
    public boolean createVmOnHost(String type, long hostId) {
        if (simulation == null || broker == null) {
            LOGGER.error("Cannot create VM, simulation or broker not initialized.");
            return false;
        }
        LOGGER.debug("{}: Agent Requesting creation of {} VM on Host {}", getSimulation().clockStr(), type, hostId);

        final Host host = datacenter.getHostById(hostId);

        if (host == Host.NULL) {
            LOGGER.debug("Vm creating ignored, no host with given id found");
            return false;
        }

        Vm newVm = DatacenterSetup.createVm(settings, type); // Uses the static method now
        newVm.setDescription(type + "-" + hostId); // Temporary description for placement policy
        newVm.setBroker(broker); // Associate VM with broker

        if (!host.isSuitableForVm(newVm)) {
            LOGGER.debug("Vm creating ignored, host not suitable");
            LOGGER.debug("Host Vm List Size: {}", host.getVmList().size());
            LOGGER.debug("Total Vm Exec List Size: {}", broker.getVmExecList().size());
            return false;
        }

        broker.submitVm(newVm); // Submit to broker
        return true; // Request submitted
    }

    /** Requests the destruction of a VM by its ID. */
    public boolean destroyVmById(int vmId) {
        if (simulation == null || broker == null) {
            LOGGER.error("Cannot destroy VM, simulation or broker not initialized.");
            return false;
        }
        LOGGER.debug("{}: Agent Requesting destruction of VM {}", getSimulation().clockStr(), vmId);

        if (vmId >= broker.getVmExecList().size()) {
            LOGGER.warn("Can't kill VM with id {}: no such id found", vmId);
            return false;
        }

        Vm vmToDestroy = broker.getVmExecList().get(vmId);
        if (vmToDestroy == Vm.NULL) {
            LOGGER.warn("Cannot destroy VM {}: Not found in broker's list.", vmId);
            return false;
        }
        if (!vmToDestroy.isCreated() || vmToDestroy.isFailed()) {
            LOGGER.warn("Cannot destroy VM {}: Not running or already failed (State: {}).", vmId,
                    vmToDestroy.getStateHistory());
            return false;
        }

        // Reschedule cloudlets first
        broker.rescheduleFailedCloudlets(vmToDestroy);

        // Send destruction request (happens after delay)
        LOGGER.info("Destroying VM {} with delay {}", vmId, settings.getVmShutdownDelay());
        // Deallocate VM from host (this internally calls Host.destroyVm())
        datacenter.getVmAllocationPolicy().deallocateHostForVm(vmToDestroy);

        return true; // Destruction initiated
    }
}
