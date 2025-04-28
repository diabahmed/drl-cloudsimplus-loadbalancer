package giu.edu.cspg.utils;

import giu.edu.cspg.CloudletCost;
import giu.edu.cspg.LoadBalancingBroker;
import giu.edu.cspg.SimulationCore;
import giu.edu.cspg.tables.CloudletsTableBuilderWithDetails;
import giu.edu.cspg.tables.HostHistoryTableBuilderCsv;
import giu.edu.cspg.tables.TableLogger;
import giu.edu.cspg.tables.VmsTableBuilderWithDetails;

import org.cloudsimplus.builders.tables.AbstractTable;
import org.cloudsimplus.builders.tables.CsvTable;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostStateHistoryEntry;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmCost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import static java.util.Comparator.comparingDouble;
import static java.util.Comparator.comparingLong;
import java.util.List;
import java.util.Map;

public class SimulationResultUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimulationResultUtils.class.getSimpleName());

    /**
     * Prints standard simulation results tables (Cloudlets, VMs, Hosts) and saves
     * them to CSV files.
     * 
     * @param simulationCore The simulation core instance after the simulation has
     *                       finished.
     * @param baseFileName   A base name for the output files (e.g.,
     *                       "SimulationName_PolicyName").
     */
    public static void printAndSaveResults(SimulationCore simulationCore, String baseFileName) {
        LOGGER.info("Processing simulation results for {}...", baseFileName);

        LoadBalancingBroker broker = simulationCore.getBroker();
        if (broker == null) {
            LOGGER.error("Broker is null. Cannot print results.");
            return;
        }

        // --- Cloudlet Results ---
        List<Cloudlet> finishedList = broker.getCloudletFinishedList();
        if (finishedList.isEmpty()) {
            LOGGER.info("No cloudlets finished to print in the results table.");
        } else {
            LOGGER.info("Generating Cloudlet Results Table ({} finished)...", finishedList.size());
            final Map<Long, Double> arrivalTimeMap = broker.getCloudletArrivalTimeMap();
            // Sort Cloudlets (by Arrival Time first, then by VM ID, then by Cloudlet ID)
            List<Cloudlet> sortedCloudlets = new ArrayList<>(finishedList);
            final Comparator<Cloudlet> sortByVmId = comparingLong(c -> c.getVm().getId());
            final Comparator<Cloudlet> sortByArrivalTime = comparingDouble(c -> arrivalTimeMap.get(c.getId()));
            sortedCloudlets.sort(sortByArrivalTime.thenComparing(sortByVmId).thenComparing(Cloudlet::getId));

            CloudletsTableBuilderWithDetails cloudletsTable = new CloudletsTableBuilderWithDetails(sortedCloudlets,
                    new CsvTable(), arrivalTimeMap);
            cloudletsTable.build();
            String cloudletPath = String.format("results/%s/cloudlets.csv", baseFileName);
            TableLogger.logAndSaveTable((AbstractTable) cloudletsTable.getTable(), cloudletPath);
            LOGGER.info("Cloudlet results saved to {}", cloudletPath);
        }

        // --- VM Results ---
        List<Vm> vmList = broker.getVmCreatedList();
        if (vmList.isEmpty()) {
            LOGGER.info("No VMs were created or tracked for results.");
        } else {
            LOGGER.info("Generating VM Results Table ({} total)...", vmList.size());
            VmsTableBuilderWithDetails vmsTable = new VmsTableBuilderWithDetails(vmList, new CsvTable());
            vmsTable.build();
            String vmPath = String.format("results/%s/vms.csv", baseFileName);
            TableLogger.logAndSaveTable((AbstractTable) vmsTable.getTable(), vmPath);
            LOGGER.info("VM results saved to {}", vmPath);
        }

        // --- Host Results ---
        List<Host> hostList = simulationCore.getDatacenter().getHostList();
        if (hostList.isEmpty()) {
            LOGGER.info("No Hosts were created.");
        } else {
            LOGGER.info("Generating Host History Tables ({} hosts)...", hostList.size());
            hostList.forEach(host -> printAndSaveHostHistory(host, baseFileName));
            LOGGER.info("Host history saved.");
        }

        // --- Overall Cost/Stats ---
        showOverallStats(broker, vmList, finishedList);

        LOGGER.info("Total simulation time: {} seconds", simulationCore.getClock());
        LOGGER.info("Result processing finished for {}.", baseFileName);
    }

    /** Prints and saves the state history for a single host. */
    private static void printAndSaveHostHistory(Host host, String baseFileName) {
        // Only print/save if the host was actually used
        final boolean cpuUtilizationNotZero = host.getStateHistory()
                .stream()
                .map(HostStateHistoryEntry::percentUsage)
                .anyMatch(cpuUtilization -> cpuUtilization > 0);

        if (cpuUtilizationNotZero) {
            HostHistoryTableBuilderCsv hostTableBuilder = new HostHistoryTableBuilderCsv(host, new CsvTable());
            hostTableBuilder.build();
            String hostPath = String.format("results/%s/host%d.csv", baseFileName, host.getId());
            TableLogger.logAndSaveTable((AbstractTable) hostTableBuilder.getTable(), hostPath);
        } else {
            LOGGER.info("\tHost {} was not utilized, skipping history table.", host.getId());
        }
    }

    /** Calculates and logs overall simulation statistics. */
    private static void showOverallStats(LoadBalancingBroker broker, List<Vm> vmList,
            List<Cloudlet> finishedCloudlets) {
        LOGGER.info("Calculating Overall Simulation Statistics...");

        double totalCloudletCost = 0.0;
        double totalVmCost = 0.0;
        double totalCompletionTime = 0.0;
        double totalVmCpuUtilizationSum = 0.0;
        int finishedCount = finishedCloudlets.size();
        int utilizedVmCount = 0;
        Map<Long, Double> arrivalTimeMap = broker.getCloudletArrivalTimeMap();

        for (Cloudlet cloudlet : finishedCloudlets) {
            CloudletCost cloudletCost = new CloudletCost(cloudlet, arrivalTimeMap);
            totalCloudletCost += cloudletCost.getTotalCost();

            // Calculate completion time = execution time + wait time
            Double arrival = arrivalTimeMap.get(cloudlet.getId());
            if (arrival != null) {
                final var totalWaitTime = Math.ceil(cloudlet.getStartTime() - arrival);
                totalCompletionTime += (cloudlet.getTotalExecutionTime() + totalWaitTime);
            }
        }

        for (Vm vm : vmList) {
            if (vm.hasStarted() || vm.isFinished()) {
                if (vm.getCpuUtilizationStats().getMean() > 0) {
                    totalVmCost += new VmCost(vm).getTotalCost();
                    totalVmCpuUtilizationSum += vm.getCpuUtilizationStats().getMean() * 100.0;
                    utilizedVmCount++;
                }
            }
        }

        LOGGER.info("Total cost of executing {} Cloudlets = ${}", finishedCount,
                String.format("%.2f", totalCloudletCost));
        LOGGER.info("Total cost of running {} VMs = ${}", utilizedVmCount, String.format("%.2f", totalVmCost));

        if (finishedCount > 0) {
            LOGGER.info("Mean cost per Cloudlet = ${}", String.format("%.2f", totalCloudletCost / finishedCount));
            LOGGER.info("Mean Completion Time per Cloudlet = {} seconds",
                    String.format("%.2f", totalCompletionTime / finishedCount));
        } else {
            LOGGER.info("No finished Cloudlets to calculate mean costs/time.");
        }

        if (utilizedVmCount > 0) {
            LOGGER.info("Mean cost per Utilized VM = ${}", String.format("%.2f", totalVmCost / utilizedVmCount));
            LOGGER.info("Mean CPU Utilization of Utilized VMs = {}%",
                    String.format("%.2f", totalVmCpuUtilizationSum / utilizedVmCount));
        } else {
            LOGGER.info("No VMs were utilized to calculate mean VM costs/utilization.");
        }
    }
}
