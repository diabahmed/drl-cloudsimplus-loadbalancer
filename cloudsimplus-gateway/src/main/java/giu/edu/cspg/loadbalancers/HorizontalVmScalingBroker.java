package giu.edu.cspg.loadbalancers;

import java.util.List;
import java.util.stream.Collectors;

import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.vms.Vm;

public class HorizontalVmScalingBroker extends DatacenterBrokerSimple {
    /**
     * Tracks the last selected VM index within the filtered list of suitable VMs.
     */
    private int lastSelectedSuitableVmIndex = -1;

    public HorizontalVmScalingBroker(CloudSimPlus simulation) {
        this(simulation, "");
    }

    public HorizontalVmScalingBroker(CloudSimPlus simulation, String name) {
        super(simulation, name);
    }

    @Override
    protected Vm defaultVmMapper(final Cloudlet cloudlet) {
        if (cloudlet.isBoundToVm()) {
            return cloudlet.getVm();
        }

        if (getVmExecList().isEmpty()) {
            LOGGER.warn("No VMs available for Cloudlet {}", cloudlet.getId());
            return Vm.NULL;
        }

        final List<Vm> suitableVms = getVmExecList().stream()
                .filter(vm -> vm.getPesNumber() >= cloudlet.getPesNumber())
                .filter(vm -> vm.getStorage().getAvailableResource() >= cloudlet.getFileSize())
                .filter(vm -> !vm.isFinished())
                .collect(Collectors.toList());

        if (suitableVms.isEmpty()) {
            LOGGER.error(
                    "No suitable VM for Cloudlet {} (PEs needed: {}). Available VMs: {}",
                    cloudlet.getId(), cloudlet.getPesNumber(), getVmExecList());
            return Vm.NULL;
        }

        // Round-robin selection within the filtered list
        lastSelectedSuitableVmIndex = (lastSelectedSuitableVmIndex + 1) % suitableVms.size();
        return suitableVms.get(lastSelectedSuitableVmIndex);
    }
}
