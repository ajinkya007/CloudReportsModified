package cloudreports.extensions.brokers;

import cloudreports.enums.BrokerPolicy;
import cloudreports.enums.VirtualMachineState;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 * A specific type of {@link Broker} that schedules virtual machines in a
 * round-robin fashion.
 *
 * @see Broker
 * @see BrokerPolicy#ROUND_ROBIN
 * @author Thiago T. Sá
 * @since 1.0
 */
public class RoundRobinVirtualMachine extends Broker {

    /**
     * The current datacenter id used to schedule virtual machines.
     */
    int currentId;
    private int currVm = -1;

    /**
     * Initializes a new instance of this class with the given name.
     *
     * @param name the name of the broker.
     * @throws java.lang.Exception
     * @since 1.0
     */
    public RoundRobinVirtualMachine(String name) throws Exception {
        super(name);
        this.currentId = 0;

        

    }

    /**
     * Gets an id of a datacenter managed by this broker.
     *
     * @return an id of a datacenter.
     * @since 1.0
     */
    @Override
    public int getDatacenterId() {
        currentId++;
        if (currentId >= vmStatesList.size()) {
            currentId = 0;
        }
        int index = currentId % getDatacenterIdsList().size();
        return getDatacenterIdsList().get(index);
    }

    /**
     * Gets a list of ids from datacenters managed by this broker.
     *
     * @return a list of datacenter ids.
     * @since 1.0
     */
    @Override
    public List<Integer> getDatacenterIdList() {
        return getDatacenterIdsList();
    }

    @Override
    public int getNextAvailableVm() {
        currVm++;

        if (currVm >= vmStatesList.size()) {
            currVm = 0;
        }

        allocatedVm(currVm);

        return currVm;

    }
}
