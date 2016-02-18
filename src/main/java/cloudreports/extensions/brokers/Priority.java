/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cloudreports.extensions.brokers;

import cloudreports.enums.VirtualMachineState;
import cloudreports.event.CloudSimEvent;
import cloudreports.event.CloudSimEventListener;
import cloudreports.event.CloudSimEvents;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.swing.RowFilter.Entry;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;

/**
 *
 * @author AjinkyaWavare
 */
public class Priority extends Broker implements CloudSimEventListener {

    /**
     * Holds the count current active allocations on each VM
     */
    private Map<Integer, Integer> currentAllocationCounts;
    int currentDataCenterId;

    public Priority(String name) throws Exception {
        super(name);
        this.currentDataCenterId = 0;
        this.vmStatesList = vmStatesList;
        this.currentAllocationCounts = Collections.synchronizedMap(new HashMap<Integer, Integer>());
        addCloudSimEventListener(this);
    }

    /**
     * @return The VM id of a VM so that the number of active tasks on each VM
     * is kept evenly distributed among the VMs.
     */
    @Override
    public int getNextAvailableVm() {
        int vmId = -1;
            //Find the vm with least number of allocations
        //If all available vms are not allocated, allocated the new ones
        for (Integer vmid : vmPriority.keySet()){
            if(vmStatesList.get(vmid) == VirtualMachineState.AVAILABLE){
                vmId = vmid;
                Log.printLine("VMID with priority is given. :"  + vmPriority.get(vmid) + " " + vmid );
            }
        }
        /*
        if (currentAllocationCounts.size() < vmStatesList.size()) {
            for (int availableVmId : vmStatesList.keySet()) {
                if (!currentAllocationCounts.containsKey(availableVmId)) {
                    vmId = availableVmId;
                    break;
                }
            }
        } else {
            int currCount;
            int minCount = Integer.MAX_VALUE;

            for (int thisVmId : currentAllocationCounts.keySet()) {
                currCount = currentAllocationCounts.get(thisVmId);
                if (currCount < minCount) {
                    minCount = currCount;
                    vmId = thisVmId;
                }
            }
        }
*/
        allocatedVm(vmId);

        return vmId;

    }

    public void cloudSimEventFired(CloudSimEvent e) {
        /*if (e.getId() == CloudSimEvents.EVENT_CLOUDLET_ALLOCATED_TO_VM) {
            int vmId = (Integer) e.getParameter("vmId");

            Integer currCount = currentAllocationCounts.remove(vmId);
            if (currCount == null) {
                currCount = 1;
            } else {
                currCount++;
            }

            currentAllocationCounts.put(vmId, currCount);

        } else if (e.getId() == CloudSimEvents.EVENT_VM_FINISHED_CLOUDLET) {
            int vmId = (Integer) e.getParameter("vmId");
            Integer currCount = currentAllocationCounts.remove(vmId);
            if (currCount != null) {
                currCount--;
                currentAllocationCounts.put(vmId, currCount);
            }
        } */
        if (e.getId() == CloudSimEvents.EVENT_CLOUDLET_ALLOCATED_TO_VM) {
            int vmId = (Integer) e.getParameter("vmId");
            vmStatesList.put(vmId, VirtualMachineState.BUSY);
        } else if (e.getId() == CloudSimEvents.EVENT_VM_FINISHED_CLOUDLET) {
            int vmId = (Integer) e.getParameter("vmId");
            vmStatesList.put(vmId, VirtualMachineState.AVAILABLE);
        } else if (e.getId() == CloudSimEvents.EVENT_ALL_VM_CREATED){
            calculatePriority();
            LinkedHashMap<Integer, Integer> vmSortedPriority = sortHashMapByValue((HashMap<Integer, Integer>) vmPriority);
            for (Map.Entry<Integer, Integer> entry : vmSortedPriority.entrySet()) {
                Log.printLine(entry.getKey()+" : "+entry.getValue() + " Aj");
            }
        }

    }

    @Override
    public int getDatacenterId() {
        currentDataCenterId++;
        if (currentDataCenterId >= vmStatesList.size()) {
            currentDataCenterId = 0;
        }
        int index = currentDataCenterId % getDatacenterIdsList().size();
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

}
