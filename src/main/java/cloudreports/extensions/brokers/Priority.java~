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
import java.util.Iterator;
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
    private LinkedHashMap<Integer, Integer> vmSortedPriority;
    private static int allVMCreated = 0;
    
    public Priority(String name) throws Exception {
        super(name);
        this.currentDataCenterId = 0;
        this.vmStatesList = vmStatesList;
        addCloudSimEventListener(this);
    }

    /**
     * @return The VM id of a VM so that the number of active tasks on each VM
     * is kept evenly distributed among the VMs.
     */
    @Override
    public int getNextAvailableVm() {
        /*if (allVMCreated == 0) {
            vmId++;

            if (vmId >= vmStatesList.size()) {
                vmId = 0;
            }
            //Log.printLine( vmId + " Aj" );

        } else {*/
        int vmId = -1;
        
        int vmid;
        for (Iterator<Integer> itr = vmSortedPriority.keySet().iterator(); itr.hasNext();) {
            //Log.printLine(vmPriority.get(vmid) + " " + vmStatesList.get(vmid));
            vmid = itr.next();
            //Log.printLine(vmid + " " + vmStatesList.get(vmid));
            if (vmStatesList.get(vmid) == VirtualMachineState.AVAILABLE) {
                vmId = vmid;
                //Log.printLine("VMID with priority is given. :"  + vmSortedPriority.get(vmid) + " " + vmid );
                break;
            }
        }

        //}
        allocatedVm(vmId);

        return vmId;

    }

    public void cloudSimEventFired(CloudSimEvent e) {
        if (e.getId() == CloudSimEvents.EVENT_CLOUDLET_ALLOCATED_TO_VM) {
            int vmId = (Integer) e.getParameter("vmId");
            vmStatesList.put(vmId, VirtualMachineState.BUSY);
        } else if (e.getId() == CloudSimEvents.EVENT_VM_FINISHED_CLOUDLET) {
            int vmId = (Integer) e.getParameter("vmId");
            vmStatesList.put(vmId, VirtualMachineState.AVAILABLE);
        } else if (e.getId() == CloudSimEvents.EVENT_ALL_VM_CREATED) {
            calculatePriority();
            allVMCreated = 1;
            vmSortedPriority = sortHashMapByValue((HashMap<Integer, Integer>) vmPriority);
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
