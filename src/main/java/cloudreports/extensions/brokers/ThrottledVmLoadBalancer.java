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
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author AjinkyaWavare
 */
public class ThrottledVmLoadBalancer extends Broker implements CloudSimEventListener {

    int currentDataCenterId;

    public ThrottledVmLoadBalancer(String name) throws Exception {
        super(name);
        this.currentDataCenterId = 0;
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

    @Override
    public int getNextAvailableVm() {
        int vmId = -1;

        if (vmStatesList.size() > 0) {
            int temp;
            for (Iterator<Integer> itr = vmStatesList.keySet().iterator(); itr.hasNext();) {
                temp = itr.next();
                VirtualMachineState state = vmStatesList.get(temp); 
                if (state.equals(VirtualMachineState.AVAILABLE)) {
                    vmId = temp;
                    break;
                }
            }
        }

        allocatedVm(vmId);

        return vmId;

    }

    public void cloudSimEventFired(CloudSimEvent e) {
        if (e.getId() == CloudSimEvents.EVENT_CLOUDLET_ALLOCATED_TO_VM) {
            int vmId = (Integer) e.getParameter("vm_id");
            vmStatesList.put(vmId, VirtualMachineState.BUSY);
        } else if (e.getId() == CloudSimEvents.EVENT_VM_FINISHED_CLOUDLET) {
            int vmId = (Integer) e.getParameter("vm_id");
            vmStatesList.put(vmId, VirtualMachineState.AVAILABLE);
        }
    }

}
