/* 
 * Copyright (c) 2010-2012 Thiago T. Sá
 * 
 * This file is part of CloudReports.
 *
 * CloudReports is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * CloudReports is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * For more information about your rights as a user of CloudReports,
 * refer to the LICENSE file or see <http://www.gnu.org/licenses/>.
 */
package cloudreports.extensions.brokers;

import cloudreports.dao.CustomerRegistryDAO;
import cloudreports.enums.BrokerPolicy;
import cloudreports.enums.VirtualMachineState;
import cloudreports.event.CloudSimEvent;
import cloudreports.event.CloudSimEventListener;
import cloudreports.event.CloudSimEvents;
import cloudreports.event.CloudsimObservable;
import cloudreports.models.CustomerRegistry;
import cloudreports.utils.RandomNumberGenerator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

/**
 * A subtype of CloudSim's DatacenterBroker class. All user-implemented new
 * broker policies must be a subtype of this class.
 *
 * @see BrokerPolicy
 * @author Thiago T. Sá
 * @since 1.0
 */
public abstract class Broker extends DatacenterBroker implements CloudsimObservable {

    protected int roundRobinDataCenter = 0;
    /**
     * The maximum length of cloudlets assigned to this broker.
     */
    private long maxLengthOfCloudlets;
    private List<CloudSimEventListener> listeners;
    /**
     * The cloudlet id.
     */
    private int cloudletId;
    /*
    *Vm states list and Prioirty list
     */
    protected Map<Integer, VirtualMachineState> vmStatesList;
    protected Map<Integer, Integer> vmPriority;
    /**
     * Holds the count of allocations for each VM
     */
    protected Map<Integer, Integer> vmAllocationCounts;

    /*
    *GCD calculations of various attributes of the Virtual Machines
     */
    int gcdRam;
    int gcdNumberOfPes;
    double gcdMips;

    public Map<Integer, VirtualMachineState> getVmStatesList() {
        return vmStatesList;
    }

    public void setVmStatesList(Map<Integer, VirtualMachineState> vmStatesList) {
        this.vmStatesList = vmStatesList;
    }

    /**
     * An abstract method whose implementations must return an id of a
     * datacenter.
     *
     * @return the id of a datacenter.
     * @since 1.0
     */
    public abstract int getDatacenterId();

    /**
     * An abstract method whose implementations must return a list of ids from
     * datacenters managed by the broker.
     *
     * @return a list of ids from datacenters managed by this broker.
     * @since 1.0
     */
    public abstract List<Integer> getDatacenterIdList();

    /**
     * The main contract of {@link VmLoadBalancer}. All load balancers should
     * implement this method according to their specific load balancing policy.
     *
     * @return id of the next available Virtual Machine to which the next task
     * should be allocated
     */
    abstract public int getNextAvailableVm();

    /**
     * Initializes a new instance of this class with the given name.
     *
     * @param name the name of the broker.
     * @since 1.0
     */
    public int totalVm;

    public Broker(String name) throws Exception {
        super(name);
        listeners = new ArrayList<CloudSimEventListener>();
        CustomerRegistryDAO crDAO = new CustomerRegistryDAO();
        CustomerRegistry cr = crDAO.getCustomerRegistry(name);
        this.cloudletId = cr.getUtilizationProfile().getNumOfCloudlets();
        this.maxLengthOfCloudlets = cr.getUtilizationProfile().getLength();
        vmStatesList = Collections.synchronizedMap(new HashMap<Integer, VirtualMachineState>());
        vmPriority = new HashMap<Integer, Integer>();
        vmAllocationCounts = new HashMap<Integer, Integer>();
        //Log.printLine(crDAO.getNumOfVms(cr.getId()));
        totalVm = crDAO.getNumOfVms(cr.getId());
    }

    /**
     * Processes the characteristics of datacenters assigned to this broker.
     *
     * @param ev a simulation event.
     * @since 1.0
     */
    @Override
    protected void processResourceCharacteristics(SimEvent ev) {
        DatacenterCharacteristics characteristics = (DatacenterCharacteristics) ev.getData();
        getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

        if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {
            setDatacenterRequestedIdsList(new ArrayList<Integer>());
            createVmsInDatacenter(getDatacenterIdList());
        }
    }

    /**
     * Processes the creation of virtual machines and their allocation in
     * datacenters.
     *
     * @param ev a simulation event.
     * @since 1.0
     */
    @Override
    protected void processVmCreate(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];
        //Log.printLine(ev.toString());
        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
            vmStatesList.put(vmId, VirtualMachineState.AVAILABLE);
            if (getVmsCreatedList().size() == totalVm) {
                CloudSimEvent e = new CloudSimEvent(CloudSimEvents.EVENT_ALL_VM_CREATED);
                fireCloudSimEvent(e);
            }
            Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId + " has been created in " + getDatacenterCharacteristicsList().get(datacenterId).getResourceName() + ", Host #" + VmList.getById(getVmsCreatedList(), vmId).getHost().getId() + " " + vmStatesList.get(vmId));
        } else {
            Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId + " failed in " + getDatacenterCharacteristicsList().get(datacenterId).getResourceName());
        }

        incrementVmsAcks();

        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) { // all the requested VMs have been created
            submitCloudlets();
        } else if (getVmsRequested() == getVmsAcks()) { // all the acks received, but some VMs were not created
            createVmsInDatacenter(getDatacenterIdList());

            /*if (getVmsCreatedList().size() > 0) { //if some vm were created
                submitCloudlets();
            }*/
        }
    }

    /**
     * Processes the return of cloudlets.
     *
     * @param ev a simulation event.
     * @since 1.0
     */
    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        getCloudletReceivedList().add(cloudlet);
        //Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId() + " received");
        cloudletsSubmitted -= 1;
        CloudSimEvent e = new CloudSimEvent(CloudSimEvents.EVENT_VM_FINISHED_CLOUDLET);
        e.addParameter("vmId", cloudlet.getVmId());
        fireCloudSimEvent(e);
        boolean setCloudletLength = cloudlet.setCloudletLength(maxLengthOfCloudlets);
        if (setCloudletLength == true) {
            Cloudlet newCloudlet = new Cloudlet(this.cloudletId,
                    (long) ((long) this.maxLengthOfCloudlets * 1.0 /*RandomNumberGenerator.getRandomNumbers(1).get(0)*/),
                    cloudlet.getNumberOfPes(),
                    cloudlet.getCloudletLength(),
                    cloudlet.getCloudletOutputSize(),
                    cloudlet.getUtilizationModelCpu(),
                    cloudlet.getUtilizationModelRam(),
                    cloudlet.getUtilizationModelBw());
            /*
            Log.printLine( " CLoudlet attributes: Number of PE: "+cloudlet.getNumberOfPes() +  " Cloudlet length: " +cloudlet.getCloudletLength() + " output size: " +
                cloudlet.getCloudletOutputSize() + " util cpu: " +
                cloudlet.getUtilizationModelCpu() + " util ram: " +
                cloudlet.getUtilizationModelRam() +" util bw: " +
                cloudlet.getUtilizationModelBw());
            */ 
            newCloudlet.setUserId(getId());
            newCloudlet.setVmId(cloudlet.getVmId());
            getCloudletList().add(newCloudlet);
            this.cloudletId++;
        }
        submitCloudlets();
    }

    /**
     * Submits cloudlets to be executed in virtual machines.
     *
     * @since 1.0
     */
    @Override
    protected void submitCloudlets() {

        for (Cloudlet cloudlet : getCloudletList()) {

            if (roundRobinDataCenter == 1) {
                if (cloudlet.getVmId() == -1) { //If user didn't bind this cloudlet and it has not been executed yet
                    cloudlet.setVmId(getVmsCreatedList().get(0).getId());
                }
            } //if (cloudlet.getVmId() == -1) { //If user didn't bind this cloudlet and it has not been executed yet
            else {
                cloudlet.setVmId(getNextAvailableVm());
            }
            //}

            //Check if the cloudlet VM has been allocated
            Vm cloudletVm = null;
            for (Vm vm : getVmsCreatedList()) {
                if (vm.getId() == cloudlet.getVmId()) {
                    cloudletVm = vm;
                }
            }

            //If the VM is allocated, send cloudlet
            if (cloudletVm != null) {
                //Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet " + cloudlet.getCloudletId() + " to VM #" + cloudletVm.getId());
                cloudlet.setVmId(cloudletVm.getId());

                try {
                    sendNow(getVmsToDatacentersMap().get(cloudletVm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                } catch (NullPointerException e) {
                    continue;
                }

                cloudletsSubmitted += 1;
                //Notify load balancer
                CloudSimEvent e = new CloudSimEvent(CloudSimEvents.EVENT_CLOUDLET_ALLOCATED_TO_VM);
                e.addParameter("vmId", cloudletVm.getId());
                fireCloudSimEvent(e);

                getCloudletSubmittedList().add(cloudlet);
            } else { //The VM is not allocated yet, so postpone submission
                //Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet " + cloudlet.getCloudletId() + ": bount VM not available");
            }
        }

        // remove submitted cloudlets from waiting list
        for (Cloudlet cloudlet : getCloudletSubmittedList()) {

            getCloudletList().remove(cloudlet);
        }
    }

    /**
     * Creates virtual machines in the datacenters managed by this broker.
     *
     * @param datacenterIdList the list of datacenters managed by this broker.
     * @since 1.0
     */
    protected void createVmsInDatacenter(List<Integer> datacenterIdList) {

        List<Vm> auxList = new ArrayList<Vm>();
        for (Vm vm : getVmList()) {
            auxList.add(vm);
        }

        int requestedVms = 0;
        for (Integer datacenterId : datacenterIdList) {
            String datacenterName = CloudSim.getEntityName(datacenterId);
            for (Vm vm : auxList) {
                if (!getVmsToDatacentersMap().containsKey(vm.getId())) {
                    Log.printLine(CloudSim.clock() + ": " + getName() + ": Trying to Create VM #" + vm.getId() + " in " + datacenterName);
                    sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
                    requestedVms++;
                    auxList.remove(vm);
                    break;
                }
            }
            getDatacenterRequestedIdsList().add(datacenterId);
            setVmsRequested(requestedVms);
        }
        setVmsAcks(0);
    }

    /* Cloudsim Observable interface methods */
    public void addCloudSimEventListener(CloudSimEventListener l) {
        listeners.add(l);
    }

    public void fireCloudSimEvent(CloudSimEvent e) {
        for (CloudSimEventListener l : listeners) {
            l.cloudSimEventFired(e);
        }
    }

    public void removeCloudSimEventListener(CloudSimEventListener l) {
        listeners.remove(l);
    }

    /**
     * Used internally to update VM allocation statistics. Should be called by
     * all impelementing classes to notify when a new VM is allocated.
     *
     * @param currVm
     */
    protected void allocatedVm(int currVm) {

        Integer currCount = vmAllocationCounts.get(currVm);
        if (currCount == null) {
            currCount = 0;
        }
        vmAllocationCounts.put(currVm, currCount + 1);
    }

    /**
     * Returns a {@link Map} indexed by VM id and having the number of
     * allocations for each VM.
     *
     * @return
     */
    public Map<Integer, Integer> getVmAllocationCounts() {
        return vmAllocationCounts;
    }

    public int gcd(int a, int b) {
        while (b > 0) {
            int temp = b;
            b = a % b;
            a = temp;
        }
        return a;
    }

    public double gcd(double a, double b) {
        while (b > 0) {
            double temp = b;
            b = a % b;
            a = temp;
        }
        return a;
    }

    public int gcdRam() {

        List<Vm> auxList = new ArrayList<Vm>();
        for (Vm vm : getVmList()) {
            auxList.add(vm);
        }
        int gcdRam = auxList.get(0).getRam();
        for (int i = 0; i < auxList.size(); i++) {
            gcdRam = gcd(gcdRam, auxList.get(i).getRam());
        }

        Log.printLine("Gcd Ram " + gcdRam);
        return gcdRam;
    }

    public int gcdNumberOfPes() {

        List<Vm> auxList = new ArrayList<Vm>();
        for (Vm vm : getVmList()) {
            auxList.add(vm);
        }
        int gcdNumberOfPes = auxList.get(0).getNumberOfPes();
        for (int i = 0; i < auxList.size(); i++) {
            gcdNumberOfPes = gcd(gcdNumberOfPes, auxList.get(i).getNumberOfPes());
        }

        Log.printLine("Gcd NumberOfPes " + gcdNumberOfPes);
        return gcdNumberOfPes;
    }

    public double gcdMips() {

        List<Vm> auxList = new ArrayList<Vm>();
        for (Vm vm : getVmList()) {
            auxList.add(vm);
        }
        double gcdMips = auxList.get(0).getMips();
        for (int i = 0; i < auxList.size(); i++) {
            gcdMips = gcd(gcdMips, auxList.get(i).getMips());
        }

        Log.printLine("Gcd MIPS " + gcdMips);
        return gcdMips;
    }

    public void calculatePriority() {
        gcdRam = gcdRam();
        gcdMips = gcdMips();
        gcdNumberOfPes = gcdNumberOfPes();
        for (Vm vm : getVmsCreatedList()) {
            int priority = (int) (vm.getRam() / gcdRam + vm.getNumberOfPes() / gcdNumberOfPes + vm.getMips() / gcdMips);
            vmPriority.put(vm.getId(), priority);
            //Log.printLine("Priority : " + vm.getId() + " " + priority);
        }
    }

    /**
     * * sort HashMap by value * @param map
     *
     * @param map
     */
    public LinkedHashMap<Integer, Integer> sortHashMapByValue(final HashMap<Integer, Integer> map) {
        ArrayList<Integer> keys = new ArrayList<Integer>();
        keys.addAll(map.keySet());
        Collections.sort(keys, new Comparator<Integer>() {
            @Override
            public int compare(Integer lhs, Integer rhs) {
                Integer val1 = map.get(lhs);
                Integer val2 = map.get(rhs);
                if (val1 == null) {
                    return (val2 != null) ? 1 : 0;
                } else if ((val1 != null)
                        && (val2 != null)) {
                    return val2.compareTo(val1);
                } else {
                    return 0;
                }
            }
        });
        LinkedHashMap<Integer, Integer> sortedMap;
        sortedMap = new LinkedHashMap<Integer, Integer>();
        for (Integer key : keys) {
            Integer c = map.get(key);
            if (c != null) {
                //Log.printLine("key:" + key + ", CustomData:" + c.toString());
                sortedMap.put(key, c);
            }
        }
        //Log.printLine(sortedMap.toString() + " sortedmap");
        return sortedMap;
    }

}
