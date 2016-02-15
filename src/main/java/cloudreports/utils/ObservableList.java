/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cloudreports.utils;

/**
 *
 * @author AjinkyaWavare
 */

import cloudreports.event.CloudSimEvent;
import cloudreports.event.CloudSimEventListener;
import cloudreports.event.CloudSimEvents;
import cloudreports.event.CloudsimObservable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link List} who's additions and removals can be monitored.
 * 
 * @param <DataType>
 */
public class ObservableList<DataType> extends ArrayList<DataType> 
									  implements CloudsimObservable, Serializable {

	private static final long serialVersionUID = -4005612299359693140L;
	
	//We don't want any copies to carry the same list of listeners. 
	transient private List<CloudSimEventListener> listeners;
	
	/** Constructor. */
	public ObservableList(){
		
	}
	
	public void addCloudSimEventListener(CloudSimEventListener l){
		if (listeners == null){
			listeners = new ArrayList<CloudSimEventListener>();
		}
		listeners.add(l);
	}
	
	public void removeCloudSimEventListener(CloudSimEventListener l){
		if (listeners != null){
			listeners.remove(l);
		}
	}
	
	public void fireCloudSimEvent(CloudSimEvent e){
		if (listeners != null){
			for (CloudSimEventListener l : listeners){		
				l.cloudSimEventFired(e);
			}
		}
	}
	
	@Override
	public boolean add(DataType o){
		boolean success = super.add(o);
		
		if (success){
			CloudSimEvent cloudSimEvent = new CloudSimEvent(CloudSimEvents.EVENT_LIST_CONTENT_ADDED);
			cloudSimEvent.addParameter("data_element", o);
			fireCloudSimEvent(cloudSimEvent);
		}
		
		return success;
	}
	
	@Override
	public boolean remove(Object o) {
		boolean success = super.remove(o);
		
		if (success){
			CloudSimEvent cloudSimEvent = new CloudSimEvent(CloudSimEvents.EVENT_LIST_CONTENT_REMOVED);
			cloudSimEvent.addParameter("data_element", o);
			fireCloudSimEvent(cloudSimEvent);
		}
		
		return success;
	}
	
	@Override
	public DataType remove(int index) {
		DataType removed = super.remove(index);
		
		if (removed != null){
			CloudSimEvent cloudSimEvent = new CloudSimEvent(CloudSimEvents.EVENT_LIST_CONTENT_REMOVED);
			cloudSimEvent.addParameter("data_element", removed);
			fireCloudSimEvent(cloudSimEvent);
		}
		
		return removed;
	}
	
	@Override
	public void clear() {
		super.clear();
		fireCloudSimEvent(new CloudSimEvent(CloudSimEvents.EVENT_LIST_CONTENT_CHANGED));
	}
	
	public boolean replaceContent(Collection<DataType> c){
		super.clear();
		boolean success = super.addAll(c);
		fireCloudSimEvent(new CloudSimEvent(CloudSimEvents.EVENT_LIST_CONTENT_CHANGED));
		
		return success;
	}
}
