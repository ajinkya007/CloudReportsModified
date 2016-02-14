package cloudreports.event;

public interface CloudsimObservable {

	void addCloudSimEventListener(CloudSimEventListener l);
	
	void removeCloudSimEventListener(CloudSimEventListener l);
	
	void fireCloudSimEvent(CloudSimEvent e);
}
