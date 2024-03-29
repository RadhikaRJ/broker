
package com.package1.controller;

import com.package1.model.Broker;
import com.package1.model.EventData;
import com.package1.model.SubscriberModel;
import com.package1.model.PublisherModel;
import com.package1.service.BrokerService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.scheduling.annotation.Scheduled;

import com.package1.controller.BrokerNodeController;

import org.codehaus.groovy.runtime.dgmimpl.arrays.IntegerArrayPutAtMetaMethod;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.util.EC2MetadataUtils;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@RestController
@CrossOrigin(origins = "*")
public class BrokerNodeController {

    private ConcurrentHashMap<String, String> publisherStatusMap = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, List<SubscriberModel>> publisherSubscriberMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, EventData> publisherEventDataMap = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    private Logger logger = Logger.getLogger(BrokerNodeController.class.getName());

    @Value("${configIp}")
    private String configIp;

    @Value("${ec2Port}")
    private String ec2Port;

    @Autowired
    private BrokerService brokerService;

    // copy A end here

    @Value("${server.port}")
    private int port; // Inject server port

    @Value("${config.server.url}")
    private String configServerUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    private Broker broker;

    private String currentLeaderBrokerPrivateIP;
    // Flag to track whether leader IP check is needed
    private boolean leaderIPCheckNeeded = true;

    @PostConstruct
    public void triggerRegistration() {

        // Create a sample Broker object with necessary information
        this.broker = new Broker();

        // Trigger registration with the configuration server
        System.out.println("Registering with coordinator server...");
        registerWithConfigServer(this.broker);

        currentLeaderBrokerPrivateIP = getLeaderPrivateIPFromConfigServer();

        updateLeaderStatus();

    }

    public Broker getBroker() {
        return this.broker;
    }

    // Update leader status based on current private IP
    private void updateLeaderStatus() {
        try {
            if (currentLeaderBrokerPrivateIP != null && broker != null && broker.getIpAddress() != null
                    && broker.getIpAddress().equals(currentLeaderBrokerPrivateIP)) {
                broker.setLeader(true);
                System.out.println("I am lead broker node");
            } else {
                if (broker != null) {
                    broker.setLeader(false);
                }

                System.out.println("I am just a peer node in the broker cluster.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostMapping("/register")
    public void registerWithConfigServer(@RequestBody Broker broker) {

        try {
            System.out.println("Registering broker node with Coordinator Server...");
            logger.log(Level.INFO, "Registering broker node with Coordinator Server...");

            String privateIpAddress = EC2MetadataUtils.getInstanceInfo().getPrivateIp();

            broker.setIpAddress(privateIpAddress);
            broker.setPort(port); // Set the injected port 8080
            broker.setUniqueId(60);

            // Retrieve EC2 instance ID dynamically using AWS EC2 Metadata Service
            String ec2InstanceId = EC2MetadataUtils.getInstanceId();
            broker.setEC2instanceID(ec2InstanceId);

            this.broker = broker;

            restTemplate.postForObject(configServerUrl + "/register-broker", broker, Void.class);
            System.out.println(
                    "Broker node with uniqueID: " + broker.getUniqueId() + "has sent registeration request to server");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/leadBroker-status")
    public ResponseEntity<String> checkHealth() {
        return ResponseEntity.ok("Alive");
    }

    @GetMapping("/helloBroker")
    public String hello() {
        if (broker != null) {
            return "Hello, World! BrokerServer is up & running! uniqueID: " + broker.getUniqueId();
        } else {
            return "Hello, World! BrokerServer is up & running!";
        }

    }

    @DeleteMapping("/deregister/{uniqueId}")
    public void deregisterFromConfigServer(@PathVariable Integer uniqueId) {
        System.out.println("Sending request to Coordinator Server to deregister broker with uniqueID: " + uniqueId
                + " from Coordinator Server...");
        restTemplate.delete(configServerUrl + "/deregister-broker/" + uniqueId);
    }

    // Method to handle POST request from config server to update leader's private
    // IP

    @PostMapping("/updateLeaderIPAndCheckStatus")
    public void handleUpdateLeaderIPAndCheckStatus(@RequestBody String requestBody) {
        JSONObject jsonObject = new JSONObject(requestBody);
        if (jsonObject != null) {
            String newLeadBrokerPrivateIPAddress = jsonObject.getString("newLeadBrokerPrivateIPAddress");

            this.currentLeaderBrokerPrivateIP = newLeadBrokerPrivateIPAddress;
            this.leaderIPCheckNeeded = true;
            updateLeaderStatus();
        }

    }

    // Method to retrieve the private IP of the leader broker from config server
    private String getLeaderPrivateIPFromConfigServer() {
        // Make a GET request to the config server endpoint

        try {
            System.out.println("Requesting lead broker's private IP address at Coordinator server");
            String currleadBrokerPrivateIPAtConfigServer = restTemplate.getForObject(
                    configServerUrl + "/getCurrent-leadBroker-PrivateIP",
                    String.class);

            // Return the fetched private IP of the lead broker
            return currleadBrokerPrivateIPAtConfigServer;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Scheduled(fixedRate = 70000) // 70 seconds
    private void pingLeaderBroker() {
        // if current node is leader, then it will not perform the ping operation.
        if (!broker.isLeader()) {
            // this node is not lead broker, so it will perform ping as scheduled to the
            // lead broker
            // initially, leaderIPCheckNeeded is set to true as we need to check the status
            // periodically
            // also, the currentLeaderBrokerPrivateIP is not set to null ( it will be set to
            // null in the duration when leader election is occurring at the config server
            // and until new leader's IP is not updated)
            if (leaderIPCheckNeeded && currentLeaderBrokerPrivateIP != null) {
                // we ping and check the lead broker
                System.out.println("Pinging the lead broker IP...");
                boolean isLeaderResponsive = pingLeader(currentLeaderBrokerPrivateIP);
                // if isLeaderResponsive is true, then next health check at same leadBroker's IP
                // Address will happen as per scheduled execution
                // if we don't get a response from leaderIP address, execute the next block of
                // code
                if (!isLeaderResponsive) {
                    // we inform the configServer about the leaderIP being non-responsive and send
                    // it the currentLeaderBrokerPrivateIP
                    System.out.println("Leader broker has not responded to ping");
                    informLeaderNotResponding();
                    String currLeadBrokerPrivateIPAtServer = getLeaderPrivateIPFromConfigServer();
                    System.out
                            .println("Requesting leader's privateIP at coordinating server. Server has returned value: "
                                    + currLeadBrokerPrivateIPAtServer);
                    // the configServer will have some currentBrokerLeaderIP value:
                    // 1. null if its carrying out leader election
                    // 2. another IP address if new leader has been elected
                    // 3. same IP address as that of currentLeaderBrokerPrivateIP.
                    // 3.a lead broker is unhealthy, but it is back up by the time node reports and
                    // config verifies.
                    // So configServer will returns same IP that peer reporting node has.
                    // 3.b Handles the scemnario where due to some communication failure, peer node
                    // reports leader broker to be down. But in reality, it never failed. So same IP
                    // is returned by configServer

                    // if IP returned is null, update the currentLeaderBrokerPrivateIP to null and
                    // do nothing.
                    // this ensures that no scheduled ping action happens in next iteration
                    // configServer will send an update to all peer nodes about new IP address of
                    // the leader broker node.
                    // untill then, the pinging and reporting lead broker status to configServer
                    // will be paused.

                    if (currLeadBrokerPrivateIPAtServer == null) {
                        currentLeaderBrokerPrivateIP = null;
                        // don't check untill we have a new elected value for the
                        // currentLeaderBrokerPrivateIP
                        System.out.println("leaderIPCheckNeeded is set to false.");
                        leaderIPCheckNeeded = false;
                        // this value will be set to true by configServer when it updates the
                        // currentLeaderBrokerPrivateIP.
                    }

                    if (currLeadBrokerPrivateIPAtServer != null) {
                        // configServer may also in the mean time set the current peer to lead.
                        // But if it hasn't yet completed that action, the broker node can check the
                        // returned IP and accordingly update isLead()
                        // so then the scheduled ping does not happen hereafter for the newly elected
                        // broker node
                        if (currLeadBrokerPrivateIPAtServer.equals(broker.getIpAddress())) {
                            broker.setLeader(true);
                            System.out.println("I am the leader broker node");

                        }
                        // Lead broker IP sent by server is not current node's private IP
                        // it does not match the currentLeaderBrokerPrivateIP that this current node has
                        // This means a new leader node has been elected.
                        // Its possible that before configServer has communicated to currentNode of
                        // newly elected leadBroker's IPAddress,
                        // the current node has retrieved it. So the current node updates
                        // currentLeaderBrokerPrivateIP itself.
                        // next scheduled ping will occur at the updated lead broker's private IP
                        else if (!currLeadBrokerPrivateIPAtServer.equals(currentLeaderBrokerPrivateIP)) {
                            // leaderIPCheckNeeded is still true and so next pings will execute
                            System.out.println(
                                    "Lead Broker's Private IP at server was different. Updating lead broker's private IP at local node...");
                            currentLeaderBrokerPrivateIP = currLeadBrokerPrivateIPAtServer;
                        }
                    }
                }
            }

        }
    }

    /*
     * Method to ping the leader broker
     * Perform ping operation to check the status of the leader broker using its
     * private IP Sending a GET request to a to a specific endpoint for health check
     * on the leader broker and check for a successful response
     */

    private boolean pingLeader(String leaderPrivateIp) {

        boolean isLeaderResponsive = false;
        try {

            String healthCheckUrl = "http://" + leaderPrivateIp + ":8080/leadBroker-status";
            String statusOfLeadBroker = restTemplate.getForObject(healthCheckUrl, String.class);
            System.out.println("Response from leader broker: " + statusOfLeadBroker);
            isLeaderResponsive = true;
        } catch (Exception e) {
            // Exception occurred, leader is not responsive
            System.err.println("Error occurred while pinging leader broker: " + e.getMessage());
            isLeaderResponsive = false;
        }
        return isLeaderResponsive;
    }

    // inform configServer that lead node is not responding
    // and send the currentLeaderBrokerPrivateIP stored at peer broker node
    private void informLeaderNotResponding() {
        try {
            System.out.println("Informing Coordinator Server that lead broker did not respond...");
            String requestBody = "{\"currleadBrokerIPAtNode\": \"" + currentLeaderBrokerPrivateIP + "\"}";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(requestBody, headers);
            restTemplate.postForObject(configServerUrl + "/leader-not-responding", entity, Void.class);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void triggerDeregistration() {
        System.out.println("Deregistering from Coordinator Server...");
        // try {
        // if (broker != null) {
        // deregisterFromConfigServer(broker.getUniqueId());
        // }
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        // Deregister from the config server before the instance terminates
    }

    @Scheduled(fixedRate = 60000) // 60 seconds = 1 minute
    public void checkAndNotifySubscribers() {

        boolean isLeader = broker.isLeader();

        if (isLeader) {

            Iterator<Map.Entry<String, EventData>> iterator = publisherEventDataMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, EventData> entry = iterator.next();
                String publisherId = entry.getKey();
                EventData event = entry.getValue();

                if (event != null) {
                    long startTime = System.currentTimeMillis();
                    List<SubscriberModel> subscribers = publisherSubscriberMap.getOrDefault(publisherId,
                            new ArrayList<>());
                    if (!subscribers.isEmpty()) {
                        boolean success = notifySubscriber(subscribers, event);
                        lock.lock();
                        try {
                            if (success) {
                                // Remove the event after successfully notifying subscribers
                                long endTime = System.currentTimeMillis();
                                long timeTaken = endTime - startTime;
                                System.out.println(
                                        "Time taken to send the event to the subscribers: " + timeTaken + " ms.");
                                iterator.remove();
                                System.out.println("Notified subscribers for event from publisher: " + publisherId);
                                boolean result = sendEventDataToPeerBrokers(publisherEventDataMap); // Consistency &
                                                                                                    // Replication
                                if (result) {
                                    System.out.println("Successfully sent publisherEventDataMap to peer brokers");
                                } else {
                                    System.out.println("Failed to notify peer brokers about publisherEventDataMap");
                                }
                            } else {
                                System.out.println(
                                        "Failed to notify subscribers for event from publisher: " + publisherId);
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            lock.unlock();
                        }
                    }
                } else {
                    System.out.println("No Events to be posted: ");
                }
            }

        }

    }

    @GetMapping(value = "/getPublishers")
    public ResponseEntity<List<String>> getPublishers() {
        System.out.println("in getPublishers method");
        logger.log(Level.INFO, "in getPublishers method");

        List<String> activePublishers = new ArrayList<>();

        for (Map.Entry<String, String> entry : this.publisherStatusMap.entrySet()) {
            String publisherId = entry.getKey();
            String status = entry.getValue();

            if ("active".equals(status)) {
                activePublishers.add(publisherId);
            }
        }

        System.out.print("\n sending active publishers to subscriber:::::::" + activePublishers);
        logger.log(Level.INFO, "\\n" + //
                " sending active publishers to subscriber:::::::\" + activePublisher");

        return ResponseEntity.ok(activePublishers);
    }

    @PostMapping(value = "/subscribe")
    public ResponseEntity<HttpStatus> subscribe(@RequestBody SubscriberModel subscriberModel) throws Exception {

        int subscriberId = subscriberModel.getSubscriberId();
        List<String> selectedPublishers = subscriberModel.getPublishers();

        lock.lock();
        try {
            for (String publisherId : selectedPublishers) {
                if (!publisherSubscriberMap.containsKey(publisherId)) {
                    publisherSubscriberMap.put(publisherId, new ArrayList<>());
                }
                List<SubscriberModel> subscribers = publisherSubscriberMap.get(publisherId);
                subscribers.add(subscriberModel);
            }

            boolean success = sendMapToPeerBrokers(publisherSubscriberMap); // consistency & Replication

            if (success) {
                System.out.println(
                        "\n  Subscriber with ID " + subscriberId + " subscribed to publishers: " + selectedPublishers);
                logger.log(Level.INFO,
                        "\n  Subscriber with ID " + subscriberId + " subscribed to publishers: " + selectedPublishers);
                System.out.flush();
                return ResponseEntity.ok(HttpStatus.OK);
            } else {
                System.out
                        .println("\n  Subscriber with ID " + subscriberId + " could not be subscribed to publishers ");
                System.out.flush();
                logger.log(Level.INFO, "\n" + //
                        "  Subscriber with ID \" + subscriberId + \" could not be subscribed to publishers \"");
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }

        } catch (Exception e) {
            logger.log(Level.INFO,
                    "throwing exception e in /subscribe");
            throw e;

        } finally {
            lock.unlock();
        }

    }

    public boolean notifySubscriber(List<SubscriberModel> subscribers, EventData event) {

        boolean flag = brokerService.notify(subscribers, event);

        return flag;

    }

    private boolean sendMapToPeerBrokers(ConcurrentHashMap<String, List<SubscriberModel>> publisherSubscriberMap)
            throws Exception {

        System.out.println("Entered the method :sendMapToPeerBrokers ");
        logger.log(Level.INFO, "Entered the method :sendMapToPeerBrokers ");

        boolean flag = false;
        List<String> peerBrokersIpList = getPeerIpsFromConfigServer();

        if (publisherSubscriberMap != null) {
            if (peerBrokersIpList.size() != 0) {

                for (String peerBrokerIP : peerBrokersIpList) {
                    String peerurl = "http://" + peerBrokerIP + ":" + Integer.parseInt(ec2Port)
                            + "/receiveHashMapUpdate";
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    // modified below to handle ConcurrentHashMap- Radhika
                    HttpEntity<ConcurrentHashMap> requestEntity = new HttpEntity<>(publisherSubscriberMap, headers);
                    System.out.println("Sending publisherSubscriberMap to peerBroker at url " + peerurl);
                    logger.log(Level.INFO, "Sending publisherSubscriberMap to peerBroker at url \" + peerurl");

                    HttpStatusCode status;
                    try {
                        ResponseEntity<HttpStatus> responseEntity = restTemplate.exchange(peerurl,
                                HttpMethod.POST, requestEntity, HttpStatus.class);

                        status = responseEntity.getStatusCode(); // ok
                        logger.log(Level.INFO, "status is : " + status);

                        System.out.println(
                                "status received after hitting /receiveHashMapUpdate from sendMapToPeerBrokers. ");

                    } catch (Exception e) {
                        System.err.println(
                                "Exception while sending publisherSubscriberMap to Peer Broker at " + peerurl + ": "
                                        + e.getMessage());

                        throw e;
                    }

                    if (!status.equals(HttpStatus.OK)) {

                        flag = false;
                        System.out.println("Flag in function sendMapToPeerBrokers is set to " + flag);
                        System.out.flush();
                        break;
                    } else {

                        flag = true;
                        System.out.println("Flag in function sendMapToPeerBrokers is set to " + flag);
                        System.out.flush();
                    }
                }
            } else {// there are no peer IPs. Should only update the leader broker. so set flag as
                    // true!

                flag = true;
                System.out.println("Flag in function sendMapToPeerBrokers is set to " + flag);
            }
        } else {
            System.out.println("publisherSubscriberMap is null. Printing from sendMapToPeerBrokers");
            System.out.flush();
        }

        return flag;
    }

    private boolean sendEventDataToPeerBrokers(ConcurrentHashMap<String, EventData> publisherEventDataMap)
            throws Exception {

        boolean flag = false;
        List<String> peerBrokersIpList = getPeerIpsFromConfigServer();
        // verifying if peerBrokerIPList is not empty --Radhika

        if (peerBrokersIpList.size() != 0) {
            for (String peerBrokerIP : peerBrokersIpList) {
                String peerurl = "http://" + peerBrokerIP + ":" + Integer.parseInt(ec2Port) + "/receiveEventData";
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                HttpEntity<ConcurrentHashMap> requestEntity = new HttpEntity<>(publisherEventDataMap, headers);
                System.out.println("Sending publisherEventDataMap to peerBroker at url " + peerurl);
                System.out.flush();
                HttpStatusCode status;
                try {
                    ResponseEntity<HttpStatus> responseEntity = restTemplate.exchange(peerurl,
                            HttpMethod.POST, requestEntity, HttpStatus.class);

                    status = responseEntity.getStatusCode();
                } catch (Exception e) {
                    System.err
                            .println("Exception while sending publisherEventDataMap to Peer Broker at " + peerurl + ": "
                                    + e.getMessage());
                    System.out.flush();
                    throw e;
                }

                if (!status.equals(HttpStatus.OK)) {
                    flag = false;
                    break;
                } else {
                    flag = true;
                }
            }
        } else {
            System.out.println("peerbrokeripList is null");

        }

        return flag;
    }

    public List<String> getPeerIpsFromConfigServer() {
        List<String> peerBrokersIpList = new ArrayList<>();
        String configUrl = "http://" + configIp + ":" + ec2Port;
        try {

            String appendedUrl = configUrl + "/get-peerBrokers-IPList";
            System.out.println("Appended URL in : " + appendedUrl);
            System.out.flush();
            ParameterizedTypeReference<List<String>> responseType = new ParameterizedTypeReference<List<String>>() {
            };

            ResponseEntity<List<String>> responseEntity = restTemplate.exchange(
                    appendedUrl, HttpMethod.GET, null, responseType);

            if (responseEntity.getStatusCode().is2xxSuccessful()) {

                peerBrokersIpList = responseEntity.getBody();
                for (String peerBrokerIP : peerBrokersIpList) {
                    System.out.println("Peer Broker IP: " + peerBrokerIP);

                }
            } else {
                System.err.println(
                        "Failed to fetch peer broker IP list. Status code: " + responseEntity.getStatusCode());
                System.out.flush();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return peerBrokersIpList;
    }

    @PostMapping("/receiveEventData")
    public ResponseEntity<String> receiveEventData(@RequestBody ConcurrentHashMap<String, EventData> receivedMap) {

        try {

            this.publisherEventDataMap.clear();
            this.publisherEventDataMap.putAll(receivedMap);

            System.out.print("Received publisherEventDataMap from lead broker. Updated successfully. here is the " +
                    "list I received from lead Broker");
            System.out.flush();
            for (String publisherId : publisherEventDataMap.keySet()) {
                EventData event = publisherEventDataMap.get(publisherId);

                System.out.println("\n");
                System.out.println("Publisher ID: " + publisherId);
                System.out.println("EventId: " + event.getEventId());

                System.out.println("\n");
            }

            return ResponseEntity.ok("");

        } catch (Exception e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error updating publisherEventDataMap: " + e.getMessage());
        }
    }

    @PostMapping("/receiveHashMapUpdate")
    public ResponseEntity<String> receiveHashMapUpdate(
            @RequestBody ConcurrentHashMap<String, List<SubscriberModel>> receivedMap) {

        try {

            this.publisherSubscriberMap.clear();
            this.publisherSubscriberMap.putAll(receivedMap);

            System.out.print("Received publisherSubscriberMap from lead broker. Updated successfully. here is the " +
                    "list I received from lead Broker");
            System.out.flush();
            for (String publisherId : publisherSubscriberMap.keySet()) {
                List<SubscriberModel> subscribers = publisherSubscriberMap.get(publisherId);
                System.out.println("/n");
                System.out.println("Publisher ID: " + publisherId);
                System.out.println("Subscribers: " + subscribers);
                for (SubscriberModel subModel : subscribers) {
                    logger.log(Level.INFO, "Subscriber ID : " + subModel.getSubscriberId());
                }
                System.out.println("/n");
            }

            return ResponseEntity.ok("");

        } catch (Exception e) {

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error updating publisherSubscriberMap: " + e.getMessage());
        }
    }

    @PostMapping(value = "/unsubscribe")
    public ResponseEntity<HttpStatus> unsubscribe(@RequestBody SubscriberModel subscriberModel) {

        int subscriberId = subscriberModel.getSubscriberId();
        List<String> selectedPublishers = subscriberModel.getPublishers();

        lock.lock();
        try {

            for (String publisherId : selectedPublishers) {
                if (publisherSubscriberMap.containsKey(publisherId)) {
                    List<SubscriberModel> subscribers = publisherSubscriberMap.get(publisherId);
                    // Remove the subscriber with the specified ID
                    subscribers.removeIf(subscriber -> subscriber.getSubscriberId() == subscriberId);
                }
            }

            boolean success = sendMapToPeerBrokers(publisherSubscriberMap); // Consistency & Replication

            if (success) {
                System.out.println(
                        "Subscriber with ID " + subscriberId + " unsubscribed from publishers: " + selectedPublishers);
                System.out.flush();
                return ResponseEntity.ok(HttpStatus.OK);
            } else {
                System.out.println("Subscriber with ID " + subscriberId + " could not be unsubscribed from publishers");
                System.out.flush();
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            lock.unlock();
        }

    }

    private boolean sendStatusMapToPeerBrokers(ConcurrentHashMap<String, String> publisherStatusMap) throws Exception {
        System.out.println("entering the sendStatusMapToPeerBrokers function");
        boolean flag = false;
        List<String> peerBrokersIpList = getPeerIpsFromConfigServer();
        if (publisherStatusMap != null) {
            if (peerBrokersIpList.size() != 0) {
                for (String peerBrokerIP : peerBrokersIpList) {
                    String peerurl = "http://" + peerBrokerIP + ":" + Integer.parseInt(ec2Port)
                            + "/receiveStatusMapUpdate";
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);

                    HttpEntity<ConcurrentHashMap> requestEntity = new HttpEntity<>(publisherStatusMap, headers);
                    System.out.println("Sending publisherStatusMap to peerBroker at url " + peerurl);
                    System.out.flush();
                    HttpStatusCode status;
                    try {
                        ResponseEntity<String> responseEntity = restTemplate.exchange(peerurl,
                                HttpMethod.POST, requestEntity, String.class);

                        status = responseEntity.getStatusCode();
                    } catch (Exception e) {
                        System.err.println(
                                "Exception while sending publisherStatusMap to Peer Broker at " + peerurl + ": "
                                        + e.getMessage());
                        System.out.flush();
                        throw e;
                    }

                    if (!status.equals(HttpStatus.OK)) {
                        flag = false;
                        break;
                    } else {
                        flag = true;
                    }
                }
            } else {
                // there are no peer broker IPs and the flag should be set to true
                flag = true;
            }
        } else {
            System.out.println("publisherStatusMap is null.");
            System.out.flush();
        }

        return flag;
    }

    @PostMapping("/receiveStatusMapUpdate")
    public ResponseEntity<String> receiveStatusMapUpdate(@RequestBody ConcurrentHashMap<String, String> receivedMap) {

        System.out.println("Enetered into the executiion block of /receiveStatusMapUpdate");
        System.out.flush();
        try {

            if (receivedMap == null) {
                System.out.println("received map is null in receiveStatusMapUpdate function.");
                System.out.flush();
            }
            this.publisherStatusMap.clear();
            this.publisherStatusMap.putAll(receivedMap);
            logger.log(Level.INFO, "publisherStatusMap is updated at peer node" + publisherStatusMap);
            return ResponseEntity.ok("Received publisherStatusMap from lead broker. Updated successfully"); // Should be
                                                                                                            // a log
                                                                                                            // statement.

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error updating publisherStatusMap: " + e.getMessage());

        }
    }

    @PostMapping(value = "/changePublisherStatus")
    public ResponseEntity<HttpStatus> changePublisherStatus(@RequestBody PublisherModel publisherModel) {

        String publisherId = publisherModel.getPublisherId();
        String status = publisherModel.getStatus();
        // boolean success = false;

        // Validate publisherId and status --Radhika
        if (publisherId == null || status == null) {
            return ResponseEntity.badRequest().build();
        }

        lock.lock();
        try {
            if (publisherStatusMap.size() != 0 && !publisherStatusMap.containsKey(publisherId)) {
                // Publisher not found in the status map
                System.out.println("Publisher with ID " + publisherId + " not found");
                System.out.flush();
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }
            // Update status in the map
            publisherStatusMap.put(publisherId, status);

            boolean success = sendStatusMapToPeerBrokers(publisherStatusMap); // consistency & Replication
            if (success) {
                System.out.println("Publisher name: " + publisherId + "| Status changed to: " + status);
                System.out.flush();

                return ResponseEntity.ok().build();

            } else {
                System.out.println("Publisher name:" + publisherId + " some other error caused this failure.");
                System.out.flush();
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        } catch (Exception e) {

            System.out.println("An error occurred while processing the request: " + e.getMessage());
            System.out.flush();
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            lock.unlock();
        }
    }

    @PostMapping(value = "/addPublisher")
    public ResponseEntity<String> addPublisher(@RequestBody PublisherModel publisherModel) throws Exception {

        String publisherId = publisherModel.getPublisherId();
        logger.log(Level.INFO, "publisher ID value: " + publisherId);
        String status = publisherModel.getStatus();
        logger.log(Level.INFO, "publisher status: " + status);
        boolean successForSubscriberMap = false; // maybe declare inside the lock
        boolean successForStatusMap = false;// maybe declare inside the lock

        lock.lock();
        try {
            if (!publisherSubscriberMap.containsKey(publisherId)) {
                publisherSubscriberMap.put(publisherId, new ArrayList<>());
            }
            System.out.println("Sending the updated map to peer broker: ");
            System.out.flush();
            successForSubscriberMap = sendMapToPeerBrokers(publisherSubscriberMap); //
            // consistency & Replication

            publisherStatusMap.put(publisherId, status);

            logger.log(Level.INFO, "the publsiherStatusMap: " + publisherStatusMap);

            successForStatusMap = sendStatusMapToPeerBrokers(publisherStatusMap); //
            // consistency & Replication

            if (successForSubscriberMap && successForStatusMap) {
                System.out.println("Publisher: " + publisherId + "added to the broker system");
                logger.log(Level.INFO, String.format("Publisher: %s added to the broker system", publisherId));
                return ResponseEntity.status(HttpStatus.OK).body("Publisher added successfully");
            } else {
                System.out.println("Publisher: " + publisherId + "could not be added to the broker system");
                logger.log(Level.INFO,
                        String.format("Publisher: %s could not be added to the broker system", publisherId));
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("status error");
            }

        } catch (Exception e) {

            throw e;
        } finally {
            lock.unlock();
        }

    }

    @PostMapping(value = "/pushEvent")
    public ResponseEntity<HttpStatus> pushEvent(@RequestBody EventData eventData) {
        String publisherId = eventData.getPublisherId();
        int eventId = eventData.getEventId();
        lock.lock();
        try {
            publisherEventDataMap.put(publisherId, eventData);
            System.out.println("Event data with id: " + eventId + " received by the broker");

            return ResponseEntity.ok(HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            lock.unlock();
        }
    }
}
