package com.package1.serviceImpl;

import com.package1.model.EventData;
import com.package1.model.SubscriberModel;
import com.package1.service.BrokerService;
import org.springframework.stereotype.Service;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@Service
public class BrokerServiceImpl implements BrokerService {

    @Autowired
    RestTemplate restTemplate;

    @Override
    public boolean notify (List<SubscriberModel> subscribers, EventData event){

        boolean flag = false;

        for (SubscriberModel node : subscribers) {
            String url = "http://" + node.getUrl() + ":" + node.getPort() + "/notify";
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<EventData> requestEntity = new HttpEntity<>(event,headers);
            System.out.println("Sending eventId " + event.getEventId() + " to subscriber " +
                    node.getSubscriberId() + " at url " + url +" to port " +node.getPort());
            HttpStatus statusCode;
            try {
                ResponseEntity<HttpStatus> responseEntity = restTemplate.exchange(url,
                        HttpMethod.POST,requestEntity,HttpStatus.class);
                statusCode = responseEntity.getBody();
            } catch (Exception e) {
                System.out.println("Error while notifying to subscriber id : "+node.getSubscriberId()
                        + "::error is " + ":::" + e.getMessage());
                statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
            }

            if (!statusCode.equals(HttpStatus.OK)) {
                flag = false;
                break;
            }else{
                flag = true;
            }

        }

        return flag;
    }

}
