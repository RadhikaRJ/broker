package com.package1.service;

import com.package1.model.EventData;
import com.package1.model.SubscriberModel;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BrokerService {

    public boolean notify (List<SubscriberModel> subscribers, EventData event);
}
