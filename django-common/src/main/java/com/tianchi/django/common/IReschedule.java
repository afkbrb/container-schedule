package com.tianchi.django.common;

import java.util.List;

import com.tianchi.django.common.pojo.*;

public interface IReschedule {

    List<RescheduleResult> reschedule(final List<NodeWithPod> nodeWithPods, final Rule rule) throws Exception;

}
