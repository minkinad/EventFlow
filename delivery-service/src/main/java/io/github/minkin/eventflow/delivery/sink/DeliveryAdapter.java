package io.github.minkin.eventflow.delivery.sink;

import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.job.DeliveryJob;

public interface DeliveryAdapter {
  TargetType targetType();

  void deliver(DeliveryJob job);
}
