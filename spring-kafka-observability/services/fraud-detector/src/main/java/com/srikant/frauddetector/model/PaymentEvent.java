package com.srikant.frauddetector.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {

    private String transactionId;

    private String userId;

    private String cardId;

    private Double amount;

    private String merchant;

    private String country;

    private Instant timestamp;
}