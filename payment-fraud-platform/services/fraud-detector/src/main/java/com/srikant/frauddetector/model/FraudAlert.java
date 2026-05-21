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
public class FraudAlert {

    private String transactionId;

    private String cardId;

    private Double amount;

    private String reason;

    private Instant detectedAt;
}