package com.ddori.sample.batch.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@ToString
@Getter
@Setter
@NoArgsConstructor
@Entity
public class Pay {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

    @Id
    @GeneratedValue(strategy =
            GenerationType.IDENTITY)
    private Long id;
    private Long amount;
    private String txName;
    private LocalDateTime txDateTime;
    private boolean successStatus;

    public Pay(Long amount, String txName, String txDateTime) {
        this.amount = amount;
        this.txName = txName;
        this.txDateTime = LocalDateTime.parse(txDateTime, FORMATTER);
    }

    public Pay(Long id, Long amount, String txName, String txDateTime) {
        this.id = id;
        this.amount = amount;
        this.txName = txName;
        this.txDateTime = LocalDateTime.parse(txDateTime, FORMATTER);
    }

    public Pay(Long amount, boolean successStatus) {
        this.amount = amount;
        this.successStatus = successStatus;
    }

    public void success() {
        this.successStatus = true;
    }
}
