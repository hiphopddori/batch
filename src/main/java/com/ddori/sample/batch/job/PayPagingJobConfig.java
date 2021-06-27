package com.ddori.sample.batch.job;
//https://github.com/jojoldu/spring-batch-in-action/blob/master/src/main/java/com/jojoldu/batch/example/reader/jdbc/JdbcPagingItemReaderJobConfiguration.java

import com.ddori.sample.batch.domain.Pay;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

import static com.ddori.sample.batch.job.PayPagingJobConfig.JOB_NAME;


@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class PayPagingJobConfig {
    public static final String JOB_NAME = "payPagingFailJob";
    private final EntityManagerFactory entityManagerFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    private final int chunkSize = 10;

    @Bean
    public Job payPagingJob() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(payPagingStep())
                .build();
    }

    @Bean
    @JobScope
    public Step payPagingStep() {
        return stepBuilderFactory.get("payPagingStep")
                .<Pay, Pay>chunk(chunkSize)
                .reader(payPagingReader())
                .processor(payPagingProcessor())
                .writer(writer())
                .build();
    }

    @Bean
    @StepScope
    public JpaPagingItemReader<Pay> payPagingReader() {
        /* paging로 chunkSize로 10건 Update로 인해서 이제 테이블에는 false인 데이터가 40개만 존재
        아래 쿼리시 offset 11 limit 10 로 인해 배치 update 누락 발생할수 있어 아래 코드로 보완 처리 함
        return new JpaPagingItemReaderBuilder<Pay>()
                .queryString("SELECT p FROM Pay p WHERE p.successStatus = false")
                .pageSize(chunkSize)
                .entityManagerFactory(entityManagerFactory)
                .name("payPagingReader")
                .build();
         */

        JpaPagingItemReader<Pay> reader = new JpaPagingItemReader<Pay>() {
            @Override
            public int getPage() {
                return 0;
            }
        };

        reader.setQueryString("SELECT p FROM Pay p WHERE p.successStatus = false");
        reader.setPageSize(chunkSize);
        reader.setEntityManagerFactory(entityManagerFactory);
        reader.setName("payPagingReader");
        return reader;
    }

    @Bean
    @StepScope
    public ItemProcessor<Pay, Pay> payPagingProcessor() {
        return item -> {
            item.success();
            return item;
        };
    }

    @Bean
    @StepScope
    public JpaItemWriter<Pay> writer() {
        JpaItemWriter<Pay> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(entityManagerFactory);
        return writer;
    }
}
