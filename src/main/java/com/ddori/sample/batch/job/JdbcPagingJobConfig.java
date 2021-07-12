package com.ddori.sample.batch.job;

import com.ddori.sample.batch.domain.Pay;
import com.ddori.sample.batch.util.DailyJobTimestamper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ddori.sample.batch.job.JdbcPagingJobConfig.JOB_NAME;

/* 참조
https://github.com/jojoldu/spring-batch-in-action/blob/master/src/main/java/com/jojoldu/batch/example/reader/jdbc/JdbcPagingItemReaderJobConfiguration.java
https://github.com/springframework-storage/SpringBoot-Batch-Demo/blob/master/src/main/java/com/example/batch/writer/jdbc/JdbcBatchItemWriterJobConfiguration.java
 */

@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = JOB_NAME)
public class JdbcPagingJobConfig {

    public static final String JOB_NAME = "jdbcPagingJob";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource; // DataSource DI

    private static final int chunkSize = 10;

    @Bean
    public Job jdbcPagingItemReaderJob() throws Exception {
        return jobBuilderFactory.get(JOB_NAME)
                .start(jdbcPagingItemReaderStep())
                .incrementer(new DailyJobTimestamper())
                // .validator()
                // .listener()
                .build();
    }

    @Bean
    //@StepScope
    public Step jdbcPagingItemReaderStep() throws Exception {
        return stepBuilderFactory.get("jdbcPagingItemReaderStep")
                .<Pay, Pay>chunk(chunkSize)
                .reader(jdbcPagingItemReader())
                //.reader(testPayReader())
                .writer(jdbcBatchItemWriterUpdate())
                //.listener(154 참조)
                .build();
    }

    @Bean
    @StepScope
    public ListItemReader<Pay> testPayReader() {
        log.info("********** This is testPayReader");
        List<Pay> pays = new ArrayList<Pay>();

        return new ListItemReader<>(pays);
    }


    @Bean
    public JdbcPagingItemReader<Pay> jdbcPagingItemReader() throws Exception {
        Map<String, Object> parameterValues = new HashMap<>();
        parameterValues.put("amount", 2000);

        return new JdbcPagingItemReaderBuilder<Pay>()
                .pageSize(chunkSize)
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Pay.class))
                .queryProvider(createQueryProvider())
                .parameterValues(parameterValues)
                .name("jdbcPagingItemReader")
                .build();
    }

    @Bean
    public JdbcBatchItemWriter jdbcBatchItemWriter() {
        return new JdbcBatchItemWriterBuilder<Pay>()
                .beanMapped()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Pay>())
                .sql("INSERT INTO pay2(amount, tx_name, tx_date_time) VALUES (:amount, :txName, :txDateTime)")
                .build();
    }

    @Bean
    public JdbcBatchItemWriter jdbcBatchItemWriterUpdate() {
        return new JdbcBatchItemWriterBuilder<Pay>()
                .beanMapped()

                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Pay>())
                .sql("UPDATE PAY SET" +
                        " success_status = false" +
                        " WHERE id = :id"
                 )
                //.assertUpdates(true)
                /*.sql("UPDATE pay SET" +
                        "amount = COALESCE(:amount, amount)" +
                        "WHERE id = :id"
                 )*/
                .dataSource(dataSource)
                .build();
    }

    private ItemWriter<Pay> jdbcPagingItemWriter() {

        return list -> {
            String id = "";
            for (Pay pay: list) {
                log.info("PAY_LOG" + pay.toString());
                // pay.setSuccessStatus(false);
                //if (id.length() > 0) id+=",";
                //id+=String.valueOf(pay.getId());
                new JdbcBatchItemWriterBuilder<Pay>()
                        .dataSource(dataSource)
                        .sql("INSERT INTO pay2(amount, tx_name, tx_date_time) VALUES (:amount, :txName, :txDateTime)")
                        //.sql("UPDATE pay SET success_status = false where id = :id")
                        .beanMapped()
                        .build();
            }

        };

        /**
         * JdbcBatchItemWriter 의 설정에서 주의할 점
         * - JdbcBatchItemWriter 의 제네릭 타입은 Reader 에서 넘겨주는 값의 타입이다.
         * - Pay2 테이블에 데이터를 넣은 Writer 이지만, 선언된 제네릭 타입은 Reader/Processor 에서 넘겨준 Pay 클래스이다.
         */
        /*
        return new JdbcBatchItemWriterBuilder<Pay>()
                .dataSource(dataSource)
                .sql("INSERT INTO pay2(amount, tx_name, tx_date_time) VALUES (:amount, :txName, :txDateTime)")
                //.sql("UPDATE pay SET success_status = 0 where id = in(" + id+ ")")
                .beanMapped()
                .build();
        */
        /**
         * 위와 아래의 차이 즉, beanMapped() 와 columnMapped() 의 차이는
         * Reader 에서 Writer 로 넘겨주는 타입이 Map<String, Object> 냐, Pay.class 와 같은 POJO 타입이냐 입니다.
         */

//    return new JdbcBatchItemWriterBuilder<Map<String, Object>>()  // Map 사용
//            .columnMapped()
//            .dataSource(dataSource)
//            .sql("INSERT INTO pay2(amount, tx_name, tx_date_time) VALUES (:amount, :txName, :txDateTime)")
//            .build();
    }

    @Bean(name = "JdbcPagingItemReader_queryProvider")
    public PagingQueryProvider createQueryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource); // Database에 맞는 PagingQueryProvider를 선택하기 위해
        //queryProvider.setSelectClause("id, amount, tx_name, tx_date_time");
        queryProvider.setSelectClause("id, amount, tx_date_time, success_status");
        queryProvider.setFromClause("from pay");
        queryProvider.setWhereClause("where amount >= :amount");

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);

        return queryProvider.getObject();
    }

}
