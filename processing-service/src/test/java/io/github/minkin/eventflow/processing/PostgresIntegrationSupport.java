package io.github.minkin.eventflow.processing;

import com.fasterxml.jackson.databind.json.JsonMapper;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class PostgresIntegrationSupport {
  @Container
  protected static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17-alpine");

  protected final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
  protected JdbcClient jdbc;
  protected JdbcTransactionManager transactionManager;
  protected TransactionTemplate transactions;

  @BeforeEach
  void prepareDatabase() {
    var dataSource =
        new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    var flyway = Flyway.configure().dataSource(dataSource).cleanDisabled(false).load();
    flyway.clean();
    flyway.migrate();
    jdbc = JdbcClient.create(dataSource);
    transactionManager = new JdbcTransactionManager(dataSource);
    transactions = new TransactionTemplate(transactionManager);
  }

  @SuppressWarnings("unchecked")
  protected <T> T transactional(T target) {
    var advice = new TransactionInterceptor();
    advice.setTransactionManager(transactionManager);
    advice.setTransactionAttributeSource(new AnnotationTransactionAttributeSource());
    var factory = new ProxyFactory(target);
    factory.setProxyTargetClass(true);
    factory.addAdvice(advice);
    return (T) factory.getProxy();
  }

  protected long count(String table) {
    return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
  }
}
