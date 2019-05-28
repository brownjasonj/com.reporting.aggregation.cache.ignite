package com.reporting.aggregation.cache.ignite.configuration;

import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
//@Scope(value= ConfigurableBeanFactory.SCOPE_SINGLETON)
@Scope
public class ApplicationConfig implements IResultPublisherConfiguration {
    @Autowired
    private Environment environment;

    public String getKafkaServer() {
        return environment.getProperty("kafka.server");
    }

    public String getIntradayRiskSetTopic() {
        return environment.getProperty("kafka.topic.intradayriskset");
    }

    public String getIntradayRiskTickTopic() {
        return environment.getProperty("kafka.topic.intradayrisktick");
    }

    public String getCalculationContextTopic() {
        return environment.getProperty("kafka.topic.calccontext");
    }

    public String getIntradayTradeTopic() {
        return environment.getProperty("kafka.topic.intradaytrade");
    }

    public String getMarketEnvTopic() {
        return environment.getProperty("kafka.topic.market");
    }

    public String getRiskAggregatesTopic() {
        return environment.getProperty("kafka.topic.riskaggregates");        
    }

    public String getCalculationContextDataSetName() { return environment.getProperty("persistence.dataset.calculationcontext");}
    public String getMarketEnvDataSetName() { return environment.getProperty("persistence.dataset.marketenv");}
    public String getRiskResultDataSetName() { return environment.getProperty("persistence.dataset.riskresultset");}
    public String getRiskResultSetDataSetName() { return environment.getProperty("persistence.dataset.riskresult");}
    public String getTradePopulationDataSetName() { return environment.getProperty("persistence.dataset.tradepopulation");}
    public String getTradeDataSetName() { return environment.getProperty("persistence.dataset.trade");}

}
