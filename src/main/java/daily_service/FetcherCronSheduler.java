package daily_service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
public class FetcherCronSheduler  implements SchedulingConfigurer {

    @Autowired
    DailyTopService service;

    @Autowired
    DailyTopServiceProperties properties;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addCronTask(service::run,
                properties.getFetcherCron());
    }
}

