package daily_service;

import daily_service.props.DailyTopServiceProperties;
import news_service.NewsServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
public class FetcherCronSheduler  implements SchedulingConfigurer {

    @Autowired
    DailyTopService dailyTopService;

    @Autowired
    NewsServiceImpl newsService;

    @Autowired
    DailyTopServiceProperties properties;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        if (properties.getFetcherCron() != null) {
            taskRegistrar.addCronTask(() -> {
                        newsService.run();
                        dailyTopService.run();
                    },
                    properties.getFetcherCron());
        }
    }
}

