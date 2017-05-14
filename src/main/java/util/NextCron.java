package util;

import org.springframework.scheduling.support.CronSequenceGenerator;

import java.util.Date;

public class NextCron {
    static void main(String[] args) {
        CronSequenceGenerator seq = new CronSequenceGenerator(args[0]);
        Date now = new Date();
        Date next = seq.next(now);
        long delta = next.getTime() - now.getTime();
        System.out.println(delta / 1000L);
    }
}
