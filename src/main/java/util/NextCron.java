package util;

import org.springframework.scheduling.support.CronSequenceGenerator;

import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public class NextCron {
    public static void main(String[] args) {
        CronSequenceGenerator seq = new CronSequenceGenerator(
                Arrays.stream(args).collect(Collectors.joining(" "))
                );
        Date now = new Date();
        Date next = seq.next(now);
        long delta = next.getTime() - now.getTime();
        System.out.println(delta / 1000L);
    }
}
