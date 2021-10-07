package com.redis.redisbloomspringdedup;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.gears.Registration;
import com.redis.lettucemod.api.sync.RedisGearsCommands;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.api.timeseries.Aggregation;
import com.redis.lettucemod.api.timeseries.CreateOptions;
import com.redis.lettucemod.api.timeseries.RangeOptions;
import com.redis.lettucemod.api.timeseries.RangeResult;
import com.redis.lettucemod.api.timeseries.Sample;
import com.redis.lettucemod.output.ExecutionResults;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.Data;

@SpringBootApplication
public class DeduplicationApplication {

  Logger logger = LoggerFactory.getLogger(DeduplicationApplication.class);

  @Autowired
  StatefulRedisModulesConnection<String, String> connection;

  public static void main(String[] args) {
    SpringApplication.run(DeduplicationApplication.class, args);
  }

  @Bean
  CommandLineRunner createTimeSeries(StatefulRedisModulesConnection<String, String> connection) {
    RedisTimeSeriesCommands<String, String> rts = connection.sync();

    return args -> {
      // Create 4 time-series.
      try {
        rts.create("s-unfiltered", CreateOptions.<String, String>builder().retentionTime(60000).build());
        rts.create("s-filtered", CreateOptions.<String, String>builder().retentionTime(60000).build());
        rts.create("unfiltered",
            CreateOptions.<String, String>builder().label("Type", "Final").retentionTime(86400000).build());
        rts.create("filtered",
            CreateOptions.<String, String>builder().label("Type", "Final").retentionTime(86400000).build());
        // Create a compaction rules
        rts.createrule("s-unfiltered", "unfiltered",
            Aggregation.builder().type(Aggregation.Type.LAST).timeBucket(1000).build());
        rts.createrule("s-filtered", "filtered",
            Aggregation.builder().type(Aggregation.Type.LAST).timeBucket(1000).build());
      } catch (RedisCommandExecutionException rcee) {
        // io.lettuce.core.RedisCommandExecutionException: ERR TSDB: key already exists
      }
    };
  }

  @PostConstruct
  public void loadGearsScript() throws IOException {
    String py = StreamUtils.copyToString(new ClassPathResource("scripts/dedup.py").getInputStream(),
        Charset.defaultCharset());
    RedisGearsCommands<String, String> gears = connection.sync();
    List<Registration> registrations = gears.dumpregistrations();

    Optional<String> maybeRegistrationId = getGearsRegistrationIdForTrigger(registrations, "RateLimiter");
    if (maybeRegistrationId.isEmpty()) {
      try {
        ExecutionResults er = gears.pyexecute(py);
        if (er.isOk()) {
          logger.info("dedup.py has been registered");
        } else if (er.isError()) {
          logger.error(String.format("Could not register dedup.py -> %s", Arrays.toString(er.getErrors().toArray())));
        }
      } catch (RedisCommandExecutionException rcee) {
        logger.error(String.format("Could not register dedup.py -> %s", rcee.getMessage()));
      }
    } else {
      logger.info("dedup.py has already been registered");
    }
  }

  private Optional<String> getGearsRegistrationIdForTrigger(List<Registration> registrations, String trigger) {
    return registrations.stream().filter(r -> r.getData().getArgs().get("stream").equals("outfill")).findFirst()
        .map(Registration::getId);
  }

}

@Data
class StreamRequest {
  int messages;
  int range;
  int sleepms;
}

@Controller
class DeduplicationApplicationController {
  
  // time formatting
  String timePattern = "HH:mm:ss";
  DateTimeFormatter timeColonFormatter = DateTimeFormatter.ofPattern(timePattern);
  
  @Autowired
  StringRedisTemplate redisTemplate;

  @Autowired
  StatefulRedisModulesConnection<String, String> connection;

  @GetMapping("/")
  public String getIndex() {
    return "index";
  }

  @PostMapping("/firemessage")
  public RedirectView fireMessage(@ModelAttribute("stream-request") StreamRequest sr) {
    StreamOperations<String, Object, Object> streams = redisTemplate.opsForStream();
    
    Map<String,String> map = new HashMap<String,String>();  
    map.put("messages", Integer.toString(sr.getMessages()));
    map.put("range", Integer.toString(sr.getMessages() * sr.getRange() / 100));
    map.put("sleepms", Integer.toString(sr.getSleepms()));
    streams.add("fill", map);

    return new RedirectView("graphs"); // should be a 302
  }

  @GetMapping("/graphs")
  public ModelAndView graphs(Map<String, Object> model) {
    RedisTimeSeriesCommands<String, String> rts = connection.sync();
    // "TS.MRANGE" "0" "-1" "FILTER" "Type=Final"
    List<RangeResult<String, String>> ts = rts.mrange(RangeOptions.builder().from(0).to(-1).build(), "Type=Final");
    for (Iterator<RangeResult<String, String>> iterator = ts.iterator(); iterator.hasNext();) {
      RangeResult<String, String> rangeResult = (RangeResult<String, String>) iterator.next();
      System.out.println(">>> RangeResult ==> " + rangeResult.getSamples());
    }

    // [RangeResult(key=filtered, labels={}, samples=[]), RangeResult(key=unfiltered, labels={}, samples=[])]
    List<String> labels = new ArrayList<String>();
    List<String> filtered = new ArrayList<String>();
    List<String> unfiltered = new ArrayList<String>();
    long lastTick = 0;
    
    List<Sample> unfilteredRaw = ts.get(1).getSamples();
    List<Sample> filteredRaw = ts.get(0).getSamples();
    
    // be sure to get all of the unfiltered first otherwise the series gets truncated
    for (Sample sample : unfilteredRaw) {
      LocalTime timestamp = LocalTime.ofSecondOfDay(sample.getTimestamp() / 1000);
      labels.add(timeColonFormatter.format(timestamp));
      unfiltered.add(Double.toString(sample.getValue()));
    }
    
    for (Sample sample : filteredRaw) {
      filtered.add(Double.toString(sample.getValue()));
      lastTick = sample.getTimestamp();
    }
    
    // we need to backfill the filtered timeseries to match the length of the unfiltered
    for (int i = filtered.size(); i < unfiltered.size(); i++) {
      filtered.add(Long.toString(lastTick));
    }
    
    System.out.println(">>> LABELS <<<<");
    System.out.println(Arrays.toString(labels.toArray()));
    System.out.println(">>> FILTERED <<<<");
    System.out.println(Arrays.toString(filtered.toArray()));
    System.out.println(">>> UNFILTERED <<<<");
    System.out.println(Arrays.toString(unfiltered.toArray()));
    
    model.put("labels", labels);
    model.put("filtered", filtered);
    model.put("unfiltered", unfiltered);

    return new ModelAndView("stats", model);
  }
}
