package com.jackin.Schedule;

import com.jackin.common.EmbeddedConstant;
import com.jackin.common.EmbeddedUtil;
import com.jackin.common.MySqlUtil;
import com.jackin.entity.Job;
import com.jackin.repositories.ClientMongoOpertor;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by W541 on 2017/5/16.
 */
@Component
@ConfigurationProperties(prefix = "system")
public class JobSchedule {

    private Logger logger = LogManager.getLogger();

    private static ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * record all running jobs, stop the running job when the job stats was STOPPED
     */
    private static final Map<String, Future<?>> jobFutureMap = new HashMap<>();

    @Value("embedded.storePath")
    private String storePath;

    @Value("threshold")
    private String threshold;

    @Value("instanceNo")
    private String instanceNo;

    @Autowired
    private ClientMongoOpertor clientMongoOpertor;

    @PostConstruct
    public void init() {

        Map<String, Object> params = new HashMap<>(1);
        params.put("state", EmbeddedConstant.RUNNING);

        try {
            List<Job> jobs = clientMongoOpertor.find(params, EmbeddedConstant.JOB_COLLECTION, Job.class);
            jobs.forEach((job) -> {
                Runnable runnable = null;
                try {
                    runnable = EmbeddedUtil.prepare(storePath, job, clientMongoOpertor);
                } catch (SQLException e) {
                    logger.error("prepare job fail [job:" + job + "]", e);
                }
                jobFutureMap.put(job.get_id(), executor.submit(runnable));
            });
        } catch (IllegalAccessException e) {
            logger.error("init job schedule happen exception:", e);
        }
    }

    @Scheduled(cron = "*/1 * * * * *")
    public void scanJob() {

        try {
            // start the started-rquest job
            Job runningJob = runningJob();
            if (runningJob != null) {
                Runnable runnable = null;
                try {
                    runnable = EmbeddedUtil.prepare(storePath, runningJob, clientMongoOpertor);
                } catch (SQLException e) {
                    logger.error("prepare job fail [job:" + runningJob + "]", e);
                }
                jobFutureMap.put(runningJob.get_id(), executor.submit(runnable));
            }
            // stop the interrupted job
            Map<String, Object> update = new HashMap<>(1);
            update.put("state", EmbeddedConstant.STOPPED);
            Map<String, Object> params = new HashMap<>(1);
            List<Job> stoppedJobs = stoppedJob();
            if (!CollectionUtils.isEmpty(stoppedJobs)) {
                stoppedJobs.forEach(job -> {
                    try {
                        String jobId = job.get_id();
                        if (jobFutureMap.containsKey(jobId)) {
                            jobFutureMap.get(jobId).cancel(true);
                            jobFutureMap.remove(jobId);
                            params.put("_id", jobId);
                            clientMongoOpertor.updateAndParam(params, update, EmbeddedConstant.JOB_COLLECTION);
                        }
                    } catch (Exception e) {
                        logger.error("stop the interrupted job fail [job:"+job+"]", e);
                    }
                });
            }
        } catch (Exception e) {
            logger.error("scanJob happen exception:", e);
        }
    }

    private Job runningJob() throws IllegalAccessException {
        Map<String, Object> params = new HashMap<>();
        params.put("state", EmbeddedConstant.START_REQUESTED);

        Map<String, Object> update = new HashMap<>();
        update.put("state", EmbeddedConstant.RUNNING);

        return clientMongoOpertor.findAndModify(params, update, Job.class, EmbeddedConstant.JOB_COLLECTION);
    }

    private List<Job> stoppedJob() throws IllegalAccessException {
        Map<String, Object> params = new HashMap<>();
        params.put("state", EmbeddedConstant.INTERRUPTED);

        return clientMongoOpertor.find(params, EmbeddedConstant.JOB_COLLECTION, Job.class);
    }

    public String getStorePath() {
        return storePath;
    }

    public void setStorePath(String storePath) {
        this.storePath = storePath;
    }

    public String getThreshold() {
        return threshold;
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }

    public String getInstanceNo() {
        return instanceNo;
    }

    public void setInstanceNo(String instanceNo) {
        this.instanceNo = instanceNo;
    }
}
