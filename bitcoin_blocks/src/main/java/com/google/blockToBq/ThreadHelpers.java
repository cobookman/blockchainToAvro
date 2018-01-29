package com.google.blockToBq;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadHelpers {
  private static final Logger log = LoggerFactory.getLogger(ThreadHelpers.class);

  public static class ThreadPool extends ThreadPoolExecutor {

    /** Instantiates a new ThreadPool. */
    public ThreadPool(int minWorkers, int maxWorkers, long keepAliveTime, TimeUnit timeUnit) {
      super(minWorkers, maxWorkers, keepAliveTime, timeUnit,
          new LinkedBlockingQueue<>(), new ThreadFactoryWithErrorHandler());
    }

    /** Captures exceptions from thread pool threads. */
    public static class ExceptionHandler implements UncaughtExceptionHandler {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        log.error("BitcoinBlockHandler- uncaught exception: " + sw.toString());
      }
    }

    /** THreadFactory which attaches ExceptionHandler. */
    public static class ThreadFactoryWithErrorHandler implements ThreadFactory {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setUncaughtExceptionHandler(new ExceptionHandler());
        return thread;
      }
    }

    /** Stops thread pool and blocks till tasks done. */
    public void stop() {
      shutdown();
      while (getQueue().size() != 0) {
        System.out.println("Flushing OnFileRotation Queue, pending ops: " + getQueue().size());
        System.out.flush();
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      try {
        awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}
