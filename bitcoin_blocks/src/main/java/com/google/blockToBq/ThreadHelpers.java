package com.google.blockToBq;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadHelpers {
  private static final Logger log = LoggerFactory.getLogger(ThreadHelpers.class);

  public static final UncaughtExceptionHandler uncaughtExceptionHandler = (Thread t, Throwable e) -> {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    log.error("BitcoinBlockHandler- uncaught exception: " + sw.toString());
  };


  public static final ThreadFactory threadFactory = (Runnable r) -> {
    Thread thread = new Thread(r);
    thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    return thread;
  };

}
